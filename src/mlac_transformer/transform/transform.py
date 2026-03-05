import json
import os
import re
import jq
import yaml
from datetime import datetime
from pathlib import Path

from src.mlac_transformer.logger import write_log


class Transformers:
    """
    ETL Stage 2 — Declarative Transform Processor (multi-sheet).

    Transforms a flat JSON workbook (produced by the extractor) into one or
    more hierarchical Sitecore-ready JSON structures, driven entirely by a
    declarative YAML specification.  No business logic lives here — this class
    is a pure interpreter of the YAML declarations.

    YAML structure expected:
        input:
          workbook_key: "workbook"
          sheets: [specs, ...]

        sheets:
          specs:
            sitecore_config: { ... }
            columns:
              base: "ColName"
              packages: ["ColA", "ColB", ...]
            items: [ ... ]

    Usage:
        t = Transformers(raw_file="path/input.json", yaml_file="path/transform.yaml")
        output_path = t.run()                      # process all sheets
        output_path = t.run(sheet="specs")         # process one sheet only
        output_path = t.run(split=True)            # one file per sheet
    """

    def __init__(self, raw_file: str, yaml_file: str) -> None:
        self.raw_file  = raw_file
        self.yaml_file = yaml_file
        today = datetime.today()
        self.output_path = (
            Path("output/transform")
            / today.strftime("%Y")
            / today.strftime("%m")
            / today.strftime("%d")
        )

    # =========================================================================
    # PUBLIC ENTRY POINT
    # =========================================================================

    def run(self, sheet: str = None, split: bool = False) -> str:
        """
        Execute the transformation pipeline.

        Args:
            sheet : Process only this sheet name (overrides input.sheets in YAML).
            split : When True, write one JSON file per sheet and return the
                    output directory path.  When False (default), write all
                    sheets combined into a single JSON array and return that
                    file path.

        Returns:
            Path string of the written output file (or directory if split=True).
        """
        cfg = self._load_yaml()
        raw = self._load_raw_json()

        input_cfg    = cfg.get("input", {})
        workbook_key = input_cfg.get("workbook_key", "workbook")
        sheets_def   = cfg.get("sheets", {})

        # Resolve which sheets to process
        if sheet:
            sheet_names = [sheet]
        else:
            sheet_names = input_cfg.get("sheets", list(sheets_def.keys()))

        write_log("info", f"Sheets to process: {sheet_names}")

        # Process each sheet
        results: dict[str, dict] = {}
        for sheet_name in sheet_names:
            if sheet_name not in sheets_def:
                write_log("warning", f"Sheet '{sheet_name}' has no definition in YAML — skipping.")
                continue
            try:
                flat_rows = self._load_sheet_rows(raw, workbook_key, sheet_name)
            except KeyError as e:
                write_log("error", str(e))
                continue
            results[sheet_name] = self._process_sheet(
                sheets_def[sheet_name], flat_rows, sheet_name
            )

        # Write output
        if split:
            return self._write_split_output(results, sheet_names)
        else:
            return self._write_combined_output(results, sheet_names)

    # =========================================================================
    # I/O HELPERS
    # =========================================================================

    def _load_yaml(self) -> dict:
        try:
            with open(self.yaml_file, "r", encoding="utf-8") as fh:
                cfg = yaml.safe_load(fh)
            write_log("info", f"YAML config loaded from '{self.yaml_file}'.")
            return cfg
        except Exception as e:
            write_log("error", f"Failed to load YAML '{self.yaml_file}': {e}")
            raise

    def _load_raw_json(self) -> dict:
        try:
            with open(self.raw_file, "r", encoding="utf-8") as fh:
                data = json.load(fh)
            write_log("info", f"Raw JSON loaded from '{self.raw_file}'.")
            return data
        except Exception as e:
            write_log("error", f"Failed to load raw JSON '{self.raw_file}': {e}")
            raise

    def _load_sheet_rows(self, raw: dict, workbook_key: str, sheet_name: str) -> list:
        """
        Navigate the raw JSON to find the rows for a given sheet.
        Supports:
          - raw is a list                          → use directly
          - raw[workbook_key][sheet_name]          → standard workbook structure
        """
        if isinstance(raw, list):
            return raw

        workbook = raw.get(workbook_key, raw)

        if isinstance(workbook, dict) and sheet_name in workbook:
            rows = workbook[sheet_name]
            if isinstance(rows, list):
                return rows

        available = list(workbook.keys()) if isinstance(workbook, dict) else "N/A"
        raise KeyError(
            f"Sheet '{sheet_name}' not found in workbook. Available: {available}"
        )

    def _write_combined_output(self, results: dict, sheet_names: list) -> str:
        """Write all sheets as a single JSON array. Returns the output file path."""
        self.output_path.mkdir(parents=True, exist_ok=True)
        combined  = [results[s] for s in sheet_names if s in results]
        out_file  = self.output_path / "transform.json"
        with open(out_file, "w", encoding="utf-8") as fh:
            json.dump(combined, fh, indent=2, ensure_ascii=False)
        write_log("info", f"Combined output ({len(combined)} sheet(s)) written to '{out_file}'.")
        return str(out_file)

    def _write_split_output(self, results: dict, sheet_names: list) -> str:
        """Write one JSON file per sheet. Returns the output directory path."""
        self.output_path.mkdir(parents=True, exist_ok=True)
        for sheet_name in sheet_names:
            if sheet_name not in results:
                continue
            out_file = self.output_path / f"{sheet_name}.json"
            with open(out_file, "w", encoding="utf-8") as fh:
                json.dump([results[sheet_name]], fh, indent=2, ensure_ascii=False)
            write_log("info", f"Sheet '{sheet_name}' written to '{out_file}'.")
        return str(self.output_path)

    # =========================================================================
    # SHEET PROCESSOR
    # =========================================================================

    def _process_sheet(self, sheet_spec: dict, flat_rows: list, sheet_name: str) -> dict:
        """
        Apply the sheet's YAML definition to its flat rows.
        Returns a sitecore output dict: { sitecoreConfig, items }.
        """
        sitecore_config = self._build_sitecore_config(sheet_spec.get("sitecore_config", {}))
        columns_def     = sheet_spec.get("columns", {})
        items_def       = sheet_spec.get("items", [])

        output_items = []

        for item_def in items_def:
            source_expr = item_def.get("source", "")

            if item_def.get("templateKey") == "group":
                groups = self._split_rows_into_groups(flat_rows, source_expr)
                for group_row, spec_rows in groups:
                    output_items.append(
                        self._build_group_item(group_row, spec_rows, item_def, columns_def)
                    )
            else:
                # Top-level non-group items: apply JQ directly to all rows
                try:
                    selected = jq.first(source_expr, flat_rows) or []
                except Exception as e:
                    write_log("warning", f"JQ error on source '{source_expr}': {e}")
                    selected = []
                for row in selected:
                    output_items.append(self._build_spec_item(row, item_def, columns_def))

        groups_n  = len(output_items)
        specs_n   = sum(len(i.get("children", [])) for i in output_items)
        pkg_n     = sum(
            len(s.get("children", []))
            for i in output_items
            for s in i.get("children", [])
        )
        write_log(
            "info",
            f"[{sheet_name}] rows={len(flat_rows)}  "
            f"groups={groups_n}  specs={specs_n}  packageSpecs={pkg_n}"
        )

        return {"sitecoreConfig": sitecore_config, "items": output_items}

    def _build_sitecore_config(self, raw_cfg: dict) -> dict:
        return {
            "modelPath":   raw_cfg.get("model_path",   raw_cfg.get("modelPath", "")),
            "backupName":  raw_cfg.get("backup_name",  raw_cfg.get("backupName", "")),
            "backupPath":  raw_cfg.get("backup_path",  raw_cfg.get("backupPath", "")),
            "backupCount": raw_cfg.get("backup_count", raw_cfg.get("backupCount", 0)),
            "type":        raw_cfg.get("type", ""),
            "operation":   raw_cfg.get("operation", ""),
            "templates":   raw_cfg.get("templates", {}),
        }

    # =========================================================================
    # GROUP / SPEC / PACKAGESPEC BUILDERS
    # =========================================================================

    def _split_rows_into_groups(self, rows: list, group_source_expr: str) -> list:
        """
        Apply the JQ group-source expression to the full rows array, locate the
        matching header rows by position, then pair each header with the slice of
        rows that follow it until the next header.

        Returns: list of (group_row, [spec_rows_in_this_group])
        """
        try:
            matched = jq.first(group_source_expr, rows) or []
        except Exception as e:
            write_log("error", f"JQ failed on group source '{group_source_expr}': {e}")
            return []

        matched_keys = {str(r.get("Packages", "")) for r in matched}
        group_indices = [
            i for i, row in enumerate(rows)
            if str(row.get("Packages", "")) in matched_keys
        ]

        result = []
        for pos, idx in enumerate(group_indices):
            next_idx = group_indices[pos + 1] if pos + 1 < len(group_indices) else len(rows)
            result.append((rows[idx], rows[idx + 1 : next_idx]))
        return result

    def _select_spec_rows(self, rows: list) -> list:
        """
        Keep only genuine spec rows:
          - Non-empty Packages label
          - At least one non-empty value in a column other than Packages
        """
        result = []
        for row in rows:
            if not str(row.get("Packages", "")).strip():
                continue
            if all(str(v).strip() == "" for k, v in row.items() if k != "Packages"):
                continue
            result.append(row)
        return result

    def _build_group_item(self, group_row: dict, spec_rows: list,
                          group_def: dict, columns_def: dict) -> dict:
        """Build a group item with all its spec children."""
        children_items = []

        for child_def in group_def.get("children", []):
            source = child_def.get("source", "")

            if source == "SCOPED_BETWEEN_GROUPS":
                filtered = self._select_spec_rows(spec_rows)
            else:
                try:
                    filtered = jq.first(source, spec_rows) or []
                except Exception as e:
                    write_log("warning", f"JQ error on child source '{source}': {e}")
                    filtered = []

            for row in filtered:
                children_items.append(self._build_spec_item(row, child_def, columns_def))

        return {
            "name":        self._resolve_item_name(group_row, group_def),
            "templateKey": group_def["templateKey"],
            "fields":      self._build_fields(group_row, group_def.get("fields", [])),
            "children":    children_items,
        }

    def _build_spec_item(self, row: dict, item_def: dict, columns_def: dict) -> dict:
        """Build a single spec item plus its packageSpec children."""
        base_col = columns_def.get("base", "")
        item = {
            "name":        self._resolve_item_name(row, item_def),
            "templateKey": item_def["templateKey"],
            "fields":      self._build_fields(row, item_def.get("fields", []), base_col=base_col),
        }
        pkg_def = item_def.get("package_children")
        if pkg_def:
            children = self._build_package_children(row, pkg_def, columns_def)
            if children:
                item["children"] = children
        return item

    def _build_package_children(self, row: dict, pkg_def: dict,
                                 columns_def: dict) -> list:
        """
        Generate packageSpec children for a spec row.
        With only_when_differs=true, skip packages whose value equals the base value.
        """
        base_col     = columns_def.get("base", "")
        all_packages = columns_def.get("packages", [])
        only_differs = pkg_def.get("only_when_differs", False)
        base_val     = str(row.get(base_col, "")).strip()

        children = []
        for pkg_col in all_packages:
            if pkg_col == base_col:
                continue
            pkg_val = str(row.get(pkg_col, "")).strip()
            if only_differs and pkg_val == base_val:
                continue
            if not pkg_val:
                continue
            children.append({
                "name":        pkg_col,
                "templateKey": pkg_def["templateKey"],
                "fields":      self._build_fields(
                    row, pkg_def["fields"], base_col=base_col, pkg_col=pkg_col
                ),
            })
        return children

    # =========================================================================
    # FIELD / NAME RESOLVERS
    # =========================================================================

    def _build_fields(self, row: dict, fields_def: list,
                      base_col: str = None, pkg_col: str = None) -> list:
        """Build the fields list for an item from its field definitions."""
        return [
            {
                "name":  f["name"],
                "value": self._resolve_field_value(
                    row, f["from_field"], f.get("from_pattern"),
                    base_col=base_col, pkg_col=pkg_col
                ),
            }
            for f in fields_def
        ]

    def _resolve_field_value(self, row: dict, from_field: str,
                              from_pattern: str = None,
                              base_col: str = None,
                              pkg_col: str = None) -> str:
        """Resolve a field value, handling special tokens and regex patterns."""
        if from_field == "__base_column__":
            val = str(row.get(base_col or "", ""))
        elif from_field == "__package_column__":
            val = str(row.get(pkg_col or "", ""))
        else:
            val = str(row.get(from_field, ""))

        if from_pattern:
            val = self._extract_pattern(val, from_pattern)
        return val

    def _resolve_item_name(self, row: dict, item_def: dict) -> str:
        """Resolve the display name for an item from the YAML definition."""
        if item_def.get("name_static"):
            return item_def["name_static"]

        name_from = item_def.get("name_from")
        if name_from:
            val = str(row.get(name_from["field"], "")).strip()
            if name_from.get("pattern"):
                val = self._extract_pattern(val, name_from["pattern"])
            return val

        name_slug = item_def.get("name_slug")
        if name_slug:
            return self._slugify(str(row.get(name_slug["field"], "")))

        return "unnamed"

    # =========================================================================
    # STATIC HELPERS
    # =========================================================================

    @staticmethod
    def _extract_pattern(value: str, pattern: str) -> str:
        """Return the first capture group of pattern matched against value."""
        m = re.search(pattern, str(value), re.DOTALL)
        return m.group(1).strip() if m else str(value).strip()

    @staticmethod
    def _slugify(text: str) -> str:
        """Convert a string into a URL/name-safe slug."""
        text = str(text).strip().lower()
        text = re.sub(r"[^\w\s-]", "", text)
        text = re.sub(r"[\s_-]+", "-", text)
        text = re.sub(r"^-+|-+$", "", text)
        return text