import json
import re
import jq
import yaml
from datetime import datetime
from pathlib import Path

from src.mlac_transformer.logger import write_log


class RequiredFieldError(Exception):
    """Raised when a field marked required: true resolves to an empty, null, or undefined value."""


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
            items:
              - templateKey: "group"
                filter: '<JQ on all rows>'
                children:
                  - templateKey: "spec"
                    filter: '<JQ on scoped rows>'
                    children:                          # N levels deep
                      - templateKey: "packageSpec"
                        expand_variants: true          # JQ on normalized variant rows
                        filter: '<JQ on variant rows>'
                        fields: [...]

    expand_variants:
        false (default) — JQ operates on the inherited row list context
        true            — JQ operates on [{__column__, __value__, __base_value__}, ...]
                          derived from columns.variants and the parent spec row

    dynamic_fields:
        Generates fields dynamically from the rows in scope rather than from
        static field definitions.  Useful for long/vertical Excel formats where
        field names live in a column instead of as column headers.
            name_from : column whose value becomes the field name
            value_from: column whose value becomes the field value
            filter    : optional JQ expression to pre-filter the scoped rows
            type      : optional type resolver (e.g. "getType")
        When scope_children is true, scoped rows are the positional slice owned
        by the item.  Otherwise the scope is the single matched row.

    Usage:
        t = Transformers(raw_file="path/input.json", yaml_file="path/transform.yaml")
        output_path = t.run()                      # process all sheets
        output_path = t.run(sheet="specs")         # process one sheet only
        output_path = t.run(split=True)            # one file per sheet
    """

    def __init__(self, raw_file: str, yaml_file: str) -> None:
        self.raw_file      = raw_file
        self.yaml_file     = yaml_file
        self._current_sheet: str = ""
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
        self._current_sheet = sheet_name
        sitecore_config = self._build_sitecore_config(sheet_spec.get("sitecore_config", {}))
        columns_def     = sheet_spec.get("source_structure", {})
        items_def       = sheet_spec.get("items", [])

        output_items = self._build_items(flat_rows, items_def, columns_def)

        groups_n = len(output_items)
        specs_n  = sum(len(i.get("children", [])) for i in output_items)
        pkg_n    = sum(
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
    # RECURSIVE ITEM BUILDER
    # =========================================================================

    def _build_items(self, context: list, items_def: list,
                     columns_def: dict, parent_row: dict = None) -> list:
        """
        Recursively build output items from a context list and item definitions.

        Args:
            context    : List of row dicts to apply source JQ expressions to.
            items_def  : Item definition list from the YAML.
            columns_def: The columns block from the sheet definition.
            parent_row : The parent item's row dict, used when a child declares
                         expand_variants=true to generate normalized variant rows.
        """
        output = []
        for item_def in items_def:
            output.extend(self._build_item_list(context, item_def, columns_def, parent_row))
        return output

    def _build_item_list(self, context: list, item_def: dict,
                         columns_def: dict, parent_row: dict = None) -> list:
        """Process one item definition and return the list of built items."""
        filter_expr     = item_def.get("filter", "")
        expand_variants = item_def.get("expand_variants", False)

        # Determine the JQ input based on expand_variants flag
        if expand_variants:
            jq_input = self._normalize_package_rows(parent_row or {}, columns_def)
        else:
            jq_input = context

        # Group items use the scoping mechanism
        if item_def.get("templateKey") == "group":
            return self._build_group_items(jq_input, filter_expr, item_def, columns_def)

        # All other items: apply filter JQ to jq_input
        try:
            selected = jq.first(filter_expr, jq_input) or []
        except Exception as e:
            write_log("warning", f"JQ error on filter '{filter_expr}': {e}")
            selected = []


        scope_children       = item_def.get("scope_children", False)
        children_filter_expr = item_def.get("children_filter")

        result = []
        if scope_children:
            # Slice the parent context between consecutive selected rows so each
            # item only sees the rows that belong to it (same logic as groups).
            positions = self._find_row_positions(selected, jq_input)
            for idx, row in enumerate(selected):
                pos_start   = positions[idx]
                pos_end     = positions[idx + 1] if idx + 1 < len(positions) else len(jq_input)
                scoped_rows = jq_input[pos_start:pos_end]
                item = self._build_single_item(row, item_def, columns_def, scoped_rows=scoped_rows)
                children_def = item_def.get("children", [])
                if children_def:
                    children = self._build_items(
                        context=scoped_rows,
                        items_def=children_def,
                        columns_def=columns_def,
                        parent_row=row,
                    )
                    if children:
                        item["children"] = children
                result.append(item)
        else:
            for row in selected:
                item = self._build_single_item(row, item_def, columns_def, scoped_rows=[row])
                children_def = item_def.get("children", [])
                if children_def:
                    if children_filter_expr:
                        try:
                            child_context = jq.first(children_filter_expr, jq_input) or []
                        except Exception as e:
                            write_log("warning", f"JQ error on children_filter '{children_filter_expr}': {e}")
                            child_context = [row]
                    else:
                        child_context = [row]
                    children = self._build_items(
                        context=child_context,
                        items_def=children_def,
                        columns_def=columns_def,
                        parent_row=row,
                    )
                    if children:
                        item["children"] = children
                result.append(item)
        return result

    def _build_group_items(self, rows: list, filter_expr: str,
                           item_def: dict, columns_def: dict) -> list:
        """Handle group-level items with row scoping via _split_rows_into_groups."""
        groups = self._split_rows_into_groups(rows, filter_expr, columns_def)
        result = []
        for group_row, spec_rows in groups:
            item = self._build_single_item(group_row, item_def, columns_def)
            children_def = item_def.get("children", [])
            if children_def:
                # Pass [group_row] + spec_rows so children filter can use .[0] / .[1:]
                child_context = [group_row] + spec_rows
                children = self._build_items(child_context, children_def, columns_def, parent_row=group_row)
                if children:
                    item["children"] = children
            result.append(item)
        return result

    def _build_single_item(self, row: dict, item_def: dict, columns_def: dict,
                           scoped_rows: list = None) -> dict:
        """Build one output item dict (name, templateKey, fields) without recursing."""
        base_col = columns_def.get("column_base", "")
        fields = self._build_fields(row, item_def.get("fields", []), base_col=base_col)
        if item_def.get("dynamic_fields") is not None:
            fields += self._build_dynamic_fields(scoped_rows or [row], item_def["dynamic_fields"], row=row)
        return {
            "name":        self._resolve_item_name(row, item_def),
            "templateKey": item_def["templateKey"],
            "fields":      fields,
        }

    def _normalize_package_rows(self, row: dict, columns_def: dict) -> list:
        """
        Convert a spec row into a list of normalized variant row dicts, one per
        variant column defined in columns.variants.  Reserved double-underscore keys
        prevent collision with real column names:

            __column__    : package column name
            __value__     : value of that column in the spec row
            __base_value__: value of the base column in the spec row
        """
        base_col = columns_def.get("column_base", "")
        packages = columns_def.get("column_data", [])
        base_val = self._cell_value(row, base_col).strip()
        return [
            {
                "__column__":     pkg_col,
                "__value__":      self._cell_value(row, pkg_col).strip(),
                "__base_value__": base_val,
            }
            for pkg_col in packages
        ]

    def _split_rows_into_groups(self, rows: list, group_filter_expr: str,
                               columns_def: dict) -> list:
        """
        Apply the JQ group-filter expression to the full rows array, locate the
        matching header rows by position, then pair each header with the slice of
        rows that follow it until the next header.

        Returns: list of (group_row, [spec_rows_in_this_group])
        """
        try:
            matched = jq.first(group_filter_expr, rows) or []
        except Exception as e:
            write_log("error", f"JQ failed on group filter '{group_filter_expr}': {e}")
            return []

        label_col = columns_def.get("column_label", "")
        matched_keys = {self._cell_value(r, label_col) for r in matched}
        group_indices = [
            i for i, row in enumerate(rows)
            if self._cell_value(row, label_col) in matched_keys
        ]

        result = []
        for pos, idx in enumerate(group_indices):
            next_idx = group_indices[pos + 1] if pos + 1 < len(group_indices) else len(rows)
            result.append((rows[idx], rows[idx + 1 : next_idx]))
        return result

    # =========================================================================
    # FIELD / NAME RESOLVERS
    # =========================================================================

    def _build_fields(self, row: dict, fields_def: list, base_col: str = None) -> list:
        """Build the fields list for an item from its field definitions."""
        result = []
        for f in fields_def:
            if f.get("computed"):
                field = self._resolve_computed_field(row, f)
            else:
                resolved_value = self._resolve_field_value(
                    row, f["value"], f.get("transform"), base_col=base_col
                )
                if not resolved_value.strip() and "default" in f:
                    resolved_value = f["default"]
                if f.get("required") and not str(resolved_value).strip():
                    self._raise_required_field_error(f, row)
                field = {"name": f["name"], "value": resolved_value}
                if "type" in f:
                    field["type"] = self._resolve_field_type(resolved_value, f["type"])
            result.append(field)
        return result

    def _build_dynamic_fields(self, rows: list, dynamic_def: dict, row: dict = None) -> list:
        """
        Build fields from a list of rows where each row contributes one field.
        The field name comes from name_from column and value from value_from column.

        If dynamic_def contains a 'source' key, the rows are read from row[source]
        (a list embedded in the current row by a JQ filter) instead of scoped_rows.
        This avoids relying on scope_children and _find_row_positions.

        required:        when true, every generated field must have a non-empty value
                         (value_from column must be non-empty for every row).
        required_fields: list of attribute names (name_from values) that must appear
                         in the generated output with a non-empty value.
        Both raise RequiredFieldError with the offending row included in the log.
        """
        name_col        = dynamic_def.get("name_from", "")
        value_col       = dynamic_def.get("value_from", "")
        filter_expr     = dynamic_def.get("filter")
        type_name       = dynamic_def.get("type")
        source_key      = dynamic_def.get("source")
        required        = dynamic_def.get("required", False)
        required_fields = dynamic_def.get("required_fields", [])

        if source_key and row is not None:
            rows = row.get(source_key) or []

        if filter_expr:
            try:
                rows = jq.first(filter_expr, rows) or []
            except Exception as e:
                write_log("warning", f"JQ error on dynamic_fields filter '{filter_expr}': {e}")
                rows = []

        result = []
        for r in rows:
            name  = self._cell_value(r, name_col).strip()
            value = self._cell_value(r, value_col).strip()
            if not name:
                continue
            if required and not value:
                self._raise_required_field_error(
                    {"name": name, "value_from": value_col, "required": True},
                    r,
                )
            field = {"name": name, "value": value}
            if type_name:
                field["type"] = self._resolve_field_type(value, type_name)
            result.append(field)

        if required_fields:
            built = {f["name"]: f["value"] for f in result}
            for req_name in required_fields:
                if not str(built.get(req_name, "")).strip():
                    self._raise_required_field_error(
                        {"name": req_name, "required_fields": required_fields},
                        {},
                    )

        return result

    def _resolve_computed_field(self, row: dict, field_def: dict) -> dict:
        """
        Evaluate a computed field against the current row.

        Expects:
            computed: true
            condition: 'jq: <expression>'   — evaluated against the full row dict
            value:      <result when true>  — literal or column reference
            else_value: <result when false> — literal or column reference (optional)
            type:       <type resolver>     — optional
        """
        condition = field_def.get("condition", "")
        if not condition:
            write_log("warning", f"Computed field '{field_def['name']}' has no condition — skipping")
            return {"name": field_def["name"], "value": ""}

        try:
            result = jq.first(condition[3:].strip() if condition.startswith("jq:") else condition, row)
            matched = bool(result)
        except Exception as e:
            write_log("warning", f"Computed field '{field_def['name']}' condition error: {e}")
            matched = False

        raw_value = field_def["value"] if matched else field_def.get("else_value", "")
        resolved_value = str(raw_value) if not isinstance(raw_value, str) else raw_value

        if field_def.get("required") and not str(resolved_value).strip():
            self._raise_required_field_error(field_def, row)
        field = {"name": field_def["name"], "value": resolved_value}
        if "type" in field_def:
            field["type"] = self._resolve_field_type(resolved_value, field_def["type"])
        return field

    def _resolve_field_value(self, row: dict, value: str,
                              transform: str = None,
                              base_col: str = None) -> str:
        """
        Resolve a field value, handling special tokens and regex/JQ transforms.

        Special tokens:
            $base    → row[base_col]  (or row['__base_value__'] inside expand_variants)
            $variant → row['__value__']  (only meaningful inside expand_variants)
        """
        if value == "$base":
            val = str(row["__base_value__"]) if "__base_value__" in row else self._cell_value(row, base_col or "")
        elif value == "$variant":
            val = str(row.get("__value__", ""))
        else:
            val = self._cell_value(row, value)

        if transform:
            val = self._apply_transform(val, transform)
        return val

    def _resolve_item_name(self, row: dict, item_def: dict) -> str:
        """
        Resolve the display name for an item from the YAML definition.
        For normalized package rows with no explicit name strategy,
        defaults to the package column name stored in row['__column__'].
        """
        if item_def.get("name_static"):
            return item_def["name_static"]

        name = item_def.get("name")
        if name:
            if isinstance(name, str):
                return name
            val = self._cell_value(row, name["field"]).strip()
            if name.get("transform"):
                val = self._apply_transform(val, name["transform"])
            return val

        name_slug = item_def.get("name_slug")
        if name_slug:
            return self._slugify(self._cell_value(row, name_slug["field"]))

        # Normalized package rows: default name is the column name
        if "__column__" in row:
            return row["__column__"]

        return "unnamed"

    # =========================================================================
    # STATIC HELPERS
    # =========================================================================

    def _raise_required_field_error(self, field_def: dict, row: dict) -> None:
        """
        Log a detailed error and raise RequiredFieldError when a required field
        resolves to an empty, null, or undefined value.

        Evaluated after: value resolution, transform, default, and before type.
        Also evaluated after computed field condition resolution.
        """
        field_name = field_def.get("name", "<unknown>")
        msg = (
            f"[REQUIRED FIELD MISSING]\n"
            f"  Sheet      : {self._current_sheet}\n"
            f"  Field name : {field_name}\n"
            f"  Field def  : {field_def}\n"
            f"  Row data   : {row}"
        )
        write_log("error", msg)
        raise RequiredFieldError(msg)

    def _resolve_field_type(self, value: str, type_name: str) -> str:
        """Dispatch to the named type resolver function. Returns 'undefined' if unknown."""
        resolver = self._TYPE_RESOLVERS.get(type_name)
        if resolver is None:
            write_log("warning", f"Unknown type resolver: '{type_name}'")
            return "undefined"
        return resolver(value)

    @staticmethod
    def _get_type(value: str) -> str:
        """
        Infer a semantic type from a resolved string value.

        Returns:
            "number"  — numeric value (int or float, including negatives)
            "boolean" — availability / yes-no value
            "string"  — any other non-empty text
            "undefined" — empty or whitespace-only
        """
        v = str(value).strip()
        if not v:
            return "undefined"

        # Number: optional sign, digits, optional decimal, optional trailing non-numeric
        # e.g. "150", "2.0", "-40", "1,234" (with comma-stripping)
        if re.fullmatch(r"-?\d[\d,]*(\.\d+)?", v.replace(",", "")):
            return "number"

        # Boolean / availability markers (case-insensitive)
        _BOOL_VALUES = {True, False, "yes", "no", "true", "false", "✓", "✗"}
        if v.lower() in _BOOL_VALUES:
            return "boolean"

        return "string"

    _TYPE_RESOLVERS: dict = {
        "getType": _get_type.__func__,
    }

    @staticmethod
    def _cell_value(row: dict, col: str) -> str:
        """Extract the string value from a cell object {"value": ...} or plain string."""
        v = row.get(col, "")
        return str(v.get("value", "")) if isinstance(v, dict) else str(v)

    @staticmethod
    def _find_row_positions(selected: list, context: list) -> list:
        """Return the index in context of each row in selected (sequential scan)."""
        positions = []
        search_start = 0
        for sel in selected:
            for i in range(search_start, len(context)):
                if context[i] == sel:
                    positions.append(i)
                    search_start = i + 1
                    break
        return positions

    @staticmethod
    def _apply_transform(value: str, transform: str) -> str:
        """
        Dispatch a transform expression to JQ or regex based on prefix.

        Prefix rules:
            'jq: <expression>'  → execute expression as a JQ program on the scalar string
            '<pattern>'         → treat as a regex; return first capture group
        """
        if transform.startswith("jq:"):
            return Transformers._apply_jq_transform(value, transform[3:].strip())
        return Transformers._extract_pattern(value, transform)

    @staticmethod
    def _apply_jq_transform(value: str, expression: str) -> str:
        """Apply a JQ expression to a scalar string value. Returns original on error."""
        try:
            result = jq.first(expression, value)
            return str(result).strip() if result is not None else ""
        except Exception as e:
            write_log("warning", f"JQ transform error on expression '{expression}': {e}")
            return str(value).strip()

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