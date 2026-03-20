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
    """Interprets a declarative YAML specification to transform a flat JSON workbook into Sitecore-ready output."""

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

    def run(self) -> str:
        """Execute the transformation pipeline."""
        cfg = self._load_yaml()
        raw = self._load_raw_json()

        input_cfg    = cfg.get("input", {})
        workbook_key = input_cfg.get("workbook_key", "workbook")
        sheets_def   = cfg.get("sheets", {})

        sheet_names = input_cfg.get("sheets", list(sheets_def.keys()))

        write_log("info", f"Sheets to process: {sheet_names}")

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
        """Locate and return the row list for a sheet from the raw JSON workbook."""
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

    # =========================================================================
    # SHEET PROCESSOR
    # =========================================================================

    def _process_sheet(self, sheet_spec: dict, flat_rows: list, sheet_name: str) -> dict:
        """Apply a sheet's YAML definition to its flat rows and return the output dict."""
        self._current_sheet = sheet_name
        sitecore_config = self._build_sitecore_config(sheet_spec.get("sitecore_config", {}))
        relations       = sitecore_config.pop("relations", {})
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

        return {"sitecoreConfig": sitecore_config, "relations": relations, "items": output_items}

    def _build_sitecore_config(self, raw_cfg: dict) -> dict:
        dictionaries = raw_cfg.get("dictionaries", {})
        return {
            "rootPath":       raw_cfg.get("rootPath", ""),
            "importStrategy": raw_cfg.get("importStrategy", {}),
            "backupStrategy": raw_cfg.get("backupStrategy", {}),
            "templates":      dictionaries.get("templates", {}),
            "relations":      dictionaries.get("relations", {}),
        }

    # =========================================================================
    # RECURSIVE ITEM BUILDER
    # =========================================================================

    def _build_items(self, context: list, items_def: list,
                     columns_def: dict, parent_row: dict = None) -> list:
        """Recursively build output items from a context list and item definitions."""
        output = []
        for item_def in items_def:
            output.extend(self._build_item_list(context, item_def, columns_def, parent_row))
        return output

    def _build_item_list(self, context: list, item_def: dict,
                         columns_def: dict, parent_row: dict = None) -> list:
        """Process one item definition and return the list of built items."""
        filter_expr     = item_def.get("filter", "")
        expand_variants = item_def.get("expand_variants", False)

        if expand_variants:
            jq_input = self._normalize_package_rows(parent_row or {}, columns_def)
        else:
            jq_input = context

        if item_def.get("templateKey") == "group":
            return self._build_group_items(jq_input, filter_expr, item_def, columns_def)

        try:
            selected = jq.first(filter_expr, jq_input) or []
        except Exception as e:
            write_log("warning", f"JQ error on filter '{filter_expr}': {e}")
            selected = []


        scope_children = item_def.get("scope_children", False)
        positions = self._find_row_positions(selected, jq_input) if scope_children else None

        result = []
        for idx, row in enumerate(selected):
            if scope_children:
                pos_start   = positions[idx]
                pos_end     = positions[idx + 1] if idx + 1 < len(positions) else len(jq_input)
                scoped_rows = jq_input[pos_start:pos_end]
            else:
                scoped_rows = [row]
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
        return result

    def _build_group_items(self, rows: list, filter_expr: str,
                           item_def: dict, columns_def: dict) -> list:
        """Handle group-level items with row scoping via _split_rows_into_groups."""
        groups = self._split_rows_into_groups(rows, filter_expr)
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
        if "__base_cell__" not in row:
            row = {**row, "__base_cell__": row.get(base_col, {})}
        fields = self._build_fields(row, item_def.get("fields", []), base_col=base_col)
        if item_def.get("dynamic_fields") is not None:
            fields += self._build_dynamic_fields(scoped_rows or [row], item_def["dynamic_fields"], row=row)
        return {
            "name":        self._resolve_item_name(row, item_def),
            "templateKey": item_def["templateKey"],
            "fields":      fields,
        }

    def _normalize_package_rows(self, row: dict, columns_def: dict) -> list:
        """Expand a spec row into one normalized variant dict per column_data entry."""
        base_col = columns_def.get("column_base", "")
        packages = columns_def.get("column_data", [])
        return [
            {
                "__column__":    pkg_col,
                "__cell__":      row.get(pkg_col, {}),
                "__base_cell__": row.get(base_col, {}),
            }
            for pkg_col in packages
        ]

    def _split_rows_into_groups(self, rows: list, group_filter_expr: str) -> list:
        """Pair each JQ-matched group header with the rows that follow it until the next header."""
        try:
            matched = jq.first(group_filter_expr, rows) or []
        except Exception as e:
            write_log("error", f"JQ failed on group filter '{group_filter_expr}': {e}")
            return []

        group_indices = self._find_row_positions(matched, rows)

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
                field = self._resolve_computed_field(row, f, base_col=base_col)
                if field is None:
                    continue
            else:
                resolved_value = self._resolve_field_value(
                    row, f["value"], f.get("transform"), base_col=base_col
                )
                if not resolved_value.strip() and "default" in f:
                    resolved_value = f["default"]
                if f.get("required") and not str(resolved_value).strip():
                    self._raise_required_field_error(f, row)
                try:
                    parsed = json.loads(resolved_value)
                    value_out = parsed if isinstance(parsed, (list, dict)) else resolved_value
                except (json.JSONDecodeError, TypeError, ValueError):
                    value_out = resolved_value
                field = {"name": f["name"], "value": value_out}
                if "type" in f:
                    field["type"] = self._resolve_field_type(resolved_value, f["type"])
                if "relationKey" in f:
                    field["relationKey"] = f["relationKey"]
            result.append(field)
        return result

    def _build_dynamic_fields(self, rows: list, dynamic_def: dict, row: dict = None) -> list:
        """Generate fields from scoped rows where each row contributes one name/value pair."""
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

    def _resolve_computed_field(self, row: dict, field_def: dict, base_col: str = None) -> dict:
        """Evaluate a computed field's JQ condition and resolve value or else_value accordingly."""
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

        if not matched and field_def.get("omit_if_false"):
            return None

        raw_value = field_def["value"] if matched else field_def.get("else_value", "")
        if isinstance(raw_value, bool):
            resolved_value = raw_value
        else:
            transform = field_def.get("transform") if matched else None
            resolved_value = self._resolve_field_value(row, str(raw_value), transform, base_col=base_col)

        if field_def.get("required") and not str(resolved_value).strip():
            self._raise_required_field_error(field_def, row)
        field = {"name": field_def["name"], "value": resolved_value}
        if "type" in field_def:
            field["type"] = self._resolve_field_type(str(resolved_value), field_def["type"])
        if "relationKey" in field_def:
            field["relationKey"] = field_def["relationKey"]
        return field

    def _resolve_field_value(self, row: dict, value: str,
                              transform: str = None,
                              base_col: str = None) -> str:
        """Resolve a field value token or column reference, then apply any transform."""
        if value == "$base":
            val = str(row["__base_cell__"].get("value", "")) if "__base_cell__" in row else self._cell_value(row, base_col or "")
        elif value == "$base_annotation":
            val = str(row["__base_cell__"].get("annotation", "") or "") if "__base_cell__" in row else ""
        elif value == "$variant":
            val = str(row["__cell__"].get("value", "")) if "__cell__" in row else ""
        elif value == "$variant_annotation":
            val = str(row["__cell__"].get("annotation", "") or "") if "__cell__" in row else ""
        elif value.startswith("$annotation:"):
            col = value[12:]
            v = row.get(col, {})
            val = str(v.get("annotation", "")) if isinstance(v, dict) else ""
        else:
            val = self._cell_value(row, value)

        if transform:
            val = self._apply_transform(val, transform)
        return val

    def _resolve_item_name(self, row: dict, item_def: dict) -> str:
        """Resolve the item name from name_static, name field, name_slug, or __column__ fallback."""
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
        """Log a detailed error and raise RequiredFieldError for an empty required field."""
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
        """Run a named type resolver if one exists, otherwise return type_name as a literal."""
        resolver = self._TYPE_RESOLVERS.get(type_name)
        if resolver is None:
            return type_name
        return resolver(value)

    _BOOL_VALUES: frozenset = frozenset({"yes", "no", "true", "false", "✓", "✗"})

    @staticmethod
    def _get_type(value: str) -> str:
        """Infer number, boolean, or string from a resolved value."""
        v = str(value).strip()
        if not v:
            return "string"
        if re.fullmatch(r"-?\d[\d,]*(\.\d+)?", v.replace(",", "")):
            return "number"
        if v.lower() in Transformers._BOOL_VALUES:
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
        """Dispatch to JQ transform (jq: prefix) or regex first-capture-group."""
        if transform.startswith("jq:"):
            return Transformers._apply_jq_transform(value, transform[3:].strip())
        return Transformers._extract_pattern(value, transform)

    @staticmethod
    def _apply_jq_transform(value: str, expression: str) -> str:
        """Apply a JQ expression to a scalar string value. Returns original on error."""
        try:
            result = jq.first(expression, value)
            if result is None:
                return ""
            if isinstance(result, (list, dict)):
                return json.dumps(result)
            return str(result).strip()
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