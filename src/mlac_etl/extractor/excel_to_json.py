import json
import re
import warnings
import openpyxl
from openpyxl.cell.cell import MergedCell
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from mlac_etl.logger import write_log

_RE_AUTHOR_TIMESTAMP = re.compile(r".+\s{2,}\(\d{4}-\d{2}-\d{2}")


class ExcelToJson:
    """
    Transforms an Excel file to a JSON representation.

    Every cell value is represented as an object:
        { "value": "<cell text>" }

    If the cell has a comment, one additional key is added:
        { "value": "...", "annotation": "<comment text>" }
    """

    def __init__(self, excel_path: str) -> None:
        """Store the input path and resolve the timestamped output directory."""
        self.today = datetime.today()
        self.excel_path = excel_path
        self.output_path = (
            Path("output/extraction")
            / self.today.strftime("%Y")
            / self.today.strftime("%m")
            / self.today.strftime("%d")
        )

    def run(self) -> str:
        """Load the workbook, process every sheet, and write the output JSON. Returns the output file path."""
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
                wb = openpyxl.load_workbook(self.excel_path)
        except Exception as e:
            write_log("error", f"Failed to read Excel file '{self.excel_path}': {e}")
            raise

        raw_data = {}

        for sheet_name in wb.sheetnames:
            try:
                ws = wb[sheet_name]
                rows, group_id_map = self._build_sheet_rows(ws)
                raw_data[sheet_name] = self._collapse_groups(rows, group_id_map)
            except Exception as e:
                write_log("error", f"Failed to process sheet '{sheet_name}' in '{self.excel_path}': {e}")
                raise

        try:
            self.output_path.mkdir(parents=True, exist_ok=True)
            raw_file = self.output_path / (Path(self.excel_path).stem + "-" + self.today.strftime("%H-%M-%S") + ".json")
            raw_file.write_text(
                json.dumps({"workbook": raw_data}, indent=2),
                encoding="utf-8"
            )
        except Exception as e:
            write_log("error", f"Failed to write JSON output for '{self.excel_path}': {e}")
            raise

        return str(raw_file)

    def _build_sheet_rows(self, ws) -> tuple:
        """Read a worksheet into row dicts and a group_id_map for merged-row grouping.

        Two-pass approach: first pass handles plain cells; second pass propagates
        merged-range values and links rows that share a vertical merge into a group.
        """
        if ws.max_row < 2:
            return [], {}

        headers = [
            str(cell.value) if cell.value is not None else None
            for cell in ws[1]
        ]

        n_rows = ws.max_row - 1
        rows = [{} for _ in range(n_rows)]

        # Pre-build set of merged top-left coordinates to avoid building them twice
        merged_top_lefts = {(r.min_row, r.min_col) for r in ws.merged_cells.ranges}

        # First pass: populate non-merged cells
        for df_row, row_cells in enumerate(ws.iter_rows(min_row=2)):
            excel_row = df_row + 2
            for col_idx, cell in enumerate(row_cells, start=1):
                header = headers[col_idx - 1]
                if not header or header.startswith("Unnamed"):
                    continue
                if not isinstance(cell, MergedCell) and (excel_row, col_idx) not in merged_top_lefts:
                    rows[df_row][header] = self._build_cell_object(cell)

        # Second pass: resolve merged ranges and build group_id_map
        group_id_map = {i: i for i in range(n_rows)}
        for merged_range in ws.merged_cells.ranges:
            top_left = ws.cell(merged_range.min_row, merged_range.min_col)
            cell_obj = self._build_cell_object(top_left)
            first_df_row = None
            for excel_row in range(merged_range.min_row, merged_range.max_row + 1):
                df_row = excel_row - 2
                if 0 <= df_row < n_rows:
                    for col in range(merged_range.min_col, merged_range.max_col + 1):
                        header = headers[col - 1]
                        if not header or header.startswith("Unnamed"):
                            continue
                        rows[df_row][header] = cell_obj
                    if merged_range.max_row > merged_range.min_row:
                        if first_df_row is None:
                            first_df_row = df_row
                        else:
                            group_id_map[df_row] = group_id_map[first_df_row]

        return rows, group_id_map

    def _build_cell_object(self, cell) -> dict:
        """Return `{"value": ...}`, adding `"annotation"` only when a comment is present."""
        obj = {"value": self._clean_value(cell.value)}
        if cell.comment and cell.comment.text:
            obj["annotation"] = self._parse_comment(cell.comment.text, cell.comment.author)
        return obj

    def _collapse_groups(self, rows: list, group_id_map: dict) -> list:
        """Merge rows that share the same group_id (vertical merge group) into a single record."""
        groups = defaultdict(list)
        for i, row in enumerate(rows):
            groups[group_id_map[i]].append(row)

        result = []
        for group_id in sorted(groups.keys()):
            group_rows = groups[group_id]

            seen_keys = dict.fromkeys(k for r in group_rows for k in r)
            merged = {
                key: self._merge_cell_objects([r[key] for r in group_rows if key in r])
                for key in seen_keys
            }
            result.append(merged)
        return result

    def _merge_cell_objects(self, objects: list) -> dict:
        """Combine cell objects from the same group: unique non-empty values joined, first annotation wins."""
        seen_values = {}  # ordered set: key = value string, insertion order preserved
        annotation = None

        for obj in objects:
            v = obj.get("value", "").strip()
            if v and v != "nan" and v not in seen_values:
                seen_values[v] = None
            if annotation is None and obj.get("annotation"):
                annotation = obj["annotation"]

        result = {"value": ", ".join(seen_values)}
        if annotation:
            result["annotation"] = annotation
        return result

    @staticmethod
    def _parse_comment(text: str, author: str = "") -> str:
        """Strip Excel metadata from a comment and return only the body text.

        Handles threaded (modern) and classic (`Author:\\ntext`) comment formats.
        """
        text = (text or "").strip()

        # Threaded comment format
        if "======" in text:
            return "\n".join(
                line for line in text.split("\n")
                if not line.startswith("======")
                and not line.startswith("ID#")
                and not _RE_AUTHOR_TIMESTAMP.match(line)
            ).strip()

        # Classic comment format: "Author:\ntext"
        if author:
            prefix = f"{author}:\n"
            if text.startswith(prefix):
                text = text[len(prefix):]
        return text.strip()

    @staticmethod
    def _clean_value(value) -> str:
        """Convert a cell value to a clean string."""
        if value is None:
            return ""
        return str(value).strip()
