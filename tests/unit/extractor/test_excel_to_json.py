import json
import pytest
from pathlib import Path
from unittest.mock import MagicMock, patch
from mlac_etl.extractor.excel_to_json import ExcelToJson

MOCK_DIR = Path(__file__).parent.parent.parent.parent / "mock"
EXCEL_PATH = str(MOCK_DIR / "input.xlsx")


# =============================================================================
# run()
# =============================================================================

def test_run_returns_existing_path():
    output = ExcelToJson(EXCEL_PATH).run()
    assert isinstance(output, str)
    assert Path(output).exists()


def test_run_output_is_valid_json():
    output = ExcelToJson(EXCEL_PATH).run()
    data = json.loads(Path(output).read_text(encoding="utf-8"))
    assert isinstance(data, dict)


def test_run_output_has_workbook_key():
    output = ExcelToJson(EXCEL_PATH).run()
    data = json.loads(Path(output).read_text(encoding="utf-8"))
    assert "workbook" in data


def test_run_all_sheets_present():
    output = ExcelToJson(EXCEL_PATH).run()
    data = json.loads(Path(output).read_text(encoding="utf-8"))
    workbook = data["workbook"]
    assert "specs" in workbook
    assert "updates" in workbook


def test_run_raises_on_bad_path():
    with pytest.raises(Exception):
        ExcelToJson("nonexistent.xlsx").run()


def test_run_raises_on_write_failure(tmp_path):
    extractor = ExcelToJson(EXCEL_PATH)
    extractor.output_path = tmp_path / "locked" / "nested"
    with patch.object(extractor.output_path.__class__, "mkdir", side_effect=OSError("disk full")):
        with pytest.raises(OSError):
            extractor.run()


# =============================================================================
# Sheet rows — structure
# =============================================================================

def test_every_cell_has_value_key():
    output = ExcelToJson(EXCEL_PATH).run()
    workbook = json.loads(Path(output).read_text(encoding="utf-8"))["workbook"]
    for sheet_rows in workbook.values():
        for row in sheet_rows:
            for cell in row.values():
                assert "value" in cell


def test_unnamed_columns_not_present():
    output = ExcelToJson(EXCEL_PATH).run()
    workbook = json.loads(Path(output).read_text(encoding="utf-8"))["workbook"]
    for sheet_rows in workbook.values():
        for row in sheet_rows:
            for col_name in row.keys():
                assert not col_name.startswith("Unnamed")
                assert col_name is not None


def test_merged_cells_collapse_to_single_row():
    extractor = ExcelToJson(EXCEL_PATH)
    rows = [
        {"Cat": {"value": "Engine"}, "Item": {"value": ""}},
        {"Cat": {"value": ""},       "Item": {"value": "Displacement"}},
        {"Cat": {"value": ""},       "Item": {"value": "Horsepower"}},
    ]
    group_id_map = {0: 0, 1: 0, 2: 0}
    result = extractor._collapse_groups(rows, group_id_map)
    assert len(result) == 1
    assert result[0]["Item"]["value"] == "Displacement, Horsepower"


def test_annotation_present_on_annotated_cell():
    from unittest.mock import MagicMock
    extractor = ExcelToJson(EXCEL_PATH)
    mock_cell = MagicMock()
    mock_cell.value = "test"
    mock_cell.comment.text = "======\nID#ABC\nAuthor  (2026-01-01)\nAnnotation body"
    mock_cell.comment.author = "Author"
    result = extractor._build_cell_object(mock_cell)
    assert "annotation" in result
    assert result["annotation"] == "Annotation body"


def test_annotation_absent_on_plain_cell():
    from unittest.mock import MagicMock
    extractor = ExcelToJson(EXCEL_PATH)
    mock_cell = MagicMock()
    mock_cell.value = "plain value"
    mock_cell.comment = None
    result = extractor._build_cell_object(mock_cell)
    assert "annotation" not in result


def test_empty_cell_value_is_empty_string():
    output = ExcelToJson(EXCEL_PATH).run()
    workbook = json.loads(Path(output).read_text(encoding="utf-8"))["workbook"]
    empty_values = [
        cell["value"]
        for row in workbook["specs"]
        for cell in row.values()
        if cell["value"] == ""
    ]
    assert len(empty_values) > 0


def test_numeric_cell_value_is_string():
    output = ExcelToJson(EXCEL_PATH).run()
    workbook = json.loads(Path(output).read_text(encoding="utf-8"))["workbook"]
    for sheet_rows in workbook.values():
        for row in sheet_rows:
            for cell in row.values():
                assert isinstance(cell["value"], str)


# =============================================================================
# _build_sheet_rows() — edge cases
# =============================================================================

def test_empty_sheet_returns_no_rows():
    extractor = ExcelToJson(EXCEL_PATH)
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    # Only a header row — max_row == 1
    ws.append(["ColA", "ColB"])
    rows, group_id_map = extractor._build_sheet_rows(ws)
    assert rows == []
    assert group_id_map == {}


def test_sheet_with_only_header_and_one_row():
    extractor = ExcelToJson(EXCEL_PATH)
    import openpyxl
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["Name", "Value"])
    ws.append(["Alice", "42"])
    rows, _ = extractor._build_sheet_rows(ws)
    assert len(rows) == 1
    assert rows[0]["Name"]["value"] == "Alice"


# =============================================================================
# _merge_cell_objects()
# =============================================================================

def test_merge_deduplicates_values():
    extractor = ExcelToJson(EXCEL_PATH)
    result = extractor._merge_cell_objects([
        {"value": "hello"},
        {"value": "hello"},
        {"value": "world"},
    ])
    assert result["value"] == "hello, world"


def test_merge_skips_nan():
    extractor = ExcelToJson(EXCEL_PATH)
    result = extractor._merge_cell_objects([
        {"value": "nan"},
        {"value": "real"},
    ])
    assert result["value"] == "real"


def test_merge_first_annotation_wins():
    extractor = ExcelToJson(EXCEL_PATH)
    result = extractor._merge_cell_objects([
        {"value": "a", "annotation": "first"},
        {"value": "b", "annotation": "second"},
    ])
    assert result["annotation"] == "first"


# =============================================================================
# _parse_comment()
# =============================================================================

def test_parse_comment_threaded_strips_metadata():
    text = "======\nID#ABC123\nAlexander Wong  (2026-01-01)\nActual comment body"
    result = ExcelToJson._parse_comment(text, "Alexander Wong")
    assert result == "Actual comment body"
    assert "======" not in result
    assert "ID#" not in result


def test_parse_comment_classic_strips_author_prefix():
    result = ExcelToJson._parse_comment("John:\nThe real content", author="John")
    assert result == "The real content"


def test_parse_comment_no_author_returns_text():
    result = ExcelToJson._parse_comment("plain text", author="")
    assert result == "plain text"


def test_parse_comment_empty_returns_empty():
    result = ExcelToJson._parse_comment("", author="")
    assert result == ""


# =============================================================================
# _clean_value()
# =============================================================================

def test_clean_value_none_returns_empty_string():
    assert ExcelToJson._clean_value(None) == ""


def test_clean_value_strips_whitespace():
    assert ExcelToJson._clean_value("  hello  ") == "hello"


def test_clean_value_numeric_becomes_string():
    assert ExcelToJson._clean_value(42) == "42"
