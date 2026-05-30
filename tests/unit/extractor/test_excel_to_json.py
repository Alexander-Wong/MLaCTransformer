import json
import openpyxl
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
                if isinstance(cell, dict):
                    assert "value" in cell


def test_unnamed_columns_not_present():
    output = ExcelToJson(EXCEL_PATH).run()
    workbook = json.loads(Path(output).read_text(encoding="utf-8"))["workbook"]
    for sheet_rows in workbook.values():
        for row in sheet_rows:
            for col_name in row.keys():
                assert not str(col_name).startswith("Unnamed")
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
    extractor = ExcelToJson(EXCEL_PATH)
    mock_cell = MagicMock()
    mock_cell.value = "test"
    mock_cell.comment.text = "======\nID#ABC\nAuthor  (2026-01-01)\nAnnotation body"
    mock_cell.comment.author = "Author"
    result = extractor._build_cell_object(mock_cell)
    assert "annotation" in result
    assert result["annotation"] == "Annotation body"


def test_annotation_absent_on_plain_cell():
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
        if isinstance(cell, dict) and cell["value"] == ""
    ]
    assert len(empty_values) > 0


def test_numeric_cell_value_is_string():
    output = ExcelToJson(EXCEL_PATH).run()
    workbook = json.loads(Path(output).read_text(encoding="utf-8"))["workbook"]
    for sheet_rows in workbook.values():
        for row in sheet_rows:
            for cell in row.values():
                if isinstance(cell, dict):
                    assert isinstance(cell["value"], str)


# =============================================================================
# _build_sheet_rows() — edge cases
# =============================================================================

def test_empty_sheet_returns_no_rows():
    extractor = ExcelToJson(EXCEL_PATH)
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.append(["ColA", "ColB"])
    rows, group_id_map = extractor._build_sheet_rows(ws)
    assert rows == []
    assert group_id_map == {}


def test_sheet_with_only_header_and_one_row():
    extractor = ExcelToJson(EXCEL_PATH)
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


# =============================================================================
# _normalize_header() — header cleaning
# =============================================================================

def test_normalize_header_preserves_key_suffix():
    assert ExcelToJson._normalize_header("Powertrain ID (Key)") == "Powertrain ID (Key)"


def test_normalize_header_strips_trailing_hint():
    assert ExcelToJson._normalize_header("Revision Date (this is a hint)") == "Revision Date"


def test_normalize_header_strips_multiple_trailing_parens():
    assert ExcelToJson._normalize_header("Name (hint1) (hint2)") == "Name"


def test_normalize_header_does_not_strip_leading_paren():
    assert ExcelToJson._normalize_header("(hint) Revision Date") == "(hint) Revision Date"


def test_normalize_header_strips_common_suffixes():
    assert ExcelToJson._normalize_header("Type (Drop)") == "Type"
    assert ExcelToJson._normalize_header("Details (Modal Copy)") == "Details"
    assert ExcelToJson._normalize_header("CTA Link (updated by content team)") == "CTA Link"


def test_normalize_header_map_strips_hint_keeps_slug():
    assert ExcelToJson._normalize_header("[MAP] C-001 (link to colors)") == "[MAP] C-001"


def test_normalize_header_map_no_hint_unchanged():
    assert ExcelToJson._normalize_header("[MAP] C-001") == "[MAP] C-001"


def test_normalize_header_map_preserves_prefix_case():
    assert ExcelToJson._normalize_header("[MAP] model-a-spec (Reference)") == "[MAP] model-a-spec"


def test_normalize_header_none_returns_none():
    assert ExcelToJson._normalize_header(None) is None


def test_normalize_header_unnamed_unchanged():
    assert ExcelToJson._normalize_header("Unnamed: 0") == "Unnamed: 0"


def test_normalize_header_plain_no_parens_unchanged():
    assert ExcelToJson._normalize_header("Engine") == "Engine"


def test_normalize_header_key_with_extra_hint_preserves_key():
    assert ExcelToJson._normalize_header("Color ID (Key)") == "Color ID (Key)"


# =============================================================================
# _apply_universal_matrix_mapping() — MAP engine
# =============================================================================

def _make_extractor():
    return ExcelToJson(EXCEL_PATH)


def test_map_injects_mapped_rows():
    extractor = _make_extractor()
    raw = {
        "colors": [
            {"Color ID (Key)": {"value": "C-001"}, "Name": {"value": "Red"}},
            {"Color ID (Key)": {"value": "C-002"}, "Name": {"value": "Blue"}},
        ],
        "cars": [
            {"Car": {"value": "Sedan"}, "[MAP] C-001": {"value": "YES"}, "[MAP] C-002": {"value": ""}},
            {"Car": {"value": "Coupe"}, "[MAP] C-001": {"value": "YES"}, "[MAP] C-002": {"value": "YES"}},
        ],
    }
    result = extractor._apply_universal_matrix_mapping(raw)
    assert "__MAPPED_cars" in result["colors"][0]
    assert "__MAPPED_cars" in result["colors"][1]
    mapped_c001 = [r["Car"]["value"] for r in result["colors"][0]["__MAPPED_cars"]]
    mapped_c002 = [r["Car"]["value"] for r in result["colors"][1]["__MAPPED_cars"]]
    assert "Sedan" in mapped_c001 and "Coupe" in mapped_c001
    assert mapped_c002 == ["Coupe"]


def test_map_no_map_columns_returns_flat():
    extractor = _make_extractor()
    raw = {"sheet1": [{"Name": {"value": "Alice"}}]}
    result = extractor._apply_universal_matrix_mapping(raw)
    assert "__MAPPED_sheet1" not in result["sheet1"][0]


def test_map_falsy_values_not_matched():
    extractor = _make_extractor()
    raw = {
        "keys": [{"ID (Key)": {"value": "X"}}],
        "source": [
            {"Item": {"value": "A"}, "[MAP] X": {"value": "NO"}},
            {"Item": {"value": "B"}, "[MAP] X": {"value": "0"}},
            {"Item": {"value": "C"}, "[MAP] X": {"value": "FALSE"}},
            {"Item": {"value": "D"}, "[MAP] X": {"value": "N"}},
            {"Item": {"value": "E"}, "[MAP] X": {"value": ""}},
        ],
    }
    result = extractor._apply_universal_matrix_mapping(raw)
    assert "__MAPPED_source" not in result["keys"][0]


def test_map_truthy_values_all_matched():
    extractor = _make_extractor()
    for val in ["YES", "X", "1", "TRUE", "Y"]:
        raw = {
            "keys": [{"ID (Key)": {"value": "K"}}],
            "src":  [{"Item": {"value": "A"}, "[MAP] K": {"value": val}}],
        }
        result = extractor._apply_universal_matrix_mapping(raw)
        assert "__MAPPED_src" in result["keys"][0], f"Expected match for value={val!r}"


def test_map_value_stored_in_mapped_row():
    extractor = _make_extractor()
    raw = {
        "keys": [{"ID (Key)": {"value": "K"}}],
        "src":  [{"Item": {"value": "A"}, "[MAP] K": {"value": "YES"}}],
    }
    result = extractor._apply_universal_matrix_mapping(raw)
    assert result["keys"][0]["__MAPPED_src"][0]["__MAP_VALUE__"] == "YES"


def test_map_unmatched_key_not_injected():
    extractor = _make_extractor()
    raw = {
        "keys": [{"ID (Key)": {"value": "MISSING"}}],
        "src":  [{"Item": {"value": "A"}, "[MAP] OTHER": {"value": "YES"}}],
    }
    result = extractor._apply_universal_matrix_mapping(raw)
    assert "__MAPPED_src" not in result["keys"][0]


def test_map_sheet_name_sanitized():
    extractor = _make_extractor()
    raw = {
        "my keys":   [{"ID (Key)": {"value": "K"}}],
        "my source": [{"Item": {"value": "A"}, "[MAP] K": {"value": "YES"}}],
    }
    result = extractor._apply_universal_matrix_mapping(raw)
    assert "__MAPPED_my_source" in result["my keys"][0]


# =============================================================================
# _resolve_cell_value() — formula evaluation
# =============================================================================

def test_resolve_returns_raw_value_when_no_evaluator():
    extractor = _make_extractor()
    extractor._evaluator = None
    cell = MagicMock()
    cell.value = 42
    assert extractor._resolve_cell_value(cell, "Sheet1") == 42


def test_resolve_returns_raw_value_for_non_formula():
    extractor = _make_extractor()
    extractor._evaluator = MagicMock()
    cell = MagicMock()
    cell.value = "plain text"
    assert extractor._resolve_cell_value(cell, "Sheet1") == "plain text"


def test_resolve_calls_evaluator_for_formula():
    extractor = _make_extractor()
    mock_eval = MagicMock()
    mock_eval.evaluate.return_value = "EVALUATED"
    extractor._evaluator = mock_eval
    cell = MagicMock()
    cell.value = "=UPPER(A1)"
    cell.coordinate = "B1"
    result = extractor._resolve_cell_value(cell, "Sheet1")
    assert result == "EVALUATED"
    mock_eval.evaluate.assert_called_once_with("Sheet1!B1")


def test_resolve_quotes_sheet_name_with_space():
    extractor = _make_extractor()
    mock_eval = MagicMock()
    mock_eval.evaluate.return_value = "OK"
    extractor._evaluator = mock_eval
    cell = MagicMock()
    cell.value = "=A1"
    cell.coordinate = "B1"
    extractor._resolve_cell_value(cell, "My Sheet")
    mock_eval.evaluate.assert_called_once_with("'My Sheet'!B1")


def test_resolve_quotes_sheet_name_with_apostrophe():
    extractor = _make_extractor()
    mock_eval = MagicMock()
    mock_eval.evaluate.return_value = "OK"
    extractor._evaluator = mock_eval
    cell = MagicMock()
    cell.value = "=A1"
    cell.coordinate = "B1"
    extractor._resolve_cell_value(cell, "O'Brien")
    mock_eval.evaluate.assert_called_once_with("'O'Brien'!B1")


def test_resolve_falls_back_to_raw_on_evaluator_error():
    extractor = _make_extractor()
    mock_eval = MagicMock()
    mock_eval.evaluate.side_effect = Exception("eval error")
    extractor._evaluator = mock_eval
    cell = MagicMock()
    cell.value = "=BAD_FORMULA()"
    cell.coordinate = "A1"
    result = extractor._resolve_cell_value(cell, "Sheet1")
    assert result == "=BAD_FORMULA()"
