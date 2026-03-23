import json
import pytest
from pathlib import Path
from mlac_etl.params_validator import (
    validate_excel,
    validate_json,
    validate_yaml,
    validate_json_and_yaml,
    validate_args,
)

MOCK_DIR = Path(__file__).parent.parent.parent.parent / "mock"


# =============================================================================
# validate_excel
# =============================================================================

def test_validate_excel_valid():
    errors = validate_excel(str(MOCK_DIR / "input.xlsx"))
    assert errors == []


def test_validate_excel_not_found():
    errors = validate_excel("nonexistent.xlsx")
    assert len(errors) == 1
    assert "not found" in errors[0]


def test_validate_excel_wrong_extension(tmp_path):
    f = tmp_path / "file.csv"
    f.write_text("data")
    errors = validate_excel(str(f))
    assert len(errors) == 1
    assert "extension" in errors[0]


# =============================================================================
# validate_json
# =============================================================================

def test_validate_json_valid():
    errors = validate_json(str(MOCK_DIR / "mock_data.json"))
    assert errors == []


def test_validate_json_not_found():
    errors = validate_json("nonexistent.json")
    assert len(errors) == 1
    assert "not found" in errors[0]


def test_validate_json_wrong_extension(tmp_path):
    f = tmp_path / "file.txt"
    f.write_text("{}")
    errors = validate_json(str(f))
    assert len(errors) == 1
    assert "extension" in errors[0]


def test_validate_json_invalid_syntax(tmp_path):
    f = tmp_path / "bad.json"
    f.write_text("{not valid json}")
    errors = validate_json(str(f))
    assert len(errors) == 1
    assert "Invalid JSON" in errors[0]


def test_validate_json_not_a_dict(tmp_path):
    f = tmp_path / "array.json"
    f.write_text("[1, 2, 3]")
    errors = validate_json(str(f))
    assert len(errors) == 1
    assert "object" in errors[0]


def test_validate_json_missing_workbook_key(tmp_path):
    f = tmp_path / "no_wb.json"
    f.write_text(json.dumps({"data": {}}))
    errors = validate_json(str(f))
    assert len(errors) == 1
    assert "workbook" in errors[0]


def test_validate_json_workbook_not_a_dict(tmp_path):
    f = tmp_path / "bad_wb.json"
    f.write_text(json.dumps({"workbook": [1, 2, 3]}))
    errors = validate_json(str(f))
    assert len(errors) == 1
    assert "workbook" in errors[0]


# =============================================================================
# validate_yaml
# =============================================================================

def test_validate_yaml_valid():
    errors = validate_yaml(str(MOCK_DIR / "rules.yaml"))
    assert errors == []


def test_validate_yaml_not_found():
    errors = validate_yaml("nonexistent.yaml")
    assert len(errors) == 1
    assert "not found" in errors[0]


def test_validate_yaml_wrong_extension(tmp_path):
    f = tmp_path / "rules.txt"
    f.write_text("sheets:\n  s1: {}")
    errors = validate_yaml(str(f))
    assert len(errors) == 1
    assert "extension" in errors[0]


def test_validate_yaml_invalid_syntax(tmp_path):
    f = tmp_path / "bad.yaml"
    f.write_text("sheets: [\nunclosed")
    errors = validate_yaml(str(f))
    assert len(errors) == 1
    assert "Invalid YAML" in errors[0]


def test_validate_yaml_not_a_dict(tmp_path):
    f = tmp_path / "list.yaml"
    f.write_text("- item1\n- item2\n")
    errors = validate_yaml(str(f))
    assert len(errors) == 1
    assert "mapping" in errors[0]


def test_validate_yaml_missing_sheets_key(tmp_path):
    f = tmp_path / "no_sheets.yaml"
    f.write_text("input:\n  workbook_key: workbook\n")
    errors = validate_yaml(str(f))
    assert len(errors) == 1
    assert "sheets" in errors[0]


def test_validate_yaml_sheets_empty(tmp_path):
    f = tmp_path / "empty_sheets.yaml"
    f.write_text("sheets: {}\n")
    errors = validate_yaml(str(f))
    assert len(errors) == 1
    assert "sheets" in errors[0]


def test_validate_yaml_sheet_not_a_dict(tmp_path):
    f = tmp_path / "bad_sheet.yaml"
    f.write_text("sheets:\n  mySheet: not_a_dict\n")
    errors = validate_yaml(str(f))
    assert len(errors) == 1
    assert "mySheet" in errors[0]


def test_validate_yaml_yml_extension_accepted(tmp_path):
    f = tmp_path / "rules.yml"
    f.write_text("sheets:\n  s1: {}\n")
    errors = validate_yaml(str(f))
    assert errors == []


# =============================================================================
# validate_json_and_yaml / validate_args
# =============================================================================

def test_validate_json_and_yaml_both_valid():
    errors = validate_json_and_yaml(
        str(MOCK_DIR / "mock_data.json"),
        str(MOCK_DIR / "rules.yaml"),
    )
    assert errors == []


def test_validate_json_and_yaml_combined_errors():
    errors = validate_json_and_yaml("bad.json", "bad.yaml")
    assert len(errors) == 2


def test_validate_args_valid():
    errors = validate_args(
        str(MOCK_DIR / "input.xlsx"),
        str(MOCK_DIR / "rules.yaml"),
    )
    assert errors == []


def test_validate_args_both_invalid():
    errors = validate_args("bad.xlsx", "bad.yaml")
    assert len(errors) == 2
