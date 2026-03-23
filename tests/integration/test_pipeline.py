import json
import pytest
from pathlib import Path
from mlac_etl.extractor.excel_to_json import ExcelToJson
from mlac_etl.transformer.transform import Transformers

MOCK_DIR = Path(__file__).parent.parent.parent / "mock"


@pytest.fixture(scope="module")
def extracted_json():
    return ExcelToJson(str(MOCK_DIR / "input.xlsx")).run()


@pytest.fixture(scope="module")
def transformed_output(extracted_json):
    return Transformers(extracted_json, str(MOCK_DIR / "rules.yaml")).run()


@pytest.fixture(scope="module")
def transformed_data(transformed_output):
    return json.loads(Path(transformed_output).read_text(encoding="utf-8"))


# =============================================================================
# Stage 1 — Extractor
# =============================================================================

def test_extractor_output_file_exists(extracted_json):
    assert Path(extracted_json).exists()


def test_extractor_output_is_valid_json(extracted_json):
    data = json.loads(Path(extracted_json).read_text(encoding="utf-8"))
    assert isinstance(data, dict)


def test_extractor_output_has_workbook_key(extracted_json):
    data = json.loads(Path(extracted_json).read_text(encoding="utf-8"))
    assert "workbook" in data


def test_extractor_output_path_is_timestamped(extracted_json):
    import re
    assert re.search(r"\d{4}/\d{2}/\d{2}/", extracted_json)


# =============================================================================
# Stage 2 — Transformer
# =============================================================================

def test_transformer_output_file_exists(transformed_output):
    assert Path(transformed_output).exists()


def test_transformer_output_is_json_array(transformed_data):
    assert isinstance(transformed_data, list)


def test_transformer_output_is_not_empty(transformed_data):
    assert len(transformed_data) > 0


def test_transformer_output_path_is_timestamped(transformed_output):
    import re
    assert re.search(r"\d{4}/\d{2}/\d{2}/", transformed_output)


# =============================================================================
# Output structure
# =============================================================================

def test_each_sheet_has_sitecore_config(transformed_data):
    for sheet in transformed_data:
        assert "sitecoreConfig" in sheet


def test_each_sheet_has_items_key(transformed_data):
    for sheet in transformed_data:
        assert "items" in sheet


def test_each_sheet_has_relations_key(transformed_data):
    for sheet in transformed_data:
        assert "relations" in sheet


def test_specs_sheet_items_not_empty(transformed_data):
    specs = transformed_data[0]
    assert len(specs["items"]) > 0


def test_specs_items_have_name_and_template(transformed_data):
    specs = transformed_data[0]
    for item in specs["items"]:
        assert "name" in item
        assert "templateKey" in item
        assert "fields" in item


def test_specs_items_have_children(transformed_data):
    specs = transformed_data[0]
    items_with_children = [i for i in specs["items"] if i.get("children")]
    assert len(items_with_children) > 0


def test_sitecore_config_has_templates(transformed_data):
    for sheet in transformed_data:
        assert "templates" in sheet["sitecoreConfig"]


def test_sitecore_config_root_path_set(transformed_data):
    for sheet in transformed_data:
        assert sheet["sitecoreConfig"]["rootPath"] != ""


# =============================================================================
# End-to-end: edge_cases pipeline
# =============================================================================

def test_edge_cases_pipeline_runs():
    out = Transformers(
        str(MOCK_DIR / "mock_data.json"),
        str(MOCK_DIR / "edge_cases.yaml"),
    ).run()
    data = json.loads(Path(out).read_text(encoding="utf-8"))
    assert isinstance(data, list)
    assert len(data) == 5
