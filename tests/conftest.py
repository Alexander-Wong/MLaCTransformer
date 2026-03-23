import json
import pytest
from pathlib import Path
from mlac_etl.logger import init_log

MOCK_DIR = Path(__file__).parent.parent / "mock"


def pytest_configure(config):
    """Initialize the logger once before any tests run."""
    init_log()


@pytest.fixture
def mock_excel():
    return str(MOCK_DIR / "input.xlsx")


@pytest.fixture
def mock_rules_yaml():
    return str(MOCK_DIR / "rules.yaml")


@pytest.fixture
def mock_data_json():
    return str(MOCK_DIR / "mock_data.json")


@pytest.fixture
def edge_cases_yaml():
    return str(MOCK_DIR / "edge_cases.yaml")


@pytest.fixture
def tmp_json(tmp_path):
    """Write a workbook dict to a temp JSON file and return its path."""
    def _write(data: dict) -> str:
        p = tmp_path / "data.json"
        p.write_text(json.dumps(data), encoding="utf-8")
        return str(p)
    return _write


@pytest.fixture
def tmp_yaml(tmp_path):
    """Write a YAML string to a temp file and return its path."""
    def _write(content: str, suffix=".yaml") -> str:
        p = tmp_path / f"rules{suffix}"
        p.write_text(content, encoding="utf-8")
        return str(p)
    return _write
