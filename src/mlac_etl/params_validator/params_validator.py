import json
import os
import yaml
from typing import List

VALID_EXCEL_EXTENSIONS = {".xlsx"}
VALID_YAML_EXTENSIONS = {".yaml", ".yml"}
VALID_JSON_EXTENSIONS = {".json"}


def validate_excel(excel_path: str) -> List[str]:
    """Validate existence and extension of the Excel file."""
    errors: List[str] = []

    if not os.path.exists(excel_path):
        errors.append(f"Excel file not found: '{excel_path}'")
    else:
        _, ext = os.path.splitext(excel_path)
        if ext.lower() not in VALID_EXCEL_EXTENSIONS:
            errors.append(
                f"Invalid Excel extension '{ext}' for file '{excel_path}'. "
                f"Expected one of: {sorted(VALID_EXCEL_EXTENSIONS)}"
            )

    return errors


def validate_json(json_path: str) -> List[str]:
    """Validate existence, extension, and content structure of the extracted JSON file."""
    errors: List[str] = []

    if not os.path.exists(json_path):
        errors.append(f"JSON file not found: '{json_path}'")
        return errors

    _, ext = os.path.splitext(json_path)
    if ext.lower() not in VALID_JSON_EXTENSIONS:
        errors.append(
            f"Invalid JSON extension '{ext}' for file '{json_path}'. "
            f"Expected one of: {sorted(VALID_JSON_EXTENSIONS)}"
        )
        return errors

    try:
        with open(json_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except json.JSONDecodeError as e:
        errors.append(f"Invalid JSON content in '{json_path}': {e}")
        return errors

    if not isinstance(data, dict):
        errors.append(f"JSON file '{json_path}' must be an object at the top level.")
        return errors

    workbook_key = "workbook"
    if workbook_key not in data:
        errors.append(
            f"JSON file '{json_path}' is missing the required '{workbook_key}' key."
        )
    elif not isinstance(data[workbook_key], dict):
        errors.append(
            f"'{workbook_key}' in '{json_path}' must be an object mapping sheet names to row arrays."
        )

    return errors


def validate_json_and_yaml(json_path: str, yaml_path: str) -> List[str]:
    """Validate the JSON raw data file and the YAML config file."""
    return validate_json(json_path) + validate_yaml(yaml_path)


def validate_yaml(yaml_path: str) -> List[str]:
    """Validate existence, extension, and content structure of the YAML config file."""
    errors: List[str] = []

    if not os.path.exists(yaml_path):
        errors.append(f"YAML file not found: '{yaml_path}'")
        return errors

    _, ext = os.path.splitext(yaml_path)
    if ext.lower() not in VALID_YAML_EXTENSIONS:
        errors.append(
            f"Invalid YAML extension '{ext}' for file '{yaml_path}'. "
            f"Expected one of: {sorted(VALID_YAML_EXTENSIONS)}"
        )
        return errors

    try:
        with open(yaml_path, "r", encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh)
    except yaml.YAMLError as e:
        errors.append(f"Invalid YAML content in '{yaml_path}': {e}")
        return errors

    if not isinstance(cfg, dict):
        errors.append(f"YAML file '{yaml_path}' must be a mapping at the top level.")
        return errors

    if "sheets" not in cfg:
        errors.append(f"YAML file '{yaml_path}' is missing the required 'sheets' key.")
        return errors

    sheets = cfg["sheets"]
    if not isinstance(sheets, dict) or not sheets:
        errors.append(f"'sheets' in '{yaml_path}' must be a non-empty mapping of sheet definitions.")
        return errors

    for sheet_name, sheet_def in sheets.items():
        if not isinstance(sheet_def, dict):
            errors.append(f"Sheet '{sheet_name}' in '{yaml_path}' must be a mapping.")

    return errors


def validate_args(excel_path: str, yaml_path: str) -> List[str]:
    """Validate the Excel and YAML file arguments."""
    return validate_excel(excel_path) + validate_yaml(yaml_path)
