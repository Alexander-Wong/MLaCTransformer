import os
from typing import List

VALID_EXCEL_EXTENSIONS = {".xlsx"}
VALID_YAML_EXTENSIONS = {".yaml", ".yml"}
VALID_JSON_EXTENSIONS = {".json"}


def validate_excel(excel_path: str) -> List[str]:
    """
    Validate only the Excel file argument.
    """
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
    """
    Validate only the raw JSON file argument.
    """
    errors: List[str] = []

    if not os.path.exists(json_path):
        errors.append(f"JSON file not found: '{json_path}'")
    else:
        _, ext = os.path.splitext(json_path)
        if ext.lower() not in VALID_JSON_EXTENSIONS:
            errors.append(
                f"Invalid JSON extension '{ext}' for file '{json_path}'. "
                f"Expected one of: {sorted(VALID_JSON_EXTENSIONS)}"
            )

    return errors


def validate_json_and_yaml(json_path: str, yaml_path: str) -> List[str]:
    """
    Validate the JSON raw data file and the YAML config file.
    """
    return validate_json(json_path) + validate_yaml(yaml_path)


def validate_yaml(yaml_path: str) -> List[str]:
    """
    Validate only the YAML file argument.
    """
    errors: List[str] = []

    if not os.path.exists(yaml_path):
        errors.append(f"YAML file not found: '{yaml_path}'")
    else:
        _, ext = os.path.splitext(yaml_path)
        if ext.lower() not in VALID_YAML_EXTENSIONS:
            errors.append(
                f"Invalid YAML extension '{ext}' for file '{yaml_path}'. "
                f"Expected one of: {sorted(VALID_YAML_EXTENSIONS)}"
            )

    return errors


def validate_args(excel_path: str, yaml_path: str) -> List[str]:
    """
    Validate the two files arguments types.
    """
    errors: List[str] = []

    # Excel file
    if not os.path.exists(excel_path):
        errors.append(f"Excel file not found: '{excel_path}'")
    else:
        _, ext = os.path.splitext(excel_path)
        if ext.lower() not in VALID_EXCEL_EXTENSIONS:
            errors.append(
                f"Invalid Excel extension '{ext}' for file '{excel_path}'. "
                f"Expected one of: {sorted(VALID_EXCEL_EXTENSIONS)}"
            )

    # YAML file
    if not os.path.exists(yaml_path):
        errors.append(f"YAML file not found: '{yaml_path}'")
    else:
        _, ext = os.path.splitext(yaml_path)
        if ext.lower() not in VALID_YAML_EXTENSIONS:
            errors.append(
                f"Invalid YAML extension '{ext}' for file '{yaml_path}'. "
                f"Expected one of: {sorted(VALID_YAML_EXTENSIONS)}"
            )

    return errors
