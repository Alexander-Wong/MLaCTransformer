"""File validation module."""

import os
from typing import List

VALID_EXCEL_EXTENSIONS = {".xlsx"}
VALID_YAML_EXTENSIONS = {".yaml", ".yml"}


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
