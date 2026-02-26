"""Entry point for MLaCTransformer."""

import argparse
import sys
from src.mlac_transformer.excel_to_json import ExcelToJson
from src.mlac_transformer.transformers import Transformers
from src.mlac_transformer.file_validator import validate_args
from src.mlac_transformer.error_logger import write_error_log


def transformer() -> None:
    """
    Handles argument parsing, validation, and orchestrating the transformation process.
    """
    parser = argparse.ArgumentParser(
        description="MLaCTransformer: transform Excel + YAML inputs."
    )
    parser.add_argument("excel_file", help="Path to the input Excel file (.xlsx)")
    parser.add_argument("yaml_file", help="Path to the YAML configuration file (.yaml/.yml)")
    args = parser.parse_args()

    errors = validate_args(args.excel_file, args.yaml_file)
    if errors:
        write_error_log(errors)
        sys.exit(1)

    ExcelToJson(args.excel_file).run()
    Transformers(args.yaml_file).run()


if __name__ == "__main__":
    transformer()
