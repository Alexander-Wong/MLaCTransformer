import argparse
import sys

from src.mlac_transformer.logger import init_log, write_log
from src.mlac_transformer.params_validator import validate_args
from src.mlac_transformer.excel_to_json import ExcelToJson
from src.mlac_transformer.transform import Transformers




def transformer() -> None:
    """
    Handles argument parsing, validation, and orchestrating the transformation process.
    """
    init_log()

    parser = argparse.ArgumentParser(
        description="MLaCTransformer: transform Excel + YAML inputs."
    )

    parser.add_argument("excel_file", help="Path to the input Excel file (.xlsx)")
    parser.add_argument("yaml_file", help="Path to the YAML configuration file (.yaml/.yml)")
    args = parser.parse_args()

    write_log("info", "Step 1/3 — Parameter validation: validating input files.")
    errors = validate_args(args.excel_file, args.yaml_file)
    if errors:
        write_log("error", f"Parameter validation failed — {len(errors)} error(s) found:")
        for error in errors:
            write_log("error", f"  {error}")
        sys.exit(1)
    write_log("info", "Parameter validation passed.")

    write_log("info", f"Step 2/3 — Raw data generation: extracting '{args.excel_file}' to JSON.")
    raw_file = ExcelToJson(args.excel_file).run()
    write_log("info", f"Raw data generation complete. Output: '{raw_file}'.")

    write_log("info", f"Step 3/3 — Data transformation: applying YAML rules from '{args.yaml_file}'.")
    transform_path = Transformers(raw_file, args.yaml_file).run()
    write_log("info", f"Data transformation complete. Output: '{transform_path}'.")


if __name__ == "__main__":
    transformer()
