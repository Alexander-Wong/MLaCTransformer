import argparse
import sys
from datetime import datetime

from mlac_etl.logger import init_log, write_log
from mlac_etl.params_validator import validate_excel
from mlac_etl.extractor import ExcelToJson


def extractor() -> None:
    """
    Handles argument parsing, validation, and running the Excel extraction step only.
    """
    init_log()

    parser = argparse.ArgumentParser(
        description="MLaCTransformer Extractor: extract an Excel file to JSON."
    )

    parser.add_argument("excel_file", help="Path to the input Excel file (.xlsx)")
    args = parser.parse_args()

    write_log("info", (
        f"Process    : Extract\n"
        f"  Started  : {datetime.now().strftime('%Y/%m/%d %H:%M:%S')}\n"
        f"  excel_file : {args.excel_file}"
    ))

    write_log("info", "Step 1/2 — Parameter validation: validating input file.")
    errors = validate_excel(args.excel_file)
    if errors:
        write_log("error", f"Parameter validation failed — {len(errors)} error(s) found:")
        for error in errors:
            write_log("error", f"  {error}")
        sys.exit(1)
    write_log("info", "Parameter validation passed.")

    write_log("info", f"Step 2/2 — Raw data generation: extracting '{args.excel_file}' to JSON.")
    raw_file = ExcelToJson(args.excel_file).run()
    write_log("info", f"Raw data generation complete. Output: '{raw_file}'.")


if __name__ == "__main__":
    extractor()
