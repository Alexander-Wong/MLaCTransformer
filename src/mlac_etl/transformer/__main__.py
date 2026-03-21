import argparse
import sys
from datetime import datetime

from mlac_etl.logger import init_log, write_log
from mlac_etl.params_validator import validate_json_and_yaml
from mlac_etl.transformer import Transformers


def transformer() -> None:
    """
    Handles argument parsing, validation, and orchestrating the transformation process.
    Receives a pre-extracted raw JSON file and a YAML configuration file.
    """
    init_log()

    parser = argparse.ArgumentParser(
        description="MLaCTransformer: transform raw JSON + YAML inputs."
    )

    parser.add_argument("json_file", help="Path to the raw extracted JSON file (.json)")
    parser.add_argument("yaml_file", help="Path to the YAML configuration file (.yaml/.yml)")
    args = parser.parse_args()

    write_log("info", (
        f"Process    : Transform\n"
        f"  Started  : {datetime.now().strftime('%Y/%m/%d %H:%M:%S')}\n"
        f"  json_file  : {args.json_file}\n"
        f"  yaml_file  : {args.yaml_file}"
    ))

    write_log("info", "Step 1/2 — Parameter validation: validating input files.")
    errors = validate_json_and_yaml(args.json_file, args.yaml_file)
    if errors:
        write_log("error", f"Parameter validation failed — {len(errors)} error(s) found:")
        for error in errors:
            write_log("error", f"  {error}")
        sys.exit(1)
    write_log("info", "Parameter validation passed.")

    write_log("info", f"Step 2/2 — Data transformation: applying YAML rules from '{args.yaml_file}'.")
    transform_path = Transformers(args.json_file, args.yaml_file).run()
    write_log("info", f"Data transformation complete. Output: '{transform_path}'.")


if __name__ == "__main__":
    transformer()
