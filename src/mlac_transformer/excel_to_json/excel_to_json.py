import json
import warnings
import pandas as pd
from datetime import datetime
from pathlib import Path
from src.mlac_transformer.logger import write_log


class ExcelToJson:
    """
    Transforms an Excel file to a JSON representation.
    """

    def __init__(self, excel_path: str) -> None:
        today = datetime.today()
        self.excel_path = excel_path
        self.output_path = (
            Path("output/extraction")
            / today.strftime("%Y")
            / today.strftime("%m")
            / today.strftime("%d")
        )

    def run(self) -> str:
        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
                all_sheets = pd.read_excel(self.excel_path, sheet_name=None)
        except Exception as e:
            write_log("error", f"Failed to read Excel file '{self.excel_path}': {e}")
            raise

        raw_data = {}

        for sheet_name, df in all_sheets.items():
            try:
                # Ignore 'Unnamed' columns
                df = df.loc[:, ~df.columns.astype(str).str.startswith('Unnamed')]
                # Replace NaN with empty strings to avoid 'null' in JSON
                df_cleaned = df.fillna("")
                # Convert to records format
                raw_data[sheet_name] = df_cleaned.to_dict(orient='records')
            except Exception as e:
                write_log("error", f"Failed to process sheet '{sheet_name}' in '{self.excel_path}': {e}")
                raise

        try:
            # Build output path: output/extraction/YYYY/MM/DD/<excel_filename>.json
            self.output_path.mkdir(parents=True, exist_ok=True)
            raw_file = self.output_path / (Path(self.excel_path).stem + ".json")
            raw_file.write_text(
                json.dumps({"workbook": raw_data}, indent=2),
                encoding="utf-8"
            )
        except Exception as e:
            write_log("error", f"Failed to write JSON output for '{self.excel_path}': {e}")
            raise

        return str(raw_file)
