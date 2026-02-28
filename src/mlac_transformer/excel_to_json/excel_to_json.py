"""ExcelToJson module."""

import json
import warnings

import pandas as pd

class ExcelToJson:
    """Transforms an Excel file to a JSON representation."""

    def __init__(self, excel_path: str) -> None:
        self.excel_path = excel_path

    def run(self) -> str:
       all_sheets = pd.read_excel(self.excel_path, sheet_name=None)
    
       final_output = {}

       for sheet_name, df in all_sheets.items():
            # 2. Ignore 'Unnamed' columns
            df = df.loc[:, ~df.columns.astype(str).str.startswith('Unnamed')]
            
            # 3. Drop entirely empty rows and columns
            df = df.dropna(how='all').dropna(axis=1, how='all')
            
            # 4. Replace NaN with empty strings ""
            # We use fillna('') to ensure the JSON doesn't contain 'null'
            df_cleaned = df.fillna("")
            
            # 5. Convert to records format
            final_output[sheet_name] = df_cleaned.to_dict(orient='records')
           
       return json.dumps({"workbook": final_output}, indent=2)
