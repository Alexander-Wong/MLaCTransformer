"""ExcelToJson module."""

import json
import warnings

import pandas as pd

class ExcelToJson:
    """Transforms an Excel file to a JSON representation."""

    def __init__(self, excel_path: str) -> None:
        self.excel_path = excel_path

    def run(self) -> str:
       # Load data
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
            df = pd.read_excel(self.excel_path, header=None)
        
        # 1. FIND ANCHOR
        mask = df.apply(lambda row: row.astype(str).str.contains("Integra", case=False, na=False), axis=1)
        found_indices = mask.stack()[mask.stack()].index.tolist()
        if not found_indices: return {"error": "Anchor 'Integra' not found"}
        
        anchor_row, anchor_col = found_indices[0]

        # 2. MAP MODELS
        model_names = []
        for c in range(anchor_col, df.shape[1]):
            val = df.iloc[anchor_row, c]
            if pd.notna(val) and str(val).strip():
                model_names.append(str(val).strip())
        
        model_data = {model: {} for model in model_names}
        current_path = []

        # 3. ROBUST EXTRACTION
        for r in range(anchor_row + 1, df.shape[0]):
            label_val = None
            depth = -1
            
            # Determine depth and label safely
            for col_idx in range(anchor_col):
                cell = df.iloc[r, col_idx]
                if pd.notna(cell):
                    clean_cell = str(cell).strip()
                    if clean_cell:
                        label_val = clean_cell.replace(">", "").strip()
                        depth = col_idx
                        break
            
            # Skip rows with no labels at all
            if label_val is None:
                continue

            # Check if row is a category (no data values across models)
            has_data = False
            for i in range(len(model_names)):
                if pd.notna(df.iloc[r, anchor_col + i]):
                    has_data = True
                    break

            # Update path safely
            # Fix for the TypeError: ensure current_path is always a list
            if not isinstance(current_path, list):
                current_path = []
                
            current_path = current_path[:depth] + [label_val]

            if not has_data:
                # It's a category header
                continue
            else:
                # It's a data row - Assign to models
                for i, model in enumerate(model_names):
                    val = df.iloc[r, anchor_col + i]
                    target = model_data[model]
                    
                    # Navigate the nested dictionary safely
                    for step in current_path[:-1]:
                        if step not in target or not isinstance(target[step], dict):
                            target[step] = {}
                        target = target[step]
                    
                    # Final assignment
                    target[current_path[-1]] = val if pd.notna(val) else None

        return json.dumps({"packages": model_data}, indent=2)
