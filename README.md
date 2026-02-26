# MLaCTransformer

A Python transformer application that accepts an Excel file and a YAML configuration file, validates both inputs, and runs a processing pipeline through the `ExcelToJson` and `Transformers` modules.

---

## Project Structure

```
MLaCTransformer/
├── pyproject.toml
├── transformer.py
└── src/
    └── mlac_transformer/
        ├── __init__.py
        ├── excel_to_json.py
        └── transformers.py
```

---

## Requirements

- Python 3.9+
- [Poetry](https://python-poetry.org/docs/#installation)

---

## Setup

```bash
# Install dependencies
poetry install
```

---

## Usage

```bash
poetry run python transformer.py <excel_file> <yaml_file>
```

| Argument | Description |
|---|---|
| `excel_file` | Path to the input Excel file (`.xlsx`) |
| `yaml_file` | Path to the YAML configuration file (`.yaml` or `.yml`) |

### Examples

**Happy path:**
```bash
poetry run python transformer.py mock/input.xlsx mock/rules.yaml
```

Output:
```
Hi from ExcelToJson
Hi from Transformers
```

**Get help:**
```bash
poetry run python transformer.py --help
```

---

## Error Handling

If either file is missing or has an invalid extension, the application exits with code `1` and writes the errors to `output/stderr.json`.

**Example `output/stderr.json`:**
```json
{
  "status": "error",
  "error_count": 2,
  "errors": [
    "Excel file not found: 'data/input.xlsx'",
    "Invalid YAML extension '.txt' for file 'config.txt'. Expected one of: ['.yaml', '.yml']"
  ]
}
```

Valid extensions:
- Excel: `.xlsx`
- YAML: `.yaml`, `.yml`
