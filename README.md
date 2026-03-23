# MLaCTransformer

## Description

MLaCTransformer is a CLI tool that converts Excel spreadsheets into Sitecore-ready JSON using a declarative YAML configuration. It extracts cell data (including merged cells and comments) into a structured intermediate JSON, then applies YAML-defined transformation rules to produce the final output.

## Features

- Extracts all sheets from an `.xlsx` file into a flat JSON representation
- Preserves cell annotations (comments) alongside cell values
- Handles merged cells and groups vertically-merged rows into single records
- Declarative YAML-driven transformation with JQ filter support
- Computed fields with conditional logic (`condition`, `omit_if_false`, `else_value`)
- Dynamic field generation from scoped row data
- Column-per-variant expansion (`expand_variants`) for tabular package-style data
- Required field validation with detailed error reporting
- Timestamped output under `output/extraction/` and `output/transform/`

## Installation

Requires [Poetry](https://python-poetry.org/docs/#installation).

```bash
git clone <repository-url>
cd MLaCTransformer
poetry install
```

## Quick Start

**Step 1 — Extract** (Excel → JSON):
```bash
mlac-extractor path/to/input.xlsx
```

**Step 2 — Transform** (JSON + YAML → Sitecore JSON):
```bash
mlac-transformer output/extraction/YYYY/MM/DD/input-HH-MM-SS.json path/to/config.yaml
```

Output files will be written to:
- `output/extraction/YYYY/MM/DD/<filename>-HH-MM-SS.json` — raw extracted data
- `output/transform/YYYY/MM/DD/<filename>-HH-MM-SS.json` — final transformed output

## Usage (High-level)

The tool is split into two independent commands:

1. **`mlac-extractor`** — validates the Excel file, reads all sheets, and writes a structured intermediate JSON where every cell is `{"value": "..."}` (with an optional `"annotation"` key if a comment is present)
2. **`mlac-transformer`** — validates the JSON and YAML files, applies the declarative YAML rules, and writes the Sitecore-ready output

### Extractor

```bash
mlac-extractor <excel_file>
```

| Argument | Description |
|---|---|
| `excel_file` | Path to the input Excel file (`.xlsx`) |

```bash
mlac-extractor --help
```

### Transformer

```bash
mlac-transformer <json_file> <yaml_file>
```

| Argument | Description |
|---|---|
| `json_file` | Path to the extracted JSON file (output of `mlac-extractor`) |
| `yaml_file` | Path to the YAML configuration file (`.yaml` or `.yml`) |

```bash
mlac-transformer --help
```

### Alternative invocation (without installing)

```bash
python -m src.mlac_etl.extractor <excel_file>
python -m src.mlac_etl.transformer <json_file> <yaml_file>
```

## Testing

### Setup

Install dev dependencies (includes `pytest` and `pytest-cov`):

```bash
poetry install --with dev
```

### Running tests

```bash
# All tests
poetry run pytest

# Unit tests only
poetry run pytest tests/unit/

# Integration tests only
poetry run pytest tests/integration/

# Specific module
poetry run pytest tests/unit/transformer/
poetry run pytest tests/unit/extractor/
poetry run pytest tests/unit/validator/

# With coverage report
poetry run pytest --cov=src/mlac_etl --cov-report=term-missing

# Verbose output
poetry run pytest -v
```

### Test structure

```
tests/
├── conftest.py                        # Shared fixtures (paths, tmp file helpers, logger init)
├── unit/
│   ├── extractor/
│   │   └── test_excel_to_json.py      # Cell extraction, merged cells, comment parsing
│   ├── transformer/
│   │   └── test_transform.py          # All YAML operators: default, transform, computed,
│   │                                  # required, types, tokens, dynamic fields, scope_children
│   └── validator/
│       └── test_params_validator.py   # File validation (existence, extension, structure)
└── integration/
    └── test_pipeline.py               # End-to-end: Excel → JSON → Sitecore output
```

### Mock data

```
mock/
├── input.xlsx          # Real Excel source file (used by extractor tests)
├── rules.yaml          # Full production YAML rules (used by integration tests)
├── mock_data.json      # Pre-extracted intermediate JSON (bypasses extractor for transformer tests)
└── edge_cases.yaml     # Minimal YAML rules targeting specific code paths and edge cases
```

### Coverage

| Module | Coverage |
|---|---|
| `params_validator.py` | 100% |
| `excel_to_json.py` | 97% |
| `transform.py` | 96% |
| **Overall** | **88%** |

## Requirements

- Python `^3.9`
- [Poetry](https://python-poetry.org/docs/#installation)

| Package | Version |
|---|---|
| `openpyxl` | `^3.1.0` |
| `PyYAML` | `^6.0.1` |
| `jq` | `^1.11.0` |

## Contributing

1. Fork the repository and create a feature branch
2. Follow the existing code structure under `src/mlac_etl/`
3. Ensure all changes are covered by tests before submitting a pull request
4. Keep commits focused and write clear commit messages

## License

This project does not currently specify a license. Contact the maintainers for usage terms.

## Contact / Support

For questions or issues, please open an issue in the project repository.
