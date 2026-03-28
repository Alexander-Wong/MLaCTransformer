# How to Use MLaCTransformer

## Overview

This guide covers everything needed to run MLaCTransformer: from setting up the environment to writing advanced YAML configurations. The tool takes an Excel file and a YAML specification as inputs and produces a Sitecore-ready JSON file as output.

---

## Prerequisites

- **Python 3.10+**
- **Poetry** — dependency and environment manager ([installation guide](https://python-poetry.org/docs/#installation))
- An **Excel file** (`.xlsx`) with the data to transform
- A **YAML configuration file** (`.yaml` or `.yml`) describing the transformation rules

Install dependencies after cloning the repository:

```bash
poetry install
```

Configure the git hooks (required once per clone):

```bash
git config core.hooksPath .githooks
```

---

## Basic Usage

The ETL pipeline is split into two commands. Run them in sequence:

**Step 1 — Extract** (Excel → intermediate JSON):

```bash
mlac-extractor <excel_file>
```

**Step 2 — Transform** (intermediate JSON + YAML → Sitecore JSON):

```bash
mlac-transformer <json_file> <yaml_file>
```

**Example:**

```bash
mlac-extractor mock/input.xlsx
mlac-transformer output/extraction/YYYY/MM/DD/input-HH-MM-SS.json mock/rules.yaml
```

On success, two output files are written under timestamped directories:

| Step | Output path |
|---|---|
| Extraction | `output/extraction/YYYY/MM/DD/<filename>-HH-MM-SS.json` |
| Transform  | `output/transform/YYYY/MM/DD/<filename>-HH-MM-SS.json`  |

Logs are written to `output/logs/`.

### Alternative invocation (without installing)

```bash
python -m src.mlac_etl.extractor <excel_file>
python -m src.mlac_etl.transformer <json_file> <yaml_file>
```

---

## Detailed Examples

### Example 1 — Simple flat mapping

**Excel sheet `updates`:**

| Feature | Revision Date | Contents of Change |
|---|---|---|
| Engine update | 2025-01-10 | New torque spec |

**YAML definition:**

```yaml
input:
  workbook_key: "workbook"
  sheets:
    - updates

sheets:
  updates:
    sitecore_config:
      rootPath: "/sitecore/content/my-site/updates"
      importStrategy:
        mode: "wipeAndLoad"
        missingItemAction: "ignore"
      backupStrategy:
        enabled: true
        archivePath: "../"
        prefix: "updates"
        maxBackupsToKeep: 5
      dictionaries:
        templates:
          row: "{SOME-TEMPLATE-GUID-HERE}"

    source_structure:
      column_label: "Feature"
      column_base: "Revision Date"
      column_data: []

    items:
      - templateKey: "row"
        filter: '[.[] | select(.Feature.value != "")]'
        name: "Feature"
        fields:
          - name: "Revision Date"
            value: "Revision Date"
          - name: "Feature"
            value: "Feature"
          - name: "Change"
            value: "Contents of Change"
```

**Output (`transform.json`):**

```json
[
  {
    "sitecoreConfig": { "rootPath": "/sitecore/content/my-site/updates", "..." : "..." },
    "relations": {},
    "items": [
      {
        "name": "Engine update",
        "templateKey": "row",
        "fields": [
          { "name": "Revision Date", "value": "2025-01-10" },
          { "name": "Feature",       "value": "Engine update" },
          { "name": "Change",        "value": "New torque spec" }
        ]
      }
    ]
  }
]
```

---

### Example 2 — Grouped hierarchy with `scope_children`

**Excel sheet `specs`:** rows where `Packages` contains ` > ` are group headers; rows below them with a non-empty `Category` belong to that group.

**YAML (abbreviated):**

```yaml
items:
  - templateKey: "group"
    filter: 'map(select(.Packages.value | test(" > ")))'
    name:
      field: "Packages"
      transform: 'jq: split(" > ") | .[0]'
    fields:
      - name: "Group Label"
        value: "Packages"
        transform: 'jq: split(" > ") | .[0] | ascii_upcase'
    children:
      - templateKey: "spec"
        filter: 'map(select(.Category.value != ""))'
        scope_children: true
        name:
          field: "Category"
        fields:
          - name: "Specification Label"
            value: "Category"
```

The `scope_children: true` flag causes each matched `spec` row to act as a header: only the rows between it and the next matched row are passed as children context.

---

### Example 3 — Column-per-variant expansion with `expand_variants`

Used when each data column represents a different package/trim. The sheet has one row per spec item and one column per variant (e.g. `Integra`, `A-Spec`, `Type S`).

```yaml
source_structure:
  column_base: "Integra"
  column_data:
    - "Integra"
    - "A-Spec"
    - "Type S"

items:
  - templateKey: "spec"
    filter: 'map(select(.Item.value != ""))'
    name:
      field: "Item"
    children:
      - templateKey: "packageSpec"
        expand_variants: true
        filter: '.'
        fields:
          - name: "Specification Value"
            value: "$variant"
          - name: "Vehicle Package"
            value: "__column__"
            transform: "jq: [.]"
            relationKey: "package"
```

Each column in `column_data` becomes one child item. Inside the item, `$variant` resolves to the cell value for that column and `__column__` resolves to the column name.

---

## Configuration

The YAML file is the heart of the transformation. It is divided into two top-level keys: `input` and `sheets`.

### `input`

```yaml
input:
  workbook_key: "workbook"   # key in the raw JSON that holds all sheets
  sheets:                    # ordered list of sheet names to process
    - sheet_one
    - sheet_two
```

If `sheets` is omitted, all sheets defined under `sheets:` are processed.

---

### `sheets.<sheet_name>.sitecore_config`

Defines the Sitecore target and is passed through directly to the output.

```yaml
sitecore_config:
  rootPath: "/sitecore/content/..."        # destination path in Sitecore
  importStrategy:
    mode: "wipeAndLoad"                    # import mode
    missingItemAction: "ignore"
  backupStrategy:
    enabled: true
    archivePath: "/sitecore/content/..."
    prefix: "my-backup"
    maxBackupsToKeep: 5
  dictionaries:
    templates:                             # alias → Sitecore template GUID
      folder:  "{GUID}"
      group:   "{GUID}"
      spec:    "{GUID}"
    relations:                             # alias → Sitecore path
      package: "/sitecore/content/..."
```

---

### `sheets.<sheet_name>.source_structure`

Maps the logical roles of columns in the Excel sheet.

```yaml
source_structure:
  column_label: "Column A"   # column used as a human label (informational)
  column_base:  "Column B"   # column resolved by the $base token
  column_data:               # columns expanded by expand_variants
    - "Column C"
    - "Column D"
```

---

### `sheets.<sheet_name>.items`

A recursive list of item definitions. Each item can have `children` that follow the same structure.

#### Item definition keys

| Key | Type | Description |
|---|---|---|
| `templateKey` | string | Template alias from `dictionaries.templates` |
| `filter` | JQ expression | Selects rows from the current context list |
| `name` | `{field, transform}` or string | Resolves the item name from a row field |
| `name_static` | string | Literal item name (overrides `name`) |
| `name_slug` | `{field}` | Slugifies a field value for use as the name |
| `fields` | list | Field definitions (see below) |
| `dynamic_fields` | object | Generates fields dynamically from scoped rows |
| `scope_children` | boolean | Slices the context at each matched row for children |
| `expand_variants` | boolean | Expands `column_data` columns into one row per variant |
| `children` | list | Nested item definitions |

#### Field definition keys

| Key | Type | Description |
|---|---|---|
| `name` | string | Output field name |
| `value` | string | Column name or special token (see tokens below) |
| `transform` | string | JQ expression (`jq: ...`) or regex pattern |
| `type` | string | Output type; `"getType"` auto-detects string/number/boolean |
| `default` | string | Fallback if resolved value is empty |
| `required` | boolean | Raises an error if the resolved value is empty |
| `relationKey` | string | Links the field to a `relations` dictionary entry |
| `computed` | boolean | Enables conditional field resolution (see below) |

#### Computed field keys (when `computed: true`)

| Key | Description |
|---|---|
| `condition` | JQ expression evaluated against the row; truthy enables `value` |
| `value` | Resolved when condition is truthy |
| `else_value` | Resolved when condition is falsy |
| `omit_if_false` | If `true`, the field is omitted entirely when condition is falsy |

#### Dynamic fields keys

```yaml
dynamic_fields:
  source: "__scope__"        # key in the row that holds the sub-rows (optional)
  filter: 'map(select(...))'  # JQ filter applied to the sub-rows
  name_from: "Attribute Name" # column providing each field's name
  value_from: "Value"         # column providing each field's value
  type: "getType"             # type applied to each generated field
  required: false             # fail if any generated field value is empty
  required_fields:            # fail if specific field names are missing or empty
    - "someFieldName"
```

---

### Special value tokens

| Token | Resolves to |
|---|---|
| `$base` | Value of `source_structure.column_base` for the current row |
| `$base_annotation` | Annotation (comment) of `source_structure.column_base` |
| `$variant` | Value of the current variant column (only with `expand_variants: true`) |
| `$variant_annotation` | Annotation of the current variant column |
| `$annotation:<col>` | Annotation of any named column (e.g. `$annotation:Category`) |
| `__column__` | The column name of the current variant (only with `expand_variants: true`) |

---

### Transform syntax

| Prefix | Behavior |
|---|---|
| `jq: <expr>` | JQ expression applied to the resolved string value |
| *(no prefix)* | Regex pattern; returns the first capture group |

**JQ transform examples:**

```yaml
transform: 'jq: ascii_upcase'
transform: 'jq: split(" > ") | .[0]'
transform: 'jq: gsub("[^a-z0-9]+"; "-")'
transform: 'jq: [.]'                      # wraps value in a JSON array
```

**Regex transform examples:**

```yaml
transform: 'Headline:\s*(.+?)(?:\n|$)'   # extracts text after "Headline:"
transform: '\((\d+)\)'                   # extracts number inside parentheses
```

---

## Advanced Usage

### Extracting data from cell annotations

Excel cell comments are preserved in the extraction as an `"annotation"` key. Use `$annotation:<col>` to read them in fields, and pair with a regex transform to extract structured content.

```yaml
fields:
  - name: "Headline"
    value: "$annotation:Category"
    transform: 'Headline:\s*(.+?)(?:\n|$)'
  - name: "Description"
    value: "$annotation:Category"
    transform: 'Description:\s*([\s\S]+?)(?:\nImage:|$)'
```

---

### Conditionally including a field

Use `computed: true` with `omit_if_false: true` to add a field only when a condition is met:

```yaml
- name: "modal"
  computed: true
  condition: 'jq: .Packages.annotation != null and .Packages.annotation != ""'
  value: "Packages"
  transform: 'jq: ascii_downcase | gsub("[^a-z0-9]+"; "-") | rtrimstr("-") + "-modal"'
  omit_if_false: true
  type: "lookup"
  relationKey: "internalCrossReference"
```

---

### Exploding multi-value cells into separate items

Use a JQ `filter` to split a pipe-delimited cell into individual rows before building items:

```yaml
filter: '[.[] | . as $row | ($row.Keys.value | split("|") | map(ltrimstr(" ") | rtrimstr(" ")) | .[]) | {Component: $row.Component, Key: .}]'
```

---

### Dynamic fields from grouped rows

When a sheet uses a `Component` → `Attribute` layout (one component header row followed by attribute rows), use `dynamic_fields` with `group_by` to collect attribute rows as fields of each component:

```yaml
filter: '[group_by(.Component.value)[] | select(length > 0 and .[0].Component.value != "") | .[0] + {"__scope__": .}]'
name:
  field: "Component"
dynamic_fields:
  source: "__scope__"
  filter: 'map(select(."Attribute Name".value != ""))'
  name_from: "Attribute Name"
  value_from: "Value"
  type: "getType"
```

---

## API / Commands Reference

### CLI

**Extractor:**
```
usage: mlac-extractor [-h] excel_file

MLaCTransformer Extractor: extract an Excel file to JSON.

positional arguments:
  excel_file  Path to the input Excel file (.xlsx)

options:
  -h, --help  show this help message and exit
```

**Transformer:**
```
usage: mlac-transformer [-h] json_file yaml_file

MLaCTransformer: transform raw JSON + YAML inputs.

positional arguments:
  json_file   Path to the raw extracted JSON file (.json)
  yaml_file   Path to the YAML configuration file (.yaml/.yml)

options:
  -h, --help  show this help message and exit
```

### Exit codes

| Code | Meaning |
|---|---|
| `0` | Success |
| `1` | Validation error (missing file, wrong extension) |

### Output structure

The output is a JSON array with one object per processed sheet:

```json
[
  {
    "sitecoreConfig": {
      "rootPath": "...",
      "importStrategy": {},
      "backupStrategy": {},
      "templates": {},
    },
    "relations": {},
    "items": [
      {
        "name": "item-name",
        "templateKey": "templateAlias",
        "fields": [
          { "name": "Field Name", "value": "...", "type": "string" }
        ],
        "children": []
      }
    ]
  }
]
```

---

## Troubleshooting

### `Excel file not found` or `YAML file not found`
The path passed on the command line does not exist. Use an absolute path or verify your working directory.

### `Invalid Excel extension` / `Invalid YAML extension`
Only `.xlsx` is accepted for Excel. Only `.yaml` and `.yml` are accepted for YAML.

### `Sheet 'X' not found in workbook. Available: [...]`
The sheet name in `input.sheets` does not match any sheet in the Excel file. Sheet names are case-sensitive.

### `Sheet 'X' has no definition in YAML — skipping`
The sheet appears in `input.sheets` but has no entry under `sheets:`. Add a definition or remove the sheet from `input.sheets`.

### `JQ error on filter '...'`
The JQ expression is invalid or does not match the expected input shape. Test your expression against the raw extraction JSON at `output/extraction/YYYY/MM/DD/<filename>-HH-MM-SS.json` using a JQ playground before adding it to the YAML.

### `[REQUIRED FIELD MISSING]`
A field marked `required: true` resolved to an empty string. The log will include the sheet name, field name, field definition, and the full row data.

### Empty `items` list in output
The `filter` expression matched zero rows. Inspect the raw extraction JSON and verify that the column names and values in the JQ expression match exactly (including casing and whitespace).

---

## Contributing — Semantic Commits

This project uses [Conventional Commits](https://www.conventionalcommits.org/) to automate version bumps and changelog updates. Every commit must follow the format:

```
<type>[optional scope]: <description>
```

### Commit types

| Type | When to use | Version bump |
|---|---|---|
| `feat` | New feature or capability | `minor` — 1.0.0 → 1.1.0 |
| `fix` | Bug fix | `patch` — 1.0.0 → 1.0.1 |
| `feat!` or `BREAKING CHANGE` | Breaking API change | `major` — 1.0.0 → 2.0.0 |
| `chore` | Maintenance, tooling, dependencies | none |
| `docs` | Documentation only | none |
| `test` | Adding or updating tests | none |
| `refactor` | Code change with no behavior change | none |

### Making a commit

Use the interactive commitizen prompt instead of `git commit`:

```bash
poetry run cz commit
```

The tool guides you through selecting the type, scope, and description. On completion, the post-commit hook automatically:

1. Bumps the version in `pyproject.toml` (for `feat`, `fix`, and breaking changes)
2. Prepends a new entry to `CHANGELOG.md` with the version and timestamp
3. Amends the commit to include both files

### Examples

```bash
# Bug fix — bumps patch version
poetry run cz commit
# type: fix | scope: extractor | description: handle empty annotation on merged cells

# New feature — bumps minor version
poetry run cz commit
# type: feat | scope: transformer | description: add regex transform support for annotations

# Breaking change — bumps major version
poetry run cz commit
# type: feat! | description: change output JSON structure to array format

# No version bump
poetry run cz commit
# type: chore | description: update openpyxl to 3.2
```

### What NOT to do

- Do not edit `pyproject.toml` version manually before committing — the hook manages it
- Do not edit `CHANGELOG.md` manually — it is auto-generated by the hook
- Do not use `git commit` directly for changes that should trigger a version bump

---

## FAQ

**Q: Can I process multiple sheets in one run?**
Yes. List all sheet names under `input.sheets` and add a definition for each under `sheets:`. They are processed in order and combined into a single `transform.json`.

**Q: What happens to merged cells?**
Vertically merged cells are collapsed: the top-left value is propagated to all covered rows, and those rows are grouped into a single record in the extraction JSON.

**Q: Can I use the same column as both `column_base` and one of the `column_data` entries?**
Yes. `column_base` and `column_data` are independent mappings. `column_base` is the target of the `$base` token; `column_data` drives `expand_variants` expansion.

**Q: How are cell comments preserved?**
Any cell with a comment has an `"annotation"` key added to its cell object in the extraction JSON. Both classic (`Author:\ntext`) and threaded (modern Excel) comment formats are stripped of metadata — only the body text is kept.

**Q: Can `transform` be chained?**
With the `jq:` prefix, yes — use the standard JQ pipe: `jq: split(" > ") | .[0] | ascii_upcase`. Regex transforms support a single pattern per field.

**Q: What does `type: "getType"` do?**
It auto-detects the output type at runtime: numeric strings become `"number"`, recognized boolean strings (`yes`, `no`, `true`, `false`, `✓`, `✗`) become `"boolean"`, and everything else becomes `"string"`.

**Q: Can `expand_variants` be filtered?**
Yes. Apply a JQ `filter` to the variant rows to exclude columns where the cell value does not meet a condition — for example, `'map(select(.__cell__.value == "X"))'` keeps only columns where the cell contains `X`.
