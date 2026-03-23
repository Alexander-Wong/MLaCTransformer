import json
import textwrap
import pytest
from pathlib import Path
from mlac_etl.transformer.transform import Transformers, RequiredFieldError

MOCK_DIR = Path(__file__).parent.parent.parent.parent / "mock"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def run(json_path, yaml_path):
    """Run transformer and return parsed output list."""
    out = Transformers(json_path, yaml_path).run()
    return json.loads(Path(out).read_text(encoding="utf-8"))


def sheet(results, index=0):
    return results[index]


def items(results, index=0):
    return results[index]["items"]


def first_item(results, index=0):
    return results[index]["items"][0]


def find_field(item, name):
    return next((f for f in item["fields"] if f["name"] == name), None)


# ---------------------------------------------------------------------------
# Fixtures — edge_cases.yaml + mock_data.json
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def edge_results():
    out = Transformers(
        str(MOCK_DIR / "mock_data.json"),
        str(MOCK_DIR / "edge_cases.yaml"),
    ).run()
    return json.loads(Path(out).read_text(encoding="utf-8"))


# =============================================================================
# A — Default values
# =============================================================================

def test_default_applied_when_value_empty(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [{"Col": {"value": ""}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields:\n"
        "          - name: F\n            value: Col\n            default: fallback\n"
    )
    result = run(tmp_json(data), tmp_yaml(yaml))
    assert find_field(first_item(result), "F")["value"] == "fallback"


def test_default_not_applied_when_has_value(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [{"Col": {"value": "real"}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields:\n"
        "          - name: F\n            value: Col\n            default: fallback\n"
    )
    result = run(tmp_json(data), tmp_yaml(yaml))
    assert find_field(first_item(result), "F")["value"] == "real"


def test_no_default_empty_stays_empty(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [{"Col": {"value": ""}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields:\n"
        "          - name: F\n            value: Col\n"
    )
    result = run(tmp_json(data), tmp_yaml(yaml))
    assert find_field(first_item(result), "F")["value"] == ""


# =============================================================================
# B — JQ transforms
# =============================================================================

def test_jq_transform_ascii_upcase(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [{"Col": {"value": "hello world"}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields:\n"
        "          - name: F\n            value: Col\n            transform: 'jq: ascii_upcase'\n"
    )
    result = run(tmp_json(data), tmp_yaml(yaml))
    assert find_field(first_item(result), "F")["value"] == "HELLO WORLD"


def test_jq_transform_split_and_index(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [{"Col": {"value": "A > B > C"}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields:\n"
        "          - name: F\n            value: Col\n            transform: 'jq: split(\" > \") | .[0]'\n"
    )
    result = run(tmp_json(data), tmp_yaml(yaml))
    assert find_field(first_item(result), "F")["value"] == "A"


def test_jq_transform_array_promoted_to_list(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [{"Col": {"value": "a|b|c"}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields:\n"
        '          - name: F\n            value: Col\n            transform: \'jq: split("|")\'\n'
    )
    result = run(tmp_json(data), tmp_yaml(yaml))
    assert find_field(first_item(result), "F")["value"] == ["a", "b", "c"]


def test_jq_invalid_expr_does_not_crash(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [{"Col": {"value": "hello"}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields:\n"
        "          - name: F\n            value: Col\n            transform: 'jq: {[['\n"
    )
    result = run(tmp_json(data), tmp_yaml(yaml))
    field = find_field(first_item(result), "F")
    assert field is not None


def test_jq_null_result_becomes_empty_string(edge_results):
    jq_null_sheet = next(s for s in edge_results if s["sitecoreConfig"]["rootPath"] == "/test/jqnull")
    field = find_field(jq_null_sheet["items"][0], "NullResult")
    assert field["value"] == ""


# =============================================================================
# C — Regex transforms
# =============================================================================

def test_regex_extracts_first_capture_group(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [{"Col": {"value": "abc123def"}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields:\n"
        r"          - name: F" + "\n" +
        r"            value: Col" + "\n" +
        r"            transform: '(\d+)'" + "\n"
    )
    result = run(tmp_json(data), tmp_yaml(yaml))
    assert find_field(first_item(result), "F")["value"] == "123"


def test_regex_no_match_returns_original(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [{"Col": {"value": "no digits here"}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields:\n"
        r"          - name: F" + "\n" +
        r"            value: Col" + "\n" +
        r"            transform: '(\d+)'" + "\n"
    )
    result = run(tmp_json(data), tmp_yaml(yaml))
    assert find_field(first_item(result), "F")["value"] == "no digits here"


def test_regex_returns_first_group_only(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [{"Col": {"value": "foo-42"}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields:\n"
        r"          - name: F" + "\n" +
        r"            value: Col" + "\n" +
        r"            transform: '(\w+)-(\d+)'" + "\n"
    )
    result = run(tmp_json(data), tmp_yaml(yaml))
    assert find_field(first_item(result), "F")["value"] == "foo"


# =============================================================================
# D — Computed fields
# =============================================================================

def test_computed_condition_true_uses_value(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [{"Flag": {"value": "yes"}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields:\n"
        "          - name: F\n            computed: true\n"
        "            condition: 'jq: .Flag.value == \"yes\"'\n"
        "            value: true\n            else_value: false\n"
    )
    result = run(tmp_json(data), tmp_yaml(yaml))
    assert find_field(first_item(result), "F")["value"] is True


def test_computed_condition_false_uses_else_value(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [{"Flag": {"value": "no"}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields:\n"
        "          - name: F\n            computed: true\n"
        "            condition: 'jq: .Flag.value == \"yes\"'\n"
        "            value: true\n            else_value: false\n"
    )
    result = run(tmp_json(data), tmp_yaml(yaml))
    assert find_field(first_item(result), "F")["value"] is False


def test_computed_omit_if_false_field_absent(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [{"Flag": {"value": "no"}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields:\n"
        "          - name: F\n            computed: true\n"
        "            condition: 'jq: .Flag.value == \"yes\"'\n"
        "            value: true\n            omit_if_false: true\n"
    )
    result = run(tmp_json(data), tmp_yaml(yaml))
    assert find_field(first_item(result), "F") is None


def test_computed_omit_if_false_field_present_when_true(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [{"Flag": {"value": "yes"}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields:\n"
        "          - name: F\n            computed: true\n"
        "            condition: 'jq: .Flag.value == \"yes\"'\n"
        "            value: true\n            omit_if_false: true\n"
    )
    result = run(tmp_json(data), tmp_yaml(yaml))
    assert find_field(first_item(result), "F") is not None


def test_computed_no_condition_returns_empty(edge_results):
    computed_sheet = next(s for s in edge_results if s["sitecoreConfig"]["rootPath"] == "/test/computed")
    field = find_field(computed_sheet["items"][0], "NoCondition")
    assert field is not None
    assert field["value"] == ""


def test_computed_bad_condition_falls_back_gracefully(edge_results):
    computed_sheet = next(s for s in edge_results if s["sitecoreConfig"]["rootPath"] == "/test/computed")
    field = find_field(computed_sheet["items"][0], "BadCondition")
    assert field is not None


def test_computed_relationkey_preserved(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [{"Flag": {"value": "yes"}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields:\n"
        "          - name: F\n            computed: true\n"
        "            condition: 'jq: .Flag.value == \"yes\"'\n"
        "            value: true\n            relationKey: myRelation\n"
    )
    result = run(tmp_json(data), tmp_yaml(yaml))
    assert find_field(first_item(result), "F")["relationKey"] == "myRelation"


# =============================================================================
# E — Required fields
# =============================================================================

def test_required_raises_when_empty(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [{"Col": {"value": ""}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields:\n"
        "          - name: F\n            value: Col\n            required: true\n"
    )
    with pytest.raises(RequiredFieldError):
        run(tmp_json(data), tmp_yaml(yaml))


def test_required_does_not_raise_when_has_value(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [{"Col": {"value": "present"}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields:\n"
        "          - name: F\n            value: Col\n            required: true\n"
    )
    result = run(tmp_json(data), tmp_yaml(yaml))
    assert find_field(first_item(result), "F")["value"] == "present"


def test_not_required_empty_is_ok(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [{"Col": {"value": ""}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields:\n"
        "          - name: F\n            value: Col\n"
    )
    result = run(tmp_json(data), tmp_yaml(yaml))
    assert find_field(first_item(result), "F") is not None


# =============================================================================
# F — Types
# =============================================================================

def test_gettype_integer(edge_results):
    gt = next(s for s in edge_results if s["sitecoreConfig"]["rootPath"] == "/test/gettype")
    int_item = next(i for i in gt["items"] if find_field(i, "TypedValue")["value"] == "42")
    assert find_field(int_item, "TypedValue")["type"] == "number"


def test_gettype_decimal(edge_results):
    gt = next(s for s in edge_results if s["sitecoreConfig"]["rootPath"] == "/test/gettype")
    dec_item = next(i for i in gt["items"] if find_field(i, "TypedValue")["value"] == "3.14")
    assert find_field(dec_item, "TypedValue")["type"] == "number"


def test_gettype_boolean_yes(edge_results):
    gt = next(s for s in edge_results if s["sitecoreConfig"]["rootPath"] == "/test/gettype")
    bool_item = next(i for i in gt["items"] if find_field(i, "TypedValue")["value"] == "yes")
    assert find_field(bool_item, "TypedValue")["type"] == "boolean"


def test_gettype_boolean_checkmark(edge_results):
    gt = next(s for s in edge_results if s["sitecoreConfig"]["rootPath"] == "/test/gettype")
    check_item = next(i for i in gt["items"] if find_field(i, "TypedValue")["value"] == "✓")
    assert find_field(check_item, "TypedValue")["type"] == "boolean"


def test_gettype_string(edge_results):
    gt = next(s for s in edge_results if s["sitecoreConfig"]["rootPath"] == "/test/gettype")
    str_item = next(i for i in gt["items"] if find_field(i, "TypedValue")["value"] == "hello")
    assert find_field(str_item, "TypedValue")["type"] == "string"


def test_gettype_empty_value_returns_string(edge_results):
    gt = next(s for s in edge_results if s["sitecoreConfig"]["rootPath"] == "/test/gettype")
    empty_item = next(i for i in gt["items"] if find_field(i, "TypedValue")["value"] == "")
    assert find_field(empty_item, "TypedValue")["type"] == "string"


def test_literal_type_preserved(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [{"Col": {"value": "some value"}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields:\n"
        "          - name: F\n            value: Col\n            type: Checkbox\n"
    )
    result = run(tmp_json(data), tmp_yaml(yaml))
    assert find_field(first_item(result), "F")["type"] == "Checkbox"


def test_no_type_key_absent_from_field(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [{"Col": {"value": "x"}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields:\n"
        "          - name: F\n            value: Col\n"
    )
    result = run(tmp_json(data), tmp_yaml(yaml))
    assert "type" not in find_field(first_item(result), "F")


# =============================================================================
# G — Special tokens
# =============================================================================

def test_annotation_token_resolves(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [{"Col": {"value": "v", "annotation": "my note"}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields:\n"
        "          - name: F\n            value: '$annotation:Col'\n"
    )
    result = run(tmp_json(data), tmp_yaml(yaml))
    assert find_field(first_item(result), "F")["value"] == "my note"


def test_base_token_resolves(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [{"Base": {"value": "base-val"}, "Other": {"value": "x"}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    source_structure:\n      column_base: Base\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields:\n"
        "          - name: F\n            value: '$base'\n"
    )
    result = run(tmp_json(data), tmp_yaml(yaml))
    assert find_field(first_item(result), "F")["value"] == "base-val"


def test_variant_annotation_token_resolves(edge_results):
    va = next(s for s in edge_results if s["sitecoreConfig"]["rootPath"] == "/test/variant")
    comp_a = next(i for i in va["items"] if i["name"] == "Component A")
    variant_children = comp_a["children"]
    annotated = next(c for c in variant_children if find_field(c, "Annotation")["value"] != "")
    assert "Note for" in find_field(annotated, "Annotation")["value"]


def test_variant_token_resolves(edge_results):
    va = next(s for s in edge_results if s["sitecoreConfig"]["rootPath"] == "/test/variant")
    comp_a = next(i for i in va["items"] if i["name"] == "Component A")
    variant_children = comp_a["children"]
    assert any(find_field(c, "Value")["value"] == "X" for c in variant_children)


# =============================================================================
# H — Dynamic fields
# =============================================================================

def _group_yaml(extra_dynamic=""):
    """Minimal YAML that groups rows by Component and embeds scope for dynamic_fields."""
    return textwrap.dedent(f"""\
        sheets:
          s1:
            sitecore_config:
              dictionaries:
                templates:
                  item: '{{G}}'
            items:
              - templateKey: item
                filter: '[group_by(.Component.value)[] | select(length > 0) | .[0] + {{"__scope__": .}}]'
                fields: []
                dynamic_fields:
                  source: "__scope__"
                  name_from: "Attribute Name"
                  value_from: "Value"
                  {extra_dynamic}
    """)


def test_dynamic_fields_generated_from_rows(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [
        {"Component": {"value": "Auth"}, "Attribute Name": {"value": "Tipo"},    "Value": {"value": "Service"}},
        {"Component": {"value": "Auth"}, "Attribute Name": {"value": "Version"}, "Value": {"value": "1.0"}},
    ]}}
    result = run(tmp_json(data), tmp_yaml(_group_yaml()))
    fields = first_item(result)["fields"]
    assert any(f["name"] == "Tipo"    and f["value"] == "Service" for f in fields)
    assert any(f["name"] == "Version" and f["value"] == "1.0"     for f in fields)


def test_dynamic_fields_skips_empty_name(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [
        {"Component": {"value": "A"}, "Attribute Name": {"value": ""},     "Value": {"value": "ignored"}},
        {"Component": {"value": "A"}, "Attribute Name": {"value": "Real"}, "Value": {"value": "kept"}},
    ]}}
    result = run(tmp_json(data), tmp_yaml(_group_yaml()))
    names = [f["name"] for f in first_item(result)["fields"]]
    assert "" not in names
    assert "Real" in names


def test_dynamic_fields_required_raises(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [
        {"Component": {"value": "A"}, "Attribute Name": {"value": "MissingVal"}, "Value": {"value": ""}},
    ]}}
    result_yaml = textwrap.dedent("""\
        sheets:
          s1:
            sitecore_config:
              dictionaries:
                templates:
                  item: '{G}'
            items:
              - templateKey: item
                filter: '.'
                fields: []
                dynamic_fields:
                  name_from: "Attribute Name"
                  value_from: "Value"
                  required: true
    """)
    with pytest.raises(RequiredFieldError):
        run(tmp_json(data), tmp_yaml(result_yaml))


def test_dynamic_fields_required_fields_list_raises(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [
        {"Component": {"value": "A"}, "Attribute Name": {"value": "Tipo"}, "Value": {"value": "x"}},
    ]}}
    result_yaml = textwrap.dedent("""\
        sheets:
          s1:
            sitecore_config:
              dictionaries:
                templates:
                  item: '{G}'
            items:
              - templateKey: item
                filter: '.'
                fields: []
                dynamic_fields:
                  name_from: "Attribute Name"
                  value_from: "Value"
                  required_fields: [MandatoryMissing]
    """)
    with pytest.raises(RequiredFieldError):
        run(tmp_json(data), tmp_yaml(result_yaml))


def test_dynamic_fields_source_and_filter(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [
        {"Component": {"value": "Auth"}, "Attribute Name": {"value": "Tipo"},    "Value": {"value": "Service"}},
        {"Component": {"value": "Auth"}, "Attribute Name": {"value": "Version"}, "Value": {"value": "1.0"}},
        {"Component": {"value": "Auth"}, "Attribute Name": {"value": ""},        "Value": {"value": "skip"}},
    ]}}
    result_yaml = textwrap.dedent("""\
        sheets:
          s1:
            sitecore_config:
              dictionaries:
                templates:
                  item: '{G}'
            items:
              - templateKey: item
                filter: '[group_by(.Component.value)[] | select(length > 0) | .[0] + {"__scope__": .}]'
                fields: []
                dynamic_fields:
                  source: "__scope__"
                  filter: 'map(select(."Attribute Name".value != ""))'
                  name_from: "Attribute Name"
                  value_from: "Value"
    """)
    result = run(tmp_json(data), tmp_yaml(result_yaml))
    fields = first_item(result)["fields"]
    assert any(f["name"] == "Tipo"    for f in fields)
    assert any(f["name"] == "Version" for f in fields)
    assert not any(f["name"] == ""    for f in fields)


def test_dynamic_fields_type_applied(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [
        {"Component": {"value": "A"}, "Attribute Name": {"value": "Count"}, "Value": {"value": "42"}},
    ]}}
    result = run(tmp_json(data), tmp_yaml(_group_yaml("type: getType")))
    count_field = next(f for f in first_item(result)["fields"] if f["name"] == "Count")
    assert count_field["type"] == "number"


# =============================================================================
# I — scope_children + children recursion
# =============================================================================

def test_scope_children_builds_correct_hierarchy(edge_results):
    nested = next(s for s in edge_results if s["sitecoreConfig"]["rootPath"] == "/test/nested")
    category_items = nested["items"]
    assert len(category_items) == 2
    names = {i["name"] for i in category_items}
    assert "Engine" in names
    assert "Transmission" in names


def test_scope_children_scopes_rows_correctly(edge_results):
    nested = next(s for s in edge_results if s["sitecoreConfig"]["rootPath"] == "/test/nested")
    engine = next(i for i in nested["items"] if i["name"] == "Engine")
    child_names = {c["name"] for c in engine["children"]}
    assert "Displacement" in child_names
    assert "Horsepower" in child_names
    # Transmission's items must NOT appear under Engine
    assert "Type" not in child_names
    assert "Ratios" not in child_names


def test_children_recursion_transmission(edge_results):
    nested = next(s for s in edge_results if s["sitecoreConfig"]["rootPath"] == "/test/nested")
    transmission = next(i for i in nested["items"] if i["name"] == "Transmission")
    child_names = {c["name"] for c in transmission["children"]}
    assert "Type" in child_names
    assert "Ratios" in child_names


# =============================================================================
# J — name resolution
# =============================================================================

def test_name_from_field_dict(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [{"Label": {"value": "My Item"}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n"
        "        name:\n          field: Label\n        fields: []\n"
    )
    result = run(tmp_json(data), tmp_yaml(yaml))
    assert first_item(result)["name"] == "My Item"


def test_name_as_plain_string(tmp_json, tmp_yaml):
    # name: Label (bare string) → returns the literal string "Label", not column value
    data = {"workbook": {"s1": [{"Label": {"value": "anything"}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n"
        "        name: Label\n        fields: []\n"
    )
    result = run(tmp_json(data), tmp_yaml(yaml))
    assert first_item(result)["name"] == "Label"


def test_name_unnamed_fallback(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [{"Col": {"value": "x"}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields: []\n"
    )
    result = run(tmp_json(data), tmp_yaml(yaml))
    assert first_item(result)["name"] == "unnamed"


# =============================================================================
# K — _load_sheet_rows edge cases
# =============================================================================

def test_load_sheet_rows_raw_is_list(tmp_yaml):
    # When raw JSON is a plain list (not {"workbook": ...}), it is returned as-is
    import tempfile, os
    raw_list = [{"Col": {"value": "x"}}]
    yaml_str = (
        "input:\n  workbook_key: workbook\n  sheets: [s1]\n"
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields:\n"
        "          - name: F\n            value: Col\n"
    )
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(raw_list, f)
        json_path = f.name
    try:
        t = Transformers(json_path, tmp_yaml(yaml_str))
        t._load_sheet_rows(raw_list, "workbook", "s1")
    finally:
        os.unlink(json_path)


def test_load_sheet_rows_missing_sheet_raises():
    t = Transformers.__new__(Transformers)
    t._current_sheet = ""
    with pytest.raises(KeyError):
        t._load_sheet_rows({"workbook": {"other": []}}, "workbook", "missing")


# =============================================================================
# L — Error paths
# =============================================================================

def test_load_yaml_raises_on_missing_file(tmp_path):
    t = Transformers(str(tmp_path / "data.json"), str(tmp_path / "missing.yaml"))
    with pytest.raises(Exception):
        t._load_yaml()


def test_load_raw_json_raises_on_missing_file(tmp_path):
    t = Transformers(str(tmp_path / "missing.json"), str(tmp_path / "rules.yaml"))
    with pytest.raises(Exception):
        t._load_raw_json()


def test_run_skips_sheet_not_in_yaml_def(tmp_json, tmp_yaml):
    data = {"workbook": {"defined": [{"Col": {"value": "x"}}], "undefined": []}}
    yaml_str = (
        "input:\n  workbook_key: workbook\n  sheets: [defined, undefined]\n"
        "sheets:\n  defined:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields: []\n"
    )
    result = run(tmp_json(data), tmp_yaml(yaml_str))
    assert len(result) == 1


# =============================================================================
# M — relationKey on standard field
# =============================================================================

def test_relation_key_preserved_on_standard_field(tmp_json, tmp_yaml):
    data = {"workbook": {"s1": [{"Col": {"value": "v"}}]}}
    yaml = (
        "sheets:\n  s1:\n    sitecore_config:\n      dictionaries:\n        templates:\n          item: '{G}'\n"
        "    items:\n      - templateKey: item\n        filter: '.'\n        fields:\n"
        "          - name: F\n            value: Col\n            relationKey: myRel\n"
    )
    result = run(tmp_json(data), tmp_yaml(yaml))
    assert find_field(first_item(result), "F")["relationKey"] == "myRel"
