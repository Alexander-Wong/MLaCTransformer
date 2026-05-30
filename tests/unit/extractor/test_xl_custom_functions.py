import pytest
import mlac_etl.extractor.xl_custom_functions  # noqa: F401 — registers functions at import time
from mlac_etl.extractor.xl_custom_functions import (
    SUBSTITUTE, SEARCH, TEXT, VALUE, TEXTJOIN, PROPER, CHAR, CLEAN,
)


# =============================================================================
# SUBSTITUTE(text, old_text, new_text, [instance_num])
# =============================================================================

def test_substitute_replaces_all_occurrences():
    assert SUBSTITUTE("aaa", "a", "b") == "bbb"


def test_substitute_basic():
    assert SUBSTITUTE("hello world", "world", "there") == "hello there"


def test_substitute_specific_instance():
    assert SUBSTITUTE("aaa", "a", "b", 2) == "aba"


def test_substitute_instance_1():
    assert SUBSTITUTE("aaa", "a", "b", 1) == "baa"


def test_substitute_empty_old_text_unchanged():
    assert SUBSTITUTE("hello", "", "x") == "hello"


def test_substitute_old_not_found_unchanged():
    assert SUBSTITUTE("hello", "z", "x") == "hello"


def test_substitute_instance_beyond_count_unchanged():
    assert SUBSTITUTE("ab", "a", "x", 5) == "ab"


# =============================================================================
# SEARCH(find_text, within_text, [start_num])
# =============================================================================

def test_search_basic_returns_1based_position():
    assert SEARCH("world", "hello world") == 7


def test_search_case_insensitive():
    assert SEARCH("HELLO", "hello world") == 1


def test_search_with_start_num():
    # "hello world" — second 'o' is at position 8
    assert SEARCH("o", "hello world", 6) == 8


def test_search_not_found_returns_empty():
    assert SEARCH("xyz", "hello world") == ""


def test_search_wildcard_star():
    assert SEARCH("h*o", "hello") == 1


def test_search_wildcard_question_mark():
    assert SEARCH("h?llo", "hello") == 1


# =============================================================================
# TEXT(value, format_text)
# =============================================================================

def test_text_passthrough_at_format():
    assert TEXT("hello", "@") == "hello"


def test_text_integer_format():
    assert TEXT(1234, "0") == "1234"


def test_text_fixed_decimals():
    assert TEXT(1234.5, "0.00") == "1234.50"


def test_text_thousands_separator():
    assert TEXT(1234567.89, "#,##0.00") == "1,234,567.89"


def test_text_percent_no_decimals():
    assert TEXT(0.5, "0%") == "50%"


def test_text_percent_with_decimals():
    assert TEXT(0.1234, "0.00%") == "12.34%"


def test_text_non_numeric_passthrough():
    assert TEXT("not a number", "0.00") == "not a number"


# =============================================================================
# VALUE(text)
# =============================================================================

def test_value_integer_string():
    assert VALUE("42") == 42


def test_value_float_string():
    assert VALUE("3.14") == 3.14


def test_value_strips_commas():
    assert VALUE("1,234") == 1234


def test_value_strips_dollar():
    assert VALUE("$99.99") == 99.99


def test_value_strips_percent():
    assert VALUE("50%") == 50


def test_value_invalid_returns_empty():
    assert VALUE("not a number") == ""


def test_value_integer_float_returns_int():
    assert VALUE("10.0") == 10
    assert isinstance(VALUE("10.0"), int)


# =============================================================================
# TEXTJOIN(delimiter, ignore_empty, *texts)
# =============================================================================

def test_textjoin_basic():
    assert TEXTJOIN(", ", True, "a", "b", "c") == "a, b, c"


def test_textjoin_ignores_empty_when_true():
    assert TEXTJOIN(", ", True, "a", "", "c") == "a, c"


def test_textjoin_keeps_empty_when_false():
    assert TEXTJOIN(", ", False, "a", "", "c") == "a, , c"


def test_textjoin_empty_delimiter():
    assert TEXTJOIN("", True, "a", "b", "c") == "abc"


def test_textjoin_single_value():
    assert TEXTJOIN("-", True, "only") == "only"


def test_textjoin_all_empty_ignore_true():
    assert TEXTJOIN(", ", True, "", "", "") == ""


# =============================================================================
# PROPER(text)
# =============================================================================

def test_proper_basic():
    assert PROPER("hello world") == "Hello World"


def test_proper_all_caps():
    assert PROPER("HELLO") == "Hello"


def test_proper_mixed():
    assert PROPER("hElLo WoRlD") == "Hello World"


def test_proper_single_word():
    assert PROPER("integra") == "Integra"


# =============================================================================
# CHAR(number)
# =============================================================================

def test_char_uppercase_a():
    assert CHAR(65) == "A"


def test_char_lowercase_a():
    assert CHAR(97) == "a"


def test_char_newline():
    assert CHAR(10) == "\n"


def test_char_space():
    assert CHAR(32) == " "


def test_char_invalid_returns_empty():
    assert CHAR("abc") == ""


# =============================================================================
# CLEAN(text)
# =============================================================================

def test_clean_removes_control_chars():
    assert CLEAN("hello\x01world") == "helloworld"


def test_clean_removes_null():
    assert CLEAN("a\x00b") == "ab"


def test_clean_normal_text_unchanged():
    assert CLEAN("normal text") == "normal text"


def test_clean_preserves_spaces():
    assert CLEAN("hello world") == "hello world"


def test_clean_removes_all_below_32():
    dirty = "".join(chr(i) for i in range(32)) + "clean"
    assert CLEAN(dirty) == "clean"
