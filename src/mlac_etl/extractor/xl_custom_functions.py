"""
Custom xlcalculator implementations for Excel text functions not natively supported.
Each function is registered globally at import time via @xl.register().
"""

import re as _re
from xlcalculator.xlfunctions import xl


def _str(v) -> str:
    """Convert an xlcalculator value or plain Python value to a clean string."""
    return "" if v is None else str(v)


def _num(v) -> float:
    """Convert an xlcalculator value to float."""
    return float(_str(v).replace(",", "").strip())


# ---------------------------------------------------------------------------
# SUBSTITUTE(text, old_text, new_text, [instance_num])
# Replaces occurrences of old_text with new_text inside text.
# If instance_num is given, only that occurrence (1-based) is replaced.
# ---------------------------------------------------------------------------
@xl.register()
def SUBSTITUTE(text, old_text, new_text, instance_num=None):
    t, old, new = _str(text), _str(old_text), _str(new_text)
    if not old:
        return t
    if instance_num is None:
        return t.replace(old, new)
    n = int(_num(instance_num))
    if n < 1:
        return t
    count, start, parts = 0, 0, []
    while True:
        idx = t.find(old, start)
        if idx == -1:
            parts.append(t[start:])
            break
        count += 1
        parts.append(t[start:idx])
        if count == n:
            parts.append(new)
            parts.append(t[idx + len(old):])
            break
        parts.append(old)
        start = idx + len(old)
    return "".join(parts)


# ---------------------------------------------------------------------------
# SEARCH(find_text, within_text, [start_num])
# Case-insensitive search that returns the 1-based position of find_text inside
# within_text. Supports * (any chars) and ? (any single char) wildcards.
# Returns empty string if not found (mirrors Excel #VALUE! as a safe fallback).
# ---------------------------------------------------------------------------
@xl.register()
def SEARCH(find_text, within_text, start_num=None):
    find = _str(find_text)
    within = _str(within_text)
    start = max(0, int(_num(start_num)) - 1) if start_num is not None else 0
    pattern = _re.escape(find).replace(r"\*", ".*").replace(r"\?", ".")
    match = _re.search(pattern, within[start:], _re.IGNORECASE)
    if match is None:
        return ""
    return start + match.start() + 1


# ---------------------------------------------------------------------------
# TEXT(value, format_text)
# Formats a number as a string using a subset of Excel number-format codes.
# Supports: 0 / # (integer), 0.0+ (fixed decimals), #,##0 (thousands),
# 0% / 0.00% (percent), @ (text passthrough).
# ---------------------------------------------------------------------------
@xl.register()
def TEXT(value, format_text):
    fmt = _str(format_text).strip()
    raw = _str(value)
    if fmt == "@":
        return raw
    try:
        num = _num(raw)
    except (ValueError, ZeroDivisionError):
        return raw
    if fmt.endswith("%"):
        pct = num * 100
        inner = fmt.rstrip("%").strip()
        decimals = len(inner.split(".")[-1]) if "." in inner else 0
        return f"{pct:.{decimals}f}%"
    use_thousands = "," in fmt
    decimal_part = fmt.split(".")[-1] if "." in fmt else ""
    decimals = len(_re.sub(r"[^0#]", "", decimal_part))
    if use_thousands:
        return f"{num:,.{decimals}f}"
    return f"{num:.{decimals}f}"


# ---------------------------------------------------------------------------
# VALUE(text)
# Converts a text string representing a number to a numeric value.
# Strips common non-numeric characters (commas, currency, percent sign).
# Returns empty string on parse failure (mirrors Excel #VALUE! as a fallback).
# ---------------------------------------------------------------------------
@xl.register()
def VALUE(text):
    t = _str(text).strip().replace(",", "").replace("$", "").replace("%", "")
    try:
        f = float(t)
        return int(f) if f == int(f) else f
    except ValueError:
        return ""


# ---------------------------------------------------------------------------
# TEXTJOIN(delimiter, ignore_empty, text1, [text2, ...])
# Joins multiple text values with a delimiter.
# When ignore_empty is TRUE, empty strings are excluded from the result.
# ---------------------------------------------------------------------------
@xl.register()
def TEXTJOIN(delimiter, ignore_empty, *texts):
    delim = _str(delimiter)
    ignore = _str(ignore_empty).strip().upper() in ("TRUE", "1", "YES")
    parts = [_str(t) for t in texts]
    if ignore:
        parts = [p for p in parts if p]
    return delim.join(parts)


# ---------------------------------------------------------------------------
# PROPER(text)
# Capitalizes the first letter of each word and lowercases the rest,
# matching Excel's title-case behavior.
# ---------------------------------------------------------------------------
@xl.register()
def PROPER(text):
    return _str(text).title()


# ---------------------------------------------------------------------------
# CHAR(number)
# Returns the character corresponding to the given Unicode code point.
# Mirrors Excel's CHAR function (code 1–255 in Excel, extended here to full Unicode).
# ---------------------------------------------------------------------------
@xl.register()
def CHAR(number):
    try:
        return chr(int(_num(number)))
    except (ValueError, OverflowError):
        return ""


# ---------------------------------------------------------------------------
# CLEAN(text)
# Removes non-printable characters (those below ASCII 32) from a string.
# Useful for stripping hidden control characters imported from external sources.
# ---------------------------------------------------------------------------
@xl.register()
def CLEAN(text):
    return "".join(c for c in _str(text) if ord(c) >= 32)
