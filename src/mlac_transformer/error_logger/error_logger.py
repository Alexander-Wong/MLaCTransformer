"""Error logger module."""

import json
from pathlib import Path
from typing import List

STDERROR_PATH = "output/stderr.json"


def write_error_log(errors: List[str]) -> None:
    """
    Write errors to the standard error log file as JSON.
    """
    payload = {
        "status": "error",
        "error_count": len(errors),
        "errors": errors,
    }
    Path(STDERROR_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(STDERROR_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    print(f"Validation failed. {len(errors)} error(s) written to '{STDERROR_PATH}'.")
