import logging
import logging.config
from pathlib import Path

_CONFIG_PATH = Path(__file__).resolve().parent.parent.parent.parent / "logging.ini"
_logger: logging.Logger | None = None


def init_log() -> None:
    """
    Initialize the logger by loading the logging.ini configuration.
    Must be called once before any write_log() calls.
    """
    global _logger
    Path("output/logs/").mkdir(parents=True, exist_ok=True)
    logging.config.fileConfig(str(_CONFIG_PATH), disable_existing_loggers=False)
    _logger = logging.getLogger("mlac_transformer")


def write_log(level: str, message: str) -> None:
    """
    Log a message at the given level name (debug, info, warning, error, critical).
    """
    if _logger is None:
        raise RuntimeError("Logger not initialized. Call init_logger() first.")
    log_fn = getattr(_logger, level.lower(), None)
    if log_fn is None:
        raise ValueError(f"Invalid log level: '{level}'")
    log_fn(message)
