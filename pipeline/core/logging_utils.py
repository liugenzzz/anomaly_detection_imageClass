from __future__ import annotations

import logging
from pathlib import Path

from config import LOGGING_CONFIG


def setup_stage_logger(stage_name: str, output_dir: Path | None = None) -> logging.Logger:
    logger = logging.getLogger(stage_name)
    logger.setLevel(getattr(logging, LOGGING_CONFIG["log_level"].upper(), logging.INFO))
    logger.handlers.clear()
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    log_dir = output_dir or LOGGING_CONFIG["log_dir"]
    log_dir.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_dir / f"{stage_name}.log", encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    if LOGGING_CONFIG["console"]:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


def suppress_console_progress_lines(logger: logging.Logger, prefix: str = "progress ") -> None:
    """Drop periodic ``progress ...`` lines from console output only.

    Used when a live progress bar is drawn on the same stream, so the two
    don't fight over the terminal; the file handler keeps receiving every
    line for later inspection.
    """

    class _PrefixFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            return not record.getMessage().startswith(prefix)

    prefix_filter = _PrefixFilter()
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler) and not isinstance(
            handler, logging.FileHandler
        ):
            handler.addFilter(prefix_filter)

