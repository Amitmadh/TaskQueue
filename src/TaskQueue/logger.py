from __future__ import annotations

import logging
import sys

# Re-exported so callers can write setup_logging(DEBUG) without importing logging.
DEBUG = logging.DEBUG
INFO = logging.INFO
WARNING = logging.WARNING
ERROR = logging.ERROR
CRITICAL = logging.CRITICAL

_RESET = "\x1b[0m"
_COLORS = {
    logging.DEBUG: "\x1b[36m",  # cyan
    logging.INFO: "\x1b[32m",  # green
    logging.WARNING: "\x1b[33m",  # yellow
    logging.ERROR: "\x1b[31m",  # red
    logging.CRITICAL: "\x1b[1;31m",  # bold red
}


class _ColorFormatter(logging.Formatter):
    def formatMessage(self, record: logging.LogRecord) -> str:
        color = _COLORS.get(record.levelno, "")
        return (
            f"{self.formatTime(record, '%H:%M:%S')} "
            f"{color}{record.levelname:<8}{_RESET} "
            f"{record.name}  {record.message}"
        )


def setup_logging(level: int | str = logging.INFO) -> None:
    """Configure the root logger; colorized when stderr is a terminal.

    Calling it again replaces the previous configuration (``force=True``).
    """
    handler = logging.StreamHandler()
    handler.setFormatter(
        _ColorFormatter()
        if sys.stderr.isatty()
        else logging.Formatter(
            "%(asctime)s %(levelname)-8s %(name)s  %(message)s", "%H:%M:%S"
        )
    )
    logging.basicConfig(level=level, handlers=[handler], force=True)
