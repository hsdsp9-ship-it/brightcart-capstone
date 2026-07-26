"""Simple logging helper for notebooks.

Provides a configured logger via `get_logger(name)` so notebooks can
emit structured log lines to the driver logs. Keeps configuration
centralized and suitable for Databricks or local runs.
"""
import logging
import os
from typing import Optional


def _configure_root_logger(level: Optional[int] = None):
    if logging.getLogger().handlers:
        return
    if level is None:
        level_name = os.getenv("LOG_LEVEL", "INFO").upper()
        level = getattr(logging, level_name, logging.INFO)
    handler = logging.StreamHandler()
    fmt = "%(asctime)s %(levelname)s %(name)s - %(message)s"
    handler.setFormatter(logging.Formatter(fmt))
    root = logging.getLogger()
    root.setLevel(level)
    root.addHandler(handler)


def get_logger(name: str):
    _configure_root_logger()
    return logging.getLogger(name)
