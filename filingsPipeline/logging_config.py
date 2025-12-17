"""
Structured JSON logging configuration for SEC filings pipeline.

This module configures structlog with python-json-logger to output
single-line JSON logs to stdout, compatible with Loki/Promtail.

Usage:
    from logging_config import configure_logging, get_logger
    
    configure_logging()
    logger = get_logger(__name__)
    logger.info("event_name", key="value")
"""

import logging
import sys
import os
from pythonjsonlogger import jsonlogger
import structlog


def configure_logging(level: str = None):
    """
    Configure structured JSON logging.
    
    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR). 
               Defaults to LOG_LEVEL env var or INFO.
    """
    if level is None:
        level = os.getenv("LOG_LEVEL", "INFO")
    
    # Configure standard library logging with JSON formatter
    handler = logging.StreamHandler(sys.stdout)
    formatter = jsonlogger.JsonFormatter(
        "%(asctime)s %(levelname)s %(name)s %(message)s",
        rename_fields={"asctime": "timestamp", "levelname": "level"},
        datefmt="%Y-%m-%dT%H:%M:%S"
    )
    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.handlers = []
    root.addHandler(handler)
    root.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Configure structlog to work with standard library
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


def get_logger(name: str = None):
    """
    Get a structured logger instance.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        structlog.stdlib.BoundLogger instance
    """
    return structlog.get_logger(name)
