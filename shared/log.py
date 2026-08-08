"""Logging setup shared by all services: console plus a rotating file.

Named log.py, not logging.py — a module shadowing a stdlib name breaks every
library that logs the moment this directory lands on sys.path directly.
"""

import logging
import os
import sys
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from logging.handlers import RotatingFileHandler

from shared import config

_configured = False

# Console: short, read live. File: full date plus file:line, read later out of
# context. Both carry the correlation id so interleaved work stays separable.
_CONSOLE_FORMAT = (
    "%(asctime)s %(levelname)-8s [%(service)s] [%(cid)s] %(name)s: %(message)s"
)
_CONSOLE_DATEFMT = "%H:%M:%S"
_FILE_FORMAT = (
    "%(asctime)s %(levelname)-8s [%(service)s] [%(cid)s] "
    "%(name)s (%(filename)s:%(lineno)d): %(message)s"
)
_FILE_DATEFMT = "%Y-%m-%d %H:%M:%S"

# Identifies the unit of work a log line belongs to: an HTTP request in the
# Order Service, an SQS message in a consumer, one poll in the relay.
#
# A ContextVar rather than a global: each asyncio task and each threadpool
# worker gets its own copy, so concurrent requests cannot overwrite each
# other's id. anyio propagates the context into the threadpool, so sync
# endpoints work too.
_correlation_id: ContextVar[str] = ContextVar("correlation_id", default="--------")


def new_correlation_id() -> str:
    """Short random id. 8 hex chars is enough to disambiguate concurrent work
    while staying readable in a log column."""
    return uuid.uuid4().hex[:8]


def get_correlation_id() -> str:
    return _correlation_id.get()


@contextmanager
def correlation_scope(value: str | None = None):
    """Tag every log line emitted inside this block with one id.

        with correlation_scope() as cid:
            ...

    reset() on exit rather than set("--------"), so nested scopes restore the
    outer value instead of clobbering it.
    """
    cid = value or new_correlation_id()
    token = _correlation_id.set(cid)
    try:
        yield cid
    finally:
        _correlation_id.reset(token)


class _ServiceNameFilter(logging.Filter):
    """Annotates every record with the service name and correlation id.

    A filter rather than format constants so records from third-party loggers
    (SQLAlchemy, uvicorn) are tagged too. Always returns True — it annotates,
    it does not filter.
    """

    def __init__(self, service_name: str) -> None:
        super().__init__()
        self.service_name = service_name

    def filter(self, record: logging.LogRecord) -> bool:
        record.service = self.service_name
        record.cid = _correlation_id.get()
        return True


def _build_file_handler(service_name: str) -> logging.Handler | None:
    """Rotating file handler, or None if the filesystem refuses.

    Returns None instead of raising: a service must not die because it could
    not open a log file.
    """
    try:
        os.makedirs(config.LOG_DIR, exist_ok=True)
        path = os.path.join(config.LOG_DIR, f"{service_name}.log")
        # Rotating, not plain: a polling loop would otherwise fill the disk.
        handler = RotatingFileHandler(
            path,
            maxBytes=config.LOG_FILE_MAX_BYTES,
            backupCount=config.LOG_FILE_BACKUP_COUNT,
            encoding="utf-8",
        )
        handler.setFormatter(logging.Formatter(_FILE_FORMAT, datefmt=_FILE_DATEFMT))
        return handler
    except OSError as exc:
        print(
            f"[log] WARNING: file logging disabled — could not open "
            f"{config.LOG_DIR}/{service_name}.log: {exc}",
            file=sys.stderr,
        )
        return None


def setup(service_name: str) -> logging.Logger:
    """Configure logging once and return this service's logger.

    Call at the top of each entrypoint. Repeat calls are no-ops; without that
    guard, stacked handlers would duplicate every line.
    """
    global _configured

    if _configured:
        return logging.getLogger(service_name)

    level = getattr(logging, config.LOG_LEVEL, logging.INFO)

    # Handlers go on the root logger only: records propagate up the dotted
    # name tree, so one setup covers our modules and third-party libraries.
    root = logging.getLogger()
    root.setLevel(level)
    # uvicorn installs handlers before our lifespan runs; leaving them
    # attached prints everything twice.
    root.handlers.clear()

    service_filter = _ServiceNameFilter(service_name)

    # stdout, not stderr, so `docker compose logs` and pipelines behave.
    console = logging.StreamHandler(stream=sys.stdout)
    console.setFormatter(logging.Formatter(_CONSOLE_FORMAT, datefmt=_CONSOLE_DATEFMT))
    console.addFilter(service_filter)
    root.addHandler(console)

    file_handler = None
    if config.LOG_TO_FILE:
        file_handler = _build_file_handler(service_name)
        if file_handler is not None:
            file_handler.addFilter(service_filter)
            root.addHandler(file_handler)

    # botocore at DEBUG logs every request and signature — enough to rotate
    # the log files away in minutes.
    for noisy in ("botocore", "boto3", "urllib3", "s3transfer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    # uvicorn sets propagate=False on its loggers, so its records would reach
    # the console but never the file. Route them through ours instead.
    #
    # "uvicorn.access" is deliberately excluded: uvicorn implements
    # --no-access-log by clearing that logger's handlers, and re-enabling
    # propagation here would silently undo the flag. Access logging is the
    # correlation middleware's job — it also records duration and the request
    # id, which uvicorn's line cannot.
    for uvicorn_logger in ("uvicorn", "uvicorn.error"):
        lg = logging.getLogger(uvicorn_logger)
        lg.handlers.clear()
        lg.propagate = True

    _configured = True

    logger = logging.getLogger(service_name)
    if file_handler is not None:
        logger.info(
            "logging to console and %s (rotate at %.0f MB, keep %d)",
            os.path.join(config.LOG_DIR, f"{service_name}.log"),
            config.LOG_FILE_MAX_BYTES / 1024 / 1024,
            config.LOG_FILE_BACKUP_COUNT,
        )
    else:
        logger.info("logging to console only")

    return logger


def get_logger(name: str) -> logging.Logger:
    """Logger for a submodule, e.g. get_logger(__name__). Inherits root
    handlers; assumes setup() already ran in the entrypoint."""
    return logging.getLogger(name)
