"""Logging setup, identical across all five services.

Named log.py rather than logging.py on purpose: a module named after a stdlib
module is a trap. Absolute imports mean `import logging` inside this package
still finds the standard library today, but the moment anything puts this
directory on sys.path directly, every library that logs breaks in a very
confusing way.

Two destinations, because they do different jobs:

    terminal  = a windscreen. Perfect for what is happening right now,
                useless once it has scrolled past.
    file      = a flight recorder. Still there tomorrow, greppable, and
                survives the container being destroyed.

The design doc asks for "visibility over cleverness" — logs should make it
obvious when a duplicate was correctly caught, so you can screenshot it and
so you can go back and read it after the fact.
"""

import logging
import os
import sys
from logging.handlers import RotatingFileHandler

from shared import config

_configured = False

# Terminal: short and scannable. You are watching this live, so the date is
# noise — you already know what day it is.
_CONSOLE_FORMAT = "%(asctime)s %(levelname)-8s [%(service)s] %(name)s: %(message)s"
_CONSOLE_DATEFMT = "%H:%M:%S"

# File: full date, plus the module and line number. You will read this out of
# context, possibly days later, so it has to stand on its own — and "which
# line printed this?" is the first question you will have.
_FILE_FORMAT = (
    "%(asctime)s %(levelname)-8s [%(service)s] "
    "%(name)s (%(filename)s:%(lineno)d): %(message)s"
)
_FILE_DATEFMT = "%Y-%m-%d %H:%M:%S"


class _ServiceNameFilter(logging.Filter):
    """Stamps every record with the service name.

    A "filter" in the logging module is misleadingly named: it can drop
    records, but it can also just annotate them on the way past — which is all
    we do here. Like a sorting office franking each letter with its origin.

    We need this because the service name is fixed per process, but the format
    string is applied per record. Attaching it once as a filter means every
    line — including lines emitted by SQLAlchemy or uvicorn, which know
    nothing about our services — gets tagged correctly.
    """

    def __init__(self, service_name: str) -> None:
        super().__init__()
        self.service_name = service_name

    def filter(self, record: logging.LogRecord) -> bool:
        record.service = self.service_name
        return True  # True == keep the record. We are annotating, not filtering.


def _build_file_handler(service_name: str) -> logging.Handler | None:
    """Rotating file handler, or None if the filesystem says no.

    Returns None rather than raising on failure. A service must never die
    because it could not open a log file — that would be a smoke alarm
    burning the house down. We warn on the console and carry on.
    """
    try:
        os.makedirs(config.LOG_DIR, exist_ok=True)
        path = os.path.join(config.LOG_DIR, f"{service_name}.log")

        # RotatingFileHandler, not plain FileHandler.
        #
        # A plain file handler appends forever. A relay polling every 2
        # seconds writes a surprising amount, and "disk full" takes down the
        # whole machine, not just this service.
        #
        # Rotation: when the file passes maxBytes it becomes <name>.log.1, a
        # fresh <name>.log starts, older ones shuffle down, and anything past
        # backupCount is deleted. Total usage is capped.
        handler = RotatingFileHandler(
            path,
            maxBytes=config.LOG_FILE_MAX_BYTES,
            backupCount=config.LOG_FILE_BACKUP_COUNT,
            encoding="utf-8",
        )
        handler.setFormatter(logging.Formatter(_FILE_FORMAT, datefmt=_FILE_DATEFMT))
        return handler

    except OSError as exc:
        # Read-only mount, missing permissions, no space. Say so loudly on the
        # console, then continue without file logging.
        print(
            f"[log] WARNING: file logging disabled — could not open "
            f"{config.LOG_DIR}/{service_name}.log: {exc}",
            file=sys.stderr,
        )
        return None


def setup(service_name: str) -> logging.Logger:
    """Configure logging once and return this service's logger.

    Call at the top of each service's entrypoint:

        log = setup("relay")
        log.info("relay starting")

    Safe to call repeatedly; only the first call installs handlers. Without
    that guard a second call would double every line, since handlers stack.
    """
    global _configured

    if _configured:
        return logging.getLogger(service_name)

    level = getattr(logging, config.LOG_LEVEL, logging.INFO)

    # Configure the ROOT logger. Loggers form a tree by dotted name, and a
    # record travels up to the root, so handlers attached here catch
    # everything — our modules, SQLAlchemy, uvicorn, boto3 — with one setup.
    root = logging.getLogger()
    root.setLevel(level)

    # Clear existing handlers. uvicorn installs its own before our lifespan
    # runs; leaving them attached prints every line twice.
    root.handlers.clear()

    service_filter = _ServiceNameFilter(service_name)

    # --- terminal ---------------------------------------------------------
    # stdout, not stderr. Docker captures both, but keeping normal output on
    # stdout means `docker compose logs` and shell pipelines behave as
    # expected, and stderr stays meaningful for real problems.
    console = logging.StreamHandler(stream=sys.stdout)
    console.setFormatter(logging.Formatter(_CONSOLE_FORMAT, datefmt=_CONSOLE_DATEFMT))
    console.addFilter(service_filter)
    root.addHandler(console)

    # --- file -------------------------------------------------------------
    file_handler = None
    if config.LOG_TO_FILE:
        file_handler = _build_file_handler(service_name)
        if file_handler is not None:
            file_handler.addFilter(service_filter)
            root.addHandler(file_handler)

    # Quieten the noisy libraries.
    #
    # botocore at DEBUG logs every HTTP request and signature — thousands of
    # lines a minute against a polling loop, which would also rotate your log
    # files away in minutes and bury the lines you actually care about.
    for noisy in ("botocore", "boto3", "urllib3", "s3transfer"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

    # Pull uvicorn's logs into our setup.
    #
    # uvicorn attaches its OWN handlers and sets propagate=False, which means
    # its records ("Application startup complete", every HTTP request) never
    # travel up to the root logger — so they would print on the console but be
    # missing from the file. A flight recorder with gaps in it is worse than
    # no flight recorder, because you trust it.
    #
    # Clearing its handlers and re-enabling propagation routes those records
    # through ours instead: same format, same file, one story.
    for uvicorn_logger in ("uvicorn", "uvicorn.error", "uvicorn.access"):
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
    """Logger for a submodule, e.g. get_logger(__name__).

    Assumes setup() already ran in the entrypoint. Loggers form a tree by
    dotted name, so these inherit the root handlers automatically — you never
    attach handlers anywhere but the root.
    """
    return logging.getLogger(name)
