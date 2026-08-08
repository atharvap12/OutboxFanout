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
import uuid
from contextlib import contextmanager
from contextvars import ContextVar
from logging.handlers import RotatingFileHandler

from shared import config

_configured = False

# Terminal: short and scannable. You are watching this live, so the date is
# noise — you already know what day it is.
_CONSOLE_FORMAT = (
    "%(asctime)s %(levelname)-8s [%(service)s] [%(cid)s] %(name)s: %(message)s"
)
_CONSOLE_DATEFMT = "%H:%M:%S"

# File: full date, plus the module and line number. You will read this out of
# context, possibly days later, so it has to stand on its own — and "which
# line printed this?" is the first question you will have.
_FILE_FORMAT = (
    "%(asctime)s %(levelname)-8s [%(service)s] [%(cid)s] "
    "%(name)s (%(filename)s:%(lineno)d): %(message)s"
)
_FILE_DATEFMT = "%Y-%m-%d %H:%M:%S"


# ---------------------------------------------------------------------------
# Correlation id — which unit of work does this log line belong to?
#
# The problem it solves:
#
#   Handle one request at a time and the log reads like a story, top to
#   bottom. Handle four at once and the lines interleave, like four people
#   telling four different stories into the same microphone:
#
#       ┌─ POST /orders          <- request A
#       ┌─ POST /orders          <- request B
#       staged order 3abf3e88…   <- ...whose?
#       staged order 5be1a569…   <- ...and whose is this?
#
#   Blank lines or separator bars cannot fix that, because the lines are
#   genuinely intermixed — there is no gap to draw a line in. The only thing
#   that works is TAGGING every line with an id, so you can pull one story
#   back out afterwards:
#
#       grep '\[a739f703\]' logs/order-service.log
#
# In this project a "unit of work" is an HTTP request in the Order Service,
# one SQS message in a consumer, and one poll cycle in the relay. Later, when
# an order's journey crosses all five services, sharing one id across them is
# how you prove Scenario A: one grep showing the duplicate publish and all
# three consumers correctly declining it.
# ---------------------------------------------------------------------------

# A ContextVar, NOT a plain global variable.
#
# A global would be shared by everything in the process, so two requests being
# handled at the same time would overwrite each other's id — the exact problem
# we are trying to solve. A ContextVar gives every asyncio task (and every
# threadpool worker) its own private copy, automatically.
#
# Think of it as a name badge that follows one job through the building,
# rather than a whiteboard in the lobby that everyone scribbles on.
#
# It works for FastAPI's synchronous endpoints too: those run in a threadpool,
# and anyio copies the context into the worker thread for us.
#
# The default "--------" is what you see for lines emitted outside any unit of
# work — startup, shutdown. Eight dashes so the column stays aligned.
_correlation_id: ContextVar[str] = ContextVar("correlation_id", default="--------")


def new_correlation_id() -> str:
    """A short random id.

    A full UUID is 36 characters and would dominate every log line. The first
    8 hex characters give about 4 billion possibilities — far more than enough
    to tell apart the handful of requests in flight at any moment, while
    staying narrow enough to read.

    This is an id for *finding things in a log*, not a database key, so
    collisions are merely inconvenient rather than dangerous.
    """
    return uuid.uuid4().hex[:8]


def get_correlation_id() -> str:
    """The id of the unit of work currently running, if any."""
    return _correlation_id.get()


@contextmanager
def correlation_scope(value: str | None = None):
    """Tag every log line emitted inside this block with a single id.

        with correlation_scope() as cid:
            log.info("this line is tagged")
            do_work()                      # ...and so is everything in here

    Pass a value to reuse an existing id — that is how an id given by a caller
    (an X-Request-ID header, or an SQS message id) flows through your service
    instead of a fresh one being invented.

    Note the finally block calls token.reset() rather than setting the value
    back to "--------". reset() restores whatever was there BEFORE this block,
    so nested scopes behave correctly: an inner scope ending returns you to
    the outer scope's id, instead of wiping it.
    """
    cid = value or new_correlation_id()
    token = _correlation_id.set(cid)
    try:
        yield cid
    finally:
        _correlation_id.reset(token)


class _ServiceNameFilter(logging.Filter):
    """Stamps every record with the service name and the correlation id.

    A "filter" in the logging module is misleadingly named: it can drop
    records, but it can also just annotate them on the way past — which is all
    we do here. Like a sorting office franking each letter with its origin.

    We need this because both values are decided outside the logging call. The
    service name is fixed per process; the correlation id changes per unit of
    work. Attaching them here means every line gets tagged — INCLUDING lines
    emitted by SQLAlchemy or uvicorn, which know nothing about our services or
    our request ids. That is the payoff: you never have to remember to pass
    the id, and third-party output lands in the right story anyway.
    """

    def __init__(self, service_name: str) -> None:
        super().__init__()
        self.service_name = service_name

    def filter(self, record: logging.LogRecord) -> bool:
        record.service = self.service_name
        # Read at RECORD time, not at setup time — that is what makes it
        # follow whichever unit of work happens to be running right now.
        record.cid = _correlation_id.get()
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
    # its records ("Application startup complete") never travel up to the root
    # logger — so they would print on the console but be missing from the
    # file. A flight recorder with gaps in it is worse than no flight
    # recorder, because you trust it.
    #
    # Clearing its handlers and re-enabling propagation routes those records
    # through ours instead: same format, same file, one story.
    #
    # NOTE "uvicorn.access" is deliberately NOT in this list, and leaving it
    # out was a bug fix, not an oversight.
    #
    # uvicorn implements its --no-access-log flag by emptying that logger's
    # handlers. But our setup() runs LATER (when the app module is imported),
    # so re-enabling propagation here would quietly resurrect the access log
    # and make the flag appear to do nothing. That is a nasty class of bug:
    # two pieces of code configuring the same logger, last one wins, and
    # neither is obviously wrong on its own.
    #
    # We do not want uvicorn's access line anyway. It is emitted OUTSIDE our
    # middleware, so the correlation scope has already closed and it would be
    # tagged "--------". Our own middleware logs the same request with the
    # status, the duration, and the id attached. One access log, not two.
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
    """Logger for a submodule, e.g. get_logger(__name__).

    Assumes setup() already ran in the entrypoint. Loggers form a tree by
    dotted name, so these inherit the root handlers automatically — you never
    attach handlers anywhere but the root.
    """
    return logging.getLogger(name)
