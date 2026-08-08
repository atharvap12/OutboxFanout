"""SQLAlchemy engine, session factory, and transaction boundaries.

Engine: one per process, holds the connection pool, thread-safe.
Session: one per unit of work, cheap, NOT thread-safe, holds an open
transaction (and its locks) — so never share one globally.
"""

from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from shared import config

engine = create_engine(
    config.DATABASE_URL,
    # Validate pooled connections before use. Required here because
    # `docker compose down`/`up` kills every connection the pool still holds.
    pool_pre_ping=True,
    echo=config.SQL_ECHO,
)

SessionLocal = sessionmaker(
    bind=engine,
    # No implicit flush on query: writes happen only when we say so.
    autoflush=False,
    # Keep attributes readable after commit (the endpoint returns order.id).
    expire_on_commit=False,
    class_=Session,
)


class Base(DeclarativeBase):
    """Declarative base. `orders` and `outbox` are owned by the Order Service;
    the relay may import them (shared database by design). Billing and
    Shipping must not — their only contract is the queue message."""


@contextmanager
def session_scope() -> Generator[Session]:
    """One session, one transaction: commit on clean exit, rollback on error.

        with session_scope() as session:
            session.add(order)
            session.add(outbox_row)

    Do not call session.commit() inside the block — an intermediate commit
    splits the atomic write into two transactions. Use session.flush() if you
    need a database-generated value while staying inside the transaction.
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_session() -> Generator[Session]:
    """FastAPI dependency. Lends a session; the ENDPOINT commits.

    Does not auto-commit because FastAPI runs dependency cleanup *after* the
    response is sent — a failed commit there would leave the client holding a
    201 for an order that does not exist. Rollback and close still happen here.
    """
    session = SessionLocal()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        # Closing without a commit discards the transaction: no rows appear.
        session.close()
