"""SQLAlchemy engine, session factory, and an explicit transaction boundary.

The engine is created once per process and holds a connection pool. Sessions
are cheap, short-lived objects checked out from that pool — create one per
request or per unit of work, never one global session shared by everything.
"""

from collections.abc import Generator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from shared import config

# ---------------------------------------------------------------------------
# Engine — one per process, created at import.
# ---------------------------------------------------------------------------

engine = create_engine(
    config.DATABASE_URL,
    # Send a cheap "are you still there?" before handing out a pooled
    # connection. Matters here specifically: this project restarts Postgres
    # constantly (`docker compose down`/`up`), which leaves every pooled
    # connection dead. Without this you get a confusing "server closed the
    # connection unexpectedly" on the first query after each restart.
    pool_pre_ping=True,
    # Log every statement. Set SQL_ECHO=1 in Phase 1 to literally watch both
    # INSERTs appear between one BEGIN and one COMMIT — that is the atomic
    # write, made visible.
    echo=config.SQL_ECHO,
)

# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------

SessionLocal = sessionmaker(
    bind=engine,
    # autoflush=False: don't let SQLAlchemy secretly emit INSERTs just because
    # you ran a query mid-transaction. In a project about knowing exactly when
    # writes happen, surprise flushes are the enemy.
    autoflush=False,
    # expire_on_commit=False: by default SQLAlchemy marks every attribute
    # stale after commit, so reading order.id afterwards fires a fresh SELECT
    # — or raises if the session is already closed. Since the endpoint returns
    # the order id after committing, keep the values usable.
    expire_on_commit=False,
    class_=Session,
)


class Base(DeclarativeBase):
    """Base class for ORM models.

    Both `orders` and `outbox` are owned by the Order Service and inherit from
    this. The relay may import the outbox model — it shares the Order
    Service's database by design. Billing and Shipping define their own
    tables and must not import Order's models; their only contract is the
    message that arrives on their queue.
    """


# ---------------------------------------------------------------------------
# Transaction boundary
# ---------------------------------------------------------------------------

@contextmanager
def session_scope() -> Generator[Session]:
    """One session, one transaction, committed or rolled back — never partial.

    This is the mechanism Phase 1 asks you to use *deliberately* rather than
    relying on autocommit defaults:

        with session_scope() as session:
            session.add(order)
            session.add(outbox_row)
        # commit happens here, on clean exit

    What actually happens:
      - SQLAlchemy opens a transaction lazily on the first statement (BEGIN).
      - Nothing is durable until commit() — the writes live inside the
        transaction, invisible to other connections.
      - Any exception inside the block skips the commit, runs rollback, and
        re-raises. Postgres discards every statement in that transaction, so
        a failed outbox INSERT takes the orders INSERT down with it.
      - close() returns the connection to the pool either way.

    That last point IS the outbox pattern. Two rows, two tables, one
    transaction, all-or-nothing — which is why the Order Service never has to
    talk to SNS to guarantee the event will eventually be sent.

    IMPORTANT: do not call session.commit() inside the block. An intermediate
    commit ends the transaction and silently splits your atomic write into two
    independent ones — exactly the dual-write bug you are trying to avoid.

    Use session.flush() instead if you need a database-generated value before
    the block ends: flush sends the SQL and populates the object while staying
    inside the same transaction.
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
    """FastAPI dependency. Lends a session; the ENDPOINT owns the commit.

        @app.post("/orders")
        def create_order(session: Session = Depends(get_session)):
            ...
            session.commit()      # <- you write this, on purpose

    Why this does NOT auto-commit, unlike session_scope():

    FastAPI runs the cleanup code of a `yield` dependency AFTER the response
    has already been sent to the client. So if the commit lived down here and
    it failed, the customer would already be holding a "201 Created" for an
    order that does not exist. A receipt for a sale that never happened.

    Committing inside the endpoint keeps the failure where it belongs: the
    exception is raised while FastAPI is still deciding what to send, so a
    failed commit becomes an honest 500 instead of a lie.

    It also makes the transaction boundary something you can point at in your
    own code, which is the whole exercise in Phase 1.

    This function still guarantees the safety net: any exception rolls back,
    and the connection always goes back to the pool.
    """
    session = SessionLocal()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        # If the endpoint never committed, closing here discards the
        # transaction. Silence is not success — no rows will appear.
        session.close()
