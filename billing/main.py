"""Billing consumer entrypoint. Wires the generic loop to Billing's handler.

Run it locally without Docker:

    set -a; source .env; set +a
    python -m billing.main

Note there is no uvicorn, no port, no HTTP anything. A consumer is not a
server — nobody calls it. It is a CLIENT of SQS and Postgres that happens to
run forever.
"""

from shared import config, consumer
from shared.db import Base, engine
from shared.log import setup

# LOOKS UNUSED. IS NOT. Importing the module is what runs the class body of
# BillingRecord, which is what registers the table on Base.metadata. Without
# this line, create_all() below would look at an empty metadata object, find
# nothing to do, and cheerfully create no tables — and the first INSERT would
# fail with "relation billing_records does not exist".
#
# `# noqa: F401` tells the linter we know it is unimported-but-needed.
from billing import models  # noqa: F401
from billing.service import handle

log = setup("billing")


def main() -> None:
    log.info("billing consumer starting")

    # Creates ONLY billing_records. Base.metadata contains just the tables that
    # have actually been imported in THIS process, and order.models is not
    # imported here (it is not even present in the image), so this physically
    # cannot touch the Order Service's schema. Each service creates the tables
    # it owns and no others.
    #
    # ⚠️ create_all() CREATES MISSING TABLES; IT NEVER ALTERS EXISTING ONES.
    # If you later change a column or move the UNIQUE constraint, this call
    # sees the table name already exists and silently does nothing — leaving
    # code that believes in a constraint the database does not have. The escape
    # in this project is `docker compose down -v`; the real answer is Alembic.
    Base.metadata.create_all(bind=engine)

    # Everything interesting now lives in two places: the loop
    # (shared/consumer.py) and the handler (billing/service.py). This file just
    # introduces them.
    consumer.run(config.BILLING_QUEUE_NAME, handle)


if __name__ == "__main__":
    main()
