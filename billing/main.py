"""Billing consumer entrypoint.

Local run:
    set -a; source .env; set +a
    python -m billing.main
"""

from shared import config, consumer
from shared.db import Base, engine
from shared.log import setup

# Registers BillingRecord on Base.metadata. Looks unused; without it
# create_all() below would find no tables.
from billing import models  # noqa: F401
from billing.service import handle

log = setup("billing")


def main() -> None:
    log.info("billing consumer starting")
    # Only billing_records is registered here — order.models is not imported
    # (and is not even in this image), so this cannot touch the Order
    # Service's schema. Each service creates the tables it owns.
    Base.metadata.create_all(bind=engine)
    consumer.run(config.BILLING_QUEUE_NAME, handle)


if __name__ == "__main__":
    main()
