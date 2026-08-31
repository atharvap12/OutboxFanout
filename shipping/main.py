"""Shipping consumer entrypoint. See billing/main.py for the annotated version.

    set -a; source .env; set +a
    python -m shipping.main
"""

from shared import config, consumer
from shared.db import Base, engine
from shared.log import setup

# Registers Shipment on Base.metadata — see billing/main.py for why this
# apparently-unused import is load-bearing.
from shipping import models  # noqa: F401
from shipping.service import handle

log = setup("shipping")


def main() -> None:
    log.info("shipping consumer starting")
    # Creates only `shipments`. Note that Billing does the same thing for its
    # own table at the same moment, and the two do not collide: they register
    # different tables, and CREATE TABLE IF NOT EXISTS is safe to race.
    Base.metadata.create_all(bind=engine)
    consumer.run(config.SHIPPING_QUEUE_NAME, handle)


if __name__ == "__main__":
    main()
