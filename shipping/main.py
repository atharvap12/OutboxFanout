"""Shipping consumer entrypoint.

Local run:
    set -a; source .env; set +a
    python -m shipping.main
"""

from shared import config, consumer
from shared.db import Base, engine
from shared.log import setup

from shipping import models  # noqa: F401  registers Shipment on Base.metadata
from shipping.service import handle

log = setup("shipping")


def main() -> None:
    log.info("shipping consumer starting")
    Base.metadata.create_all(bind=engine)
    consumer.run(config.SHIPPING_QUEUE_NAME, handle)


if __name__ == "__main__":
    main()
