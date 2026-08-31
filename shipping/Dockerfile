# Build from the REPO ROOT — copies from shipping/ and shared/:
#
#   build:
#     context: .
#     dockerfile: shipping/Dockerfile

FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY shipping/requirements.txt ./shipping/requirements.txt
RUN pip install --no-cache-dir -r shipping/requirements.txt

COPY shared/ ./shared/
COPY shipping/ ./shipping/

# order/ is deliberately absent, exactly as in billing/Dockerfile.
#
# And this is a SEPARATE IMAGE from Billing's even though the contents are
# currently near-identical — deliberate duplication, bought for one reason:
# Billing must be deployable without redeploying Shipping. Two services that
# share an image share a release, and then they are not two services.
#
# (Contrast the `bootstrap` container, which DOES reuse the relay's image. That
# is not a service — it is a one-shot script with the same dependencies and no
# independent release cycle of its own.)

RUN useradd --create-home --shell /bin/bash appuser \
    && chown -R appuser:appuser /app
USER appuser

CMD ["python", "-m", "shipping.main"]
