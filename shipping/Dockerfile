# Build from the REPO ROOT — this copies from shipping/ and shared/:
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

# order/ is deliberately absent — see billing/Dockerfile. A separate image
# from Billing's, though the contents are near-identical, because these are
# separate services: Billing must be deployable without redeploying Shipping.

RUN useradd --create-home --shell /bin/bash appuser \
    && chown -R appuser:appuser /app
USER appuser

CMD ["python", "-m", "shipping.main"]
