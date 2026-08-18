# Build from the REPO ROOT — this copies from relay/, shared/ and order/:
#   build:
#     context: .
#     dockerfile: relay/Dockerfile

FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Requirements before source so editing code reuses the installed layer.
COPY relay/requirements.txt ./relay/requirements.txt
RUN pip install --no-cache-dir -r relay/requirements.txt

COPY shared/ ./shared/
# The relay imports order.models: `orders` and `outbox` live in one database
# and one schema definition, so redeclaring the table here would be a second
# source of truth. Only models.py is imported — routes.py and friends ride
# along because they are the same package, and are never loaded (which is why
# fastapi is absent from requirements.txt and nothing breaks).
COPY order/ ./order/
COPY relay/ ./relay/

RUN useradd --create-home --shell /bin/bash appuser \
    && chown -R appuser:appuser /app
USER appuser

# No EXPOSE: the relay serves nothing. It is a client of Postgres and SNS only.
# `python -m` rather than a path so relative imports resolve as a package.
CMD ["python", "-m", "relay.main"]
