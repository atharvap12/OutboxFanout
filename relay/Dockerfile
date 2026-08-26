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

# The bootstrap script rides along in this image rather than getting one of its
# own. It needs exactly what the relay needs — boto3 and shared/ — and building
# a second image for a script that runs for two seconds is not worth it. The
# `bootstrap` compose service is this same image with a different command.
COPY bootstrap/ ./bootstrap/

RUN useradd --create-home --shell /bin/bash appuser \
    && chown -R appuser:appuser /app
USER appuser

# No EXPOSE: the relay serves nothing. It is a client of Postgres and SNS only.
# `python -m` rather than a path so relative imports resolve as a package.
CMD ["python", "-m", "relay.main"]
