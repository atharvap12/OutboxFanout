# Outbox Relay image.
#
# Build from the REPO ROOT — this copies from relay/, shared/ AND order/:
#   build:
#     context: .
#     dockerfile: relay/Dockerfile
# The context is the box you hand the builder; anything outside it does not
# exist, whatever path you write.

FROM python:3.13-slim

# PYTHONUNBUFFERED matters more here than anywhere else in the project: we are
# about to kill this process on purpose and read its last words. Buffered
# output would hold those lines in memory and lose them on the crash.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Requirements first, code second — layers are cached by their inputs, and
# dependencies change far less often than code.
COPY relay/requirements.txt ./relay/requirements.txt
RUN pip install --no-cache-dir -r relay/requirements.txt

COPY shared/ ./shared/

# Why the relay image contains the Order Service's code: relay/service.py does
# `from order.models import OutboxEvent`. One database, one schema definition —
# re-declaring the outbox table here would create a second copy, and two copies
# of a definition always drift.
#
# Only models.py is imported; the rest rides along and is never loaded, which
# is why fastapi is absent from requirements.txt and nothing breaks. This is a
# narrow, deliberate exception — the Phase 4-5 consumers must NOT do it, since
# their only contract is the message on their queue.
COPY order/ ./order/

COPY relay/ ./relay/

# The bootstrap script rides along in THIS image rather than getting one of its
# own. It needs exactly what the relay needs — boto3 and shared/ — and building
# a second image for a script that runs for two seconds is not worth it. The
# `bootstrap` service in docker-compose.yml is this same image with a different
# command.
COPY bootstrap/ ./bootstrap/

RUN useradd --create-home --shell /bin/bash appuser \
    && chown -R appuser:appuser /app
USER appuser

# No EXPOSE: the relay serves nothing and listens on no port. It is purely a
# client of Postgres and SNS, which is also why it needs no healthcheck.
CMD ["python", "-m", "relay.main"]
