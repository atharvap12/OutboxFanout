# Build from the REPO ROOT — this copies from notifications/ and shared/, and a
# build can only see files inside its context:
#
#   build:
#     context: .
#     dockerfile: notifications/Dockerfile

FROM python:3.13-slim

# PYTHONUNBUFFERED so log lines appear as they are written rather than being
# batched — a consumer that looks silent for a minute and then bursts is
# indistinguishable from a hung one.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Requirements before source, so editing a .py file reuses the cached pip layer
# instead of reinstalling everything.
COPY notifications/requirements.txt ./notifications/requirements.txt
RUN pip install --no-cache-dir -r notifications/requirements.txt

COPY shared/ ./shared/
COPY notifications/ ./notifications/

# ---------------------------------------------------------------------------
# TWO DELIBERATE ABSENCES IN THIS IMAGE.
#
# (1) order/ is not here, exactly as in billing/ and shipping/. This consumer's
#     only contract is the message on notify-queue.
#
# (2) There is no database driver either — see notifications/requirements.txt.
#     shared/db.py DOES get copied in (it lives in shared/), but importing it
#     would fail on `import sqlalchemy`, so nothing here can reach Postgres even
#     by accident. The separation of dedup stores is structural, not just
#     conventional.
# ---------------------------------------------------------------------------

# Containers run as root by default; drop privileges.
RUN useradd --create-home --shell /bin/bash appuser \
    && chown -R appuser:appuser /app
USER appuser

# No EXPOSE, no port: nothing ever connects TO a consumer. It is a client of
# SQS and Redis that happens to run forever.
CMD ["python", "-m", "notifications.main"]
