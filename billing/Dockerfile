# Build from the REPO ROOT, because this copies from both billing/ and shared/
# and a build can only see files inside its context:
#
#   build:
#     context: .
#     dockerfile: billing/Dockerfile

FROM python:3.13-slim

# PYTHONUNBUFFERED so log lines appear the moment they are written. Without it
# Python batches stdout when it is not a terminal, and `docker compose logs`
# shows nothing for ages and then a burst — which looks exactly like a hung
# consumer.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Requirements BEFORE source. Docker caches each layer keyed on its inputs, so
# with this order editing a .py file reuses the (slow) pip install layer.
# Copying source first would reinstall every dependency on every code change.
COPY billing/requirements.txt ./billing/requirements.txt
RUN pip install --no-cache-dir -r billing/requirements.txt

COPY shared/ ./shared/
COPY billing/ ./billing/

# ---------------------------------------------------------------------------
# LOOK AT WHAT IS NOT COPIED: order/.
#
# The relay's image DOES contain order/, and correctly so — the relay reads the
# outbox table, which is part of the Order Service's schema, and one schema
# should have exactly one definition.
#
# Billing is the opposite case. Its only contract is the message on its queue.
# So instead of writing "Billing must not import order.models" in a document
# that someone will eventually not read, we make it TRUE OF THE IMAGE: the
# files are not there, and an attempted import is an ImportError at startup.
#
# A boundary you can enforce is worth more than a boundary you can document.
# ---------------------------------------------------------------------------

# Containers run as root by default. Drop privileges — a compromised consumer
# should not be root inside its own container.
RUN useradd --create-home --shell /bin/bash appuser \
    && chown -R appuser:appuser /app
USER appuser

# No EXPOSE and no port: this serves nothing. Nothing ever connects TO it.
CMD ["python", "-m", "billing.main"]
