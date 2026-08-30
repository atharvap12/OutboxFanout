# Build from the REPO ROOT — this copies from billing/ and shared/:
#   build:
#     context: .
#     dockerfile: billing/Dockerfile

FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY billing/requirements.txt ./billing/requirements.txt
RUN pip install --no-cache-dir -r billing/requirements.txt

COPY shared/ ./shared/
COPY billing/ ./billing/

# Note what is NOT here: order/. The relay image needs it (one database, one
# schema definition), but Billing is an independent service whose only
# contract is the queue message. Leaving it out makes that boundary a build
# error rather than a rule someone has to remember.

RUN useradd --create-home --shell /bin/bash appuser \
    && chown -R appuser:appuser /app
USER appuser

# No EXPOSE: it serves nothing. It is a client of SQS and Postgres only.
CMD ["python", "-m", "billing.main"]
