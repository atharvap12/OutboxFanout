# Build from the REPO ROOT — copies from notifications/ and shared/:
#   build:
#     context: .
#     dockerfile: notifications/Dockerfile

FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

COPY notifications/requirements.txt ./notifications/requirements.txt
RUN pip install --no-cache-dir -r notifications/requirements.txt

COPY shared/ ./shared/
COPY notifications/ ./notifications/

# No order/ (same boundary as billing and shipping) and no database driver at
# all. shared/db.py ships in shared/ but is never imported, so nothing here can
# reach Postgres even by accident.

RUN useradd --create-home --shell /bin/bash appuser \
    && chown -R appuser:appuser /app
USER appuser

CMD ["python", "-m", "notifications.main"]
