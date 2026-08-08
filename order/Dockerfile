# Build from the REPO ROOT — this copies from both order/ and shared/, and a
# build can only see files inside its context:
#   build:
#     context: .
#     dockerfile: order/Dockerfile

FROM python:3.13-slim

# PYTHONUNBUFFERED so logs appear immediately rather than being batched when
# stdout is not a terminal.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Requirements before source: layers are cached by input, so editing code
# reuses the installed dependencies instead of reinstalling them.
COPY order/requirements.txt ./order/requirements.txt
RUN pip install --no-cache-dir -r order/requirements.txt

COPY shared/ ./shared/
COPY order/ ./order/

# Containers run as root by default; drop privileges.
RUN useradd --create-home --shell /bin/bash appuser \
    && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

# --host 0.0.0.0 is required: the default 127.0.0.1 accepts only connections
# originating inside this container, so the server would start cleanly and
# refuse every external request.
# --no-access-log: the correlation middleware already logs method, path,
# status and duration, tagged with the request id.
CMD ["uvicorn", "order.main:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
