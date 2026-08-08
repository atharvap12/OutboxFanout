# Order Service image.
#
# IMPORTANT — build context.
# This Dockerfile copies from BOTH order/ and shared/, so it must be built
# with the repo root as its context, not the order/ directory:
#
#   build:
#     context: .                      <- root, so shared/ is visible
#     dockerfile: order/Dockerfile
#
# A Dockerfile can only see files inside its context. Think of the context as
# the box you hand to the builder: anything outside the box does not exist,
# no matter what path you write.

FROM python:3.13-slim

# Two settings that matter specifically in containers:
#
# PYTHONDONTWRITEBYTECODE — skip .pyc files. The container is thrown away, so
#   caching compiled bytecode to disk only adds clutter.
# PYTHONUNBUFFERED — print immediately instead of holding output in a buffer.
#   Without it, Python batches stdout when it is not a terminal, so
#   `docker compose logs` shows nothing for ages and then dumps everything at
#   once — which makes a crash look like it happened at the wrong moment.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

WORKDIR /app

# Copy requirements FIRST, install, and only then copy the code.
#
# This is the single most useful Docker trick. Each instruction becomes a
# cached layer, and a layer is rebuilt only when its inputs change. Since your
# source changes constantly but your dependencies rarely do, this ordering
# means editing a Python file reuses the cached install instead of
# re-downloading everything. Copy the code first and every edit costs a full
# reinstall.
COPY order/requirements.txt ./order/requirements.txt
RUN pip install --no-cache-dir -r order/requirements.txt

# Now the code. Cheap layers, rebuilt often.
COPY shared/ ./shared/
COPY order/ ./order/

# Run as a non-root user.
#
# By default everything in a container runs as root. If a process is ever
# compromised, root inside the container is a much better starting point for
# an attacker than an unprivileged account. Costs nothing; skip it and it
# becomes a habit you carry into production.
RUN useradd --create-home --shell /bin/bash appuser \
    && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

# --host 0.0.0.0 is REQUIRED, and the reason is worth internalising.
#
# The default, 127.0.0.1, means "only accept connections from this same
# machine" — and inside a container, "this machine" is the container itself.
# The server would start perfectly, report no errors, and refuse every request
# arriving from outside. 0.0.0.0 means "accept on every network interface".
#
# This is the flip side of the localhost lesson: inside a container, localhost
# is a smaller world than you think.
CMD ["uvicorn", "order.main:app", "--host", "0.0.0.0", "--port", "8000"]
