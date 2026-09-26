# syntax=docker/dockerfile:1

# ---- build stage ----------------------------------------------------
FROM python:3.11-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build
COPY pyproject.toml ./
COPY src ./src
RUN pip install --prefix=/install .

# ---- runtime stage --------------------------------------------------
FROM python:3.11-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/install/bin:$PATH" \
    PYTHONPATH="/install/lib/python3.11/site-packages"

# Run as an unprivileged user — never root.
RUN useradd --create-home --uid 10001 appuser
COPY --from=builder /install /install
WORKDIR /home/appuser/app
COPY --chown=appuser:appuser src ./src
USER appuser

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD python -c "import app" || exit 1

CMD ["python", "-m", "app"]
