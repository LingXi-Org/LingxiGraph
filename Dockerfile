# syntax=docker/dockerfile:1.7
FROM python:3.12-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1
WORKDIR /build
COPY pyproject.toml README.md requirements.lock ./
COPY src ./src
RUN python -m pip wheel --require-hashes --wheel-dir /wheels -r requirements.lock \
    && python -m pip wheel --no-deps --wheel-dir /wheels .

FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/home/lingxigraph/.local/bin:${PATH}"
RUN groupadd --system --gid 10001 lingxigraph \
    && useradd --system --uid 10001 --gid lingxigraph --create-home lingxigraph
WORKDIR /app
COPY --from=builder /wheels /wheels
RUN python -m pip install --no-cache-dir /wheels/* && rm -rf /wheels
COPY --chown=lingxigraph:lingxigraph lingxigraph.json ./
COPY --chown=lingxigraph:lingxigraph examples ./examples
USER 10001:10001
EXPOSE 8124
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8124/health', timeout=2)"
ENTRYPOINT ["lingxigraph"]
CMD ["server"]
