FROM python:3.12-slim-bookworm

# agy y opencode se copian desde vendor/ (no versionado). Ver README.
ARG AGY_VERSION=1.5.9
ARG OPENCODE_VERSION=2.2.6
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 --home-dir /var/lib/ocdata --shell /usr/sbin/nologin ocrun \
 && install -d -o 10001 -g 10001 /var/lib/ocdata

COPY vendor/agy /usr/local/bin/agy
RUN chmod 0755 /usr/local/bin/agy \
 && /usr/local/bin/agy --version | grep -q "${AGY_VERSION}"

COPY vendor/opencode /usr/local/bin/opencode
RUN chmod 0755 /usr/local/bin/opencode \
 && /usr/local/bin/opencode --version | grep -q "${OPENCODE_VERSION}"

WORKDIR /app
COPY server.py /app/server.py

ENV HOST=0.0.0.0 \
    PORT=11444 \
    AGY_BIN=/usr/local/bin/agy \
    AGY_HOME=/root/.gemini/antigravity-cli \
    STATE_DIR=/state \
    HOME=/root \
    PYTHONUNBUFFERED=1 \
    OPENCODE_BIN=/usr/local/bin/opencode \
    OPENCODE_UID=10001 \
    OPENCODE_HOME=/var/lib/ocdata

EXPOSE 11444

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python3 -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:11444/health', timeout=4).status == 200 else 1)"

ENTRYPOINT ["python3", "/app/server.py"]
