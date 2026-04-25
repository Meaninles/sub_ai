FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    APP_HOME=/opt/ai-discovery \
    APP_DATA_DIR=/data \
    PYTHONPATH=/opt/ai-discovery/src

WORKDIR /opt/ai-discovery

COPY pyproject.toml README.md ./
COPY src ./src
COPY .env.example ./defaults/.env.example
COPY sub_sites.md ./defaults/sub_sites.md
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh

RUN chmod +x /usr/local/bin/docker-entrypoint.sh \
    && pip install .

WORKDIR /data

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["python3", "-m", "ai_discovery", "serve-admin", "--host", "0.0.0.0"]
