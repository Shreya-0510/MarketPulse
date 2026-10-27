FROM python:3.12-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
WORKDIR /app
RUN pip install pyspark==3.5.3 pymongo==4.9.1 pydantic==2.9.2 PyYAML==6.0.2 vaderSentiment==3.3.2
COPY infra/docker/fetch_spark_jars.py /tmp/fetch_spark_jars.py
RUN python /tmp/fetch_spark_jars.py
COPY pyproject.toml /app/pyproject.toml
COPY services/common /app/services/common
RUN pip install --no-deps /app
COPY services/streaming/mp_streaming /app/mp_streaming
COPY config /app/config
RUN useradd -r -m -u 10001 mp && mkdir -p /checkpoints && chown -R mp /app /checkpoints
USER mp
ENV MARKETPULSE_CONFIG_DIR=/app/config PYTHONUNBUFFERED=1 PYTHONPATH=/app TZ=UTC
VOLUME /checkpoints
CMD ["python", "-m", "mp_streaming.market_job"]
