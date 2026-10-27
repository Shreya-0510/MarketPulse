FROM python:3.12-slim
WORKDIR /app
COPY services/ingestion/requirements.txt /tmp/req.txt
RUN pip install --no-cache-dir --retries 10 --timeout 120 -r /tmp/req.txt
COPY pyproject.toml /app/pyproject.toml
COPY services/common /app/services/common
RUN pip install --no-cache-dir --no-deps /app
COPY services/ingestion/mp_ingestion /app/mp_ingestion
COPY config /app/config
RUN useradd -r -m -u 10001 mp && chown -R mp /app
USER mp
ENV MARKETPULSE_CONFIG_DIR=/app/config PYTHONUNBUFFERED=1 PYTHONPATH=/app
CMD ["python", "-m", "mp_ingestion.run_market"]
