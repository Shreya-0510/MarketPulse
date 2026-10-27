FROM python:3.12-slim
WORKDIR /app
COPY services/api/requirements.txt /tmp/req.txt
RUN pip install --no-cache-dir -r /tmp/req.txt
COPY pyproject.toml /app/pyproject.toml
COPY services/common /app/services/common
RUN pip install --no-cache-dir --no-deps /app
COPY services/api/app /app/app
COPY config /app/config
RUN useradd -r -u 10001 mp && chown -R mp /app
USER mp
ENV MARKETPULSE_CONFIG_DIR=/app/config PYTHONUNBUFFERED=1
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
