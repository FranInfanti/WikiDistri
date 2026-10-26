FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app/src \
    KAGGLEHUB_CACHE=/cache/kagglehub

WORKDIR /app

RUN pip install --no-cache-dir \
    kagglehub==0.4.3 \
    pandas==2.3.3 \
    pyarrow==23.0.1

COPY src/common ./src/common/
COPY src/services/client ./src/services/client/
COPY scripts/download_dataset.py ./scripts/download_dataset.py

CMD ["python", "src/services/client/main.py"]
