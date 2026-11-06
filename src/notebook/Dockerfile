FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/app:/app/src \
    NLTK_DATA=/usr/local/share/nltk_data

WORKDIR /app

RUN python -m pip install --upgrade pip setuptools wheel

RUN pip install --no-cache-dir \
    --only-binary=:all: \
    pandas==2.3.3 \
    pyarrow==23.0.1 \
    nltk

RUN python -m nltk.downloader \
    -d /usr/local/share/nltk_data stopwords

COPY src/notebook/main.py ./main.py

CMD ["python", "main.py"]