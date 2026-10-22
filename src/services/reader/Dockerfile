FROM python:3-alpine

RUN pip3 install pika
RUN pip install nltk && python -c "import nltk; nltk.download('stopwords')"

WORKDIR /app

COPY common ./common/
COPY services/gateway .

ENTRYPOINT ["python3", "main.py"]