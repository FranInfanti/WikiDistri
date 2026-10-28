FROM python:3-alpine

RUN pip3 install pika
RUN pip3 install requests

WORKDIR /app

COPY common ./common/
COPY services/downloader .

ENTRYPOINT ["python3", "main.py"]