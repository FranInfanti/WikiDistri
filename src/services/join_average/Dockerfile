FROM python:3-alpine

RUN pip3 install pika

WORKDIR /app

COPY common ./common/
COPY services/join_average .

ENTRYPOINT ["python3", "main.py"]