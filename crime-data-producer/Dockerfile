FROM python:3.7-slim

WORKDIR /app

COPY . .

RUN python3 -m pip install --no-cache-dir --upgrade pip

RUN python3 -m pip install kafka-python

CMD python3 ./main.py