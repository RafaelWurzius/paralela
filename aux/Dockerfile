FROM python:3.11-slim

RUN pip install psycopg2-binary

WORKDIR /app

COPY auxiliar.py .

RUN pip install --no-cache-dir pika

CMD ["python", "auxiliar.py"]
