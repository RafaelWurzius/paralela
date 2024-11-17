FROM python:3.11-slim

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir pika

# Comando para rodar o script do produtor
CMD ["python", "producer.py"]
