# Use uma imagem base do Python
FROM python:3.11-slim

# Defina o diretório de trabalho no container
WORKDIR /app

# Copie os arquivos do projeto para o container
COPY . /app

# Instale as dependências necessárias
RUN pip install --no-cache-dir pika

# Comando para rodar o script do produtor
CMD ["python", "producer.py"]
