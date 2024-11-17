# Usar uma imagem oficial do Python
FROM python:3.11-slim

# Instalar dependências necessárias
RUN pip install psycopg2-binary

# Definir diretório de trabalho
WORKDIR /app

# Copiar o script para o contêiner
COPY driver_updater.py .
COPY criarFilas.py .

# Instale as dependências necessárias
RUN pip install --no-cache-dir pika

# Comando para rodar o script
CMD ["python", "criarFilas.py"]
# CMD ["python", "driver_updater.py"]
