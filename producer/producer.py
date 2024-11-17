import pika
import json
import threading
import random
import time
from typing import Dict

class RabbitmqHandler:
    def __init__(self):
        # Configuração de conexão com o RabbitMQ
        self.__host = "192.168.1.2"
        self.__port = 5672
        self.__username = "user"
        self.__password = "password"
        self.__exchange = "data_exchange"
        self.__response_queue = "response_queue"
        self.__routing_keys = ['a', 'b', 'c', 'd', 'e', 'f']
        self.__producer_channel = self.__create_channel()
        self.__consumer_channel = self.__create_channel()
        self.__setup_response_queue()

    def __create_channel(self):
        """Cria e retorna um canal de conexão com o RabbitMQ."""
        connection_parameters = pika.ConnectionParameters(
            host=self.__host,
            port=self.__port,
            credentials=pika.PlainCredentials(
                username=self.__username,
                password=self.__password
            )
        )
        connection = pika.BlockingConnection(connection_parameters)
        channel = connection.channel()
        channel.exchange_declare(exchange=self.__exchange, exchange_type='direct')
        return channel

    def __setup_response_queue(self):
        """Declara a fila de resposta."""
        self.__consumer_channel.queue_declare(queue=self.__response_queue, durable=True)

    def send_request(self, body: Dict):
        """Envia uma requisição para uma das filas."""
        routing_key = random.choice(self.__routing_keys)
        self.__producer_channel.basic_publish(
            exchange=self.__exchange,
            routing_key=routing_key,
            body=json.dumps(body),
            properties=pika.BasicProperties(
                delivery_mode=2 
            )
        )
        print(f"Requisição enviada para a fila '{routing_key}': {body['id']}")

    def consume_responses(self, callback):
        """Consome mensagens da fila de respostas."""
        self.__consumer_channel.basic_consume(
            queue=self.__response_queue,
            on_message_callback=callback,
            auto_ack=True
        )
        print(f"Consumidor aguardando respostas na fila '{self.__response_queue}'...")
        self.__consumer_channel.start_consuming()

def send_requests(handler: RabbitmqHandler):
    """Gera e envia 1000 requisições ao RabbitMQ."""
    for i in range(1000):
        request = {
            "id": i + 1,
            "location": {
                "latitude": round(random.uniform(-90, 90), 6),
                "longitude": round(random.uniform(-180, 180), 6)
            },
            "timestamp": time.time()
        }
        handler.send_request(request)
        # time.sleep(0.01)  # Atraso para simular envio contínuo

def handle_response(ch, method, properties, body):
    """Processa uma mensagem de resposta."""
    response = json.loads(body)
    print(f"Resposta recebida: Requisição {response['request_id']} atribuída ao motorista {response['driver_id']}.")

def consume_responses(handler: RabbitmqHandler):
    """Inicia a thread para consumir as respostas."""
    handler.consume_responses(handle_response)

def main():
    handler = RabbitmqHandler()

    # Thread para enviar requisições
    producer_thread = threading.Thread(target=send_requests, args=(handler,))
    producer_thread.start()

    # Thread para consumir respostas
    consumer_thread = threading.Thread(target=consume_responses, args=(handler,))
    consumer_thread.start()

    producer_thread.join()
    consumer_thread.join()

if __name__ == "__main__":
    time.sleep(20)
    main()
