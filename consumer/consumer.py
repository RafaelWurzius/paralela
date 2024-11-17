import pika
import json
import threading
import time
import psycopg2
import uuid
from psycopg2.extras import RealDictCursor
from datetime import datetime

# Lock para garantir acesso exclusivo ao banco de dados
db_lock = threading.Lock()
priority_lock = threading.Lock()  # Lock para controlar a priorização entre threads

# Configuração do Banco de Dados
DB_CONFIG = {
    "dbname": "ride_app",
    "user": "admin",
    "password": "adminpass",
    "host": "192.168.1.5",
    "port": 5432
}

# Configuração do RabbitMQ
RABBITMQ_CONFIG = {
    "host": "192.168.1.2",
    "port": 5672,
    "username": "user",
    "password": "password",
    "exchange": "data_exchange",
    "response_queue": "response_queue"
}

# Dicionário para rastrear tentativas de cada thread
thread_attempts = {}

class RabbitmqPublisher:
    def __init__(self):
        self.__host = RABBITMQ_CONFIG['host']
        self.__port = RABBITMQ_CONFIG['port']
        self.__username = RABBITMQ_CONFIG['username']
        self.__password = RABBITMQ_CONFIG['password']
        self.__response_queue = RABBITMQ_CONFIG['response_queue']
        self.__channel = self.__create_channel()

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
        channel.queue_declare(queue=self.__response_queue, durable=True)
        return channel

    def send_response(self, response: dict):
        """Envia uma mensagem de resposta para a fila de respostas."""
        self.__channel.basic_publish(
            exchange='',
            routing_key=self.__response_queue,
            body=json.dumps(response),
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )
        print(f"Mensagem de resposta enviada: {response}")

def get_available_driver():
    """Verifica se há motoristas disponíveis e retorna um, ou None se nenhum estiver disponível."""
    with db_lock:  # Garante acesso exclusivo ao banco de dados
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            # Verificar disponibilidade de motoristas
            cursor.execute("SELECT id, latitude, longitude FROM drivers WHERE available = TRUE LIMIT 1;")
            driver = cursor.fetchone()

            if driver:
                # Atualizar status do motorista no banco
                cursor.execute("UPDATE drivers SET available = FALSE WHERE id = %s;", (driver['id'],))
                conn.commit()

            conn.close()
            return driver
        except Exception as e:
            print(f"Erro ao acessar o banco de dados: {e}")
            return None

def process_request(ch, method, properties, body, thread_name, publisher: RabbitmqPublisher):
    """Processa uma requisição de corrida com verificação de prioridade."""
    request = json.loads(body)
    print(f"Thread {thread_name}: Requisição recebida da fila '{method.routing_key}': {request['id']}")

    driver = None

    while driver is None:
        # Incrementa tentativas da thread
        with priority_lock:
            thread_attempts[thread_name] += 1
            max_attempts = max(thread_attempts.values())
            if thread_attempts[thread_name] < max_attempts:
                print(f"Thread {thread_name}: Aguardando prioridade. Tentativas: {thread_attempts[thread_name]}")
                time.sleep(1)  # Espera antes de tentar novamente
                continue

        print(f"Thread {thread_name}: Verificando disponibilidade de motoristas para a requisição {request['id']}...")
        driver = get_available_driver()

        if driver is None:
            print(f"Thread {thread_name}: Sem motoristas disponíveis para a requisição {request['id']}. Retentando em 2 segundos...")
            time.sleep(2)  # Aguarda antes de tentar novamente

    # Registrar a corrida no banco
    with db_lock:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            # Gerar um UUID para ride_id
            ride_id = str(uuid.uuid4())

            # Converter timestamp Unix para datetime
            ride_timestamp = datetime.fromtimestamp(request['timestamp'])

            cursor.execute(
                """
                INSERT INTO rides (ride_id, driver_id, passenger_location, timestamp) 
                VALUES (%s, %s, %s, %s);
                """,
                (ride_id, driver['id'], json.dumps(request['location']), ride_timestamp)
            )
            conn.commit()

            conn.close()

            print(f"Thread {thread_name}: Requisição {request['id']} atribuída ao motorista {driver['id']}.")
        except Exception as e:
            print(f"Thread {thread_name}: Erro ao registrar a corrida: {e}")

    # Enviar mensagem de resposta para a fila de respostas
    response = {
        "request_id": request['id'],
        "driver_id": driver['id']
    }
    publisher.send_response(response)

    # Confirma que a mensagem foi processada
    ch.basic_ack(delivery_tag=method.delivery_tag)

def worker(queue_name: str, thread_name: str, publisher: RabbitmqPublisher):
    """Cria um consumidor RabbitMQ para processar mensagens de uma fila específica."""
    consumer = RabbitmqConsumer(queue_name)

    def callback(ch, method, properties, body):
        process_request(ch, method, properties, body, thread_name, publisher)

    consumer.consume_messages(callback)

def main():
    # Filas correspondentes às routing keys
    queues = ['queue1', 'queue2', 'queue3', 'queue4', 'queue5', 'queue6']

    threads = []
    publisher = RabbitmqPublisher()

    # Controla tentativas para cada thread
    for i, queue in enumerate(queues):
        thread_name = f"Thread-{i+1}"
        thread_attempts[thread_name] = 0

        thread = threading.Thread(target=worker, args=(queue, thread_name, publisher))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

class RabbitmqConsumer:
    def __init__(self, queue_name: str) -> None:
        self.__host = RABBITMQ_CONFIG['host']
        self.__port = RABBITMQ_CONFIG['port']
        self.__username = RABBITMQ_CONFIG['username']
        self.__password = RABBITMQ_CONFIG['password']
        self.__exchange = RABBITMQ_CONFIG['exchange']
        self.__queue = queue_name
        self.__channel = self.__create_channel()

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
        # channel.exchange_declare(exchange=self.__exchange, exchange_type='direct')
        # channel.queue_declare(queue=self.__queue, durable=True)
        channel.queue_bind(exchange=self.__exchange, queue=self.__queue, routing_key=self.__queue)
        return channel

    def consume_messages(self, callback):
        """Consome mensagens da fila e as entrega ao callback."""
        self.__channel.basic_qos(prefetch_count=1)
        self.__channel.basic_consume(queue=self.__queue, on_message_callback=callback)
        print(f"Consumidor aguardando mensagens na fila '{self.__queue}'...")
        self.__channel.start_consuming()

if __name__ == "__main__":
    time.sleep(25)
    main()
