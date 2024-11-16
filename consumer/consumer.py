# import pika
# import json
# import threading
# import psycopg2
# from psycopg2.extras import RealDictCursor

# # Configuração do RabbitMQ
# RABBITMQ_HOST = "192.168.1.2"
# QUEUE_NAME = "ride_requests"

# # Configuração do Banco de Dados
# DB_CONFIG = {
#     "dbname": "ride_app",
#     "user": "admin",
#     "password": "adminpass",
#     "host": "database",
#     "port": 5432
# }

# def process_request(ch, method, properties, body):
#     """Processa uma requisição de corrida."""
#     try:
#         request = json.loads(body)
#         print(f"Processando requisição: {request['id']}")

#         # Conectar ao banco de dados
#         conn = psycopg2.connect(**DB_CONFIG)
#         cursor = conn.cursor(cursor_factory=RealDictCursor)

#         # Verificar disponibilidade de motoristas
#         cursor.execute("SELECT id, latitude, longitude FROM drivers WHERE available = TRUE LIMIT 1;")
#         driver = cursor.fetchone()

#         if driver:
#             # Atualizar status do motorista no banco
#             cursor.execute("UPDATE drivers SET available = FALSE WHERE id = %s;", (driver['id'],))
#             conn.commit()

#             # Registrar a corrida no banco
#             cursor.execute(
#                 "INSERT INTO rides (ride_id, driver_id, passenger_location, timestamp) VALUES (%s, %s, %s, %s);",
#                 (request['id'], driver['id'], json.dumps(request['location']), request['timestamp'])
#             )
#             conn.commit()

#             print(f"Requisição {request['id']} atribuída ao motorista {driver['id']}.")
#         else:
#             print(f"Sem motoristas disponíveis para a requisição {request['id']}.")

#         conn.close()

#     except Exception as e:
#         print(f"Erro ao processar requisição: {e}")

#     # Confirma que a mensagem foi processada
#     ch.basic_ack(delivery_tag=method.delivery_tag)

# def consume_from_queue():
#     """Consome mensagens da fila do RabbitMQ."""
#     connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
#     channel = connection.channel()

#     # Garantir que a fila existe
#     channel.queue_declare(queue=QUEUE_NAME)

#     # Consome mensagens da fila
#     channel.basic_qos(prefetch_count=1)
#     channel.basic_consume(queue=QUEUE_NAME, on_message_callback=process_request)

#     print("Consumidor aguardando mensagens...")
#     channel.start_consuming()

# def main():
#     # Criar múltiplas threads para processar mensagens simultaneamente
#     threads = []
#     for _ in range(6):  # Criar 6 threads, uma para cada fila
#         thread = threading.Thread(target=consume_from_queue)
#         thread.start()
#         threads.append(thread)

#     for thread in threads:
#         thread.join()

# if __name__ == "__main__":
#     main()

#---------------------------------------------------------------------

# import pika

# class RabbitMQConsumer:
#     def __init__(self, callback) -> None:
#         self.__host = "192.168.1.2"
#         self.__port = 5672
#         self.__username = "user"
#         self.__password = "password"
#         self.__queue = "data_queue"
#         self.__callback =callback
#         self.__channel = self.__create_channel()

#     def __create_channel(self):
#         conections_parameters = pika.ConnectionParameters(
#             host = self.__host,
#             port=self.__port,
#             credentials=pika.PlainCredentials(
#                 username=self.__username,
#                 password=self.__password
#             )
#         )

#         channel = pika.BlockingConnection(conections_parameters).channel()
#         channel.queue_declare(
#             queue=self.__queue,
#             durable=True
#         )

#         channel.basic_consume(
#             queue=self.__queue,
#             auto_ack=True,
#             on_message_callback= self.__callback
#         )
#         return channel
#     def start(self):
#         print(f"Listen RabbitMQ on port 5672")
#         self.__channel.start_consuming()


# def minha_callback(ch,method, propet, body):
#     print(body)

# consumer = RabbitMQConsumer(minha_callback)
# consumer.start()

#---------------------------------------------------------------------

# import pika
# import json
# import threading
# import psycopg2
# from psycopg2.extras import RealDictCursor

# class RabbitmqConsumer:
#     def __init__(self) -> None:
#         self.__host = "192.168.1.2"
#         self.__port = 5672
#         self.__username = "user"
#         self.__password = "password"
#         self.__exchange = "data_exchange"
#         # self.__queue = "ride_requests"
#         self.__queue = "data_queue"
#         self.__channel = self.__create_channel()

#     def __create_channel(self):
#         """Cria e retorna um canal de conexão com o RabbitMQ."""
#         connection_parameters = pika.ConnectionParameters(
#             host=self.__host,
#             port=self.__port,
#             credentials=pika.PlainCredentials(
#                 username=self.__username,
#                 password=self.__password
#             )
#         )
#         connection = pika.BlockingConnection(connection_parameters)
#         channel = connection.channel()
#         # channel.exchange_declare(exchange=self.__exchange, exchange_type='direct')
#         # channel.queue_declare(queue=self.__queue, durable=True)
#         channel.queue_bind(exchange=self.__exchange, queue=self.__queue)
#         return channel

#     def consume_messages(self, callback):
#         """Consome mensagens da fila e as entrega ao callback."""
#         self.__channel.basic_qos(prefetch_count=1)
#         self.__channel.basic_consume(queue=self.__queue, on_message_callback=callback)
#         print("Consumidor aguardando mensagens...")
#         self.__channel.start_consuming()

# # Configuração do Banco de Dados
# DB_CONFIG = {
#     "dbname": "ride_app",
#     "user": "admin",
#     "password": "adminpass",
#     "host": "192.168.1.5",
#     "port": 5432
# }

# def process_request(ch, method, properties, body):
#     """Processa uma requisição de corrida."""
#     try:
#         request = json.loads(body)
#         print(f"Processando requisição: {request['id']}")

#         # Conectar ao banco de dados
#         conn = psycopg2.connect(**DB_CONFIG)
#         cursor = conn.cursor(cursor_factory=RealDictCursor)

#         # Verificar disponibilidade de motoristas
#         cursor.execute("SELECT id, latitude, longitude FROM drivers WHERE available = TRUE LIMIT 1;")
#         driver = cursor.fetchone()

#         if driver:
#             # Atualizar status do motorista no banco
#             cursor.execute("UPDATE drivers SET available = FALSE WHERE id = %s;", (driver['id'],))
#             conn.commit()

#             # Registrar a corrida no banco
#             cursor.execute(
#                 "INSERT INTO rides (ride_id, driver_id, passenger_location, timestamp) VALUES (%s, %s, %s, %s);",
#                 (request['id'], driver['id'], json.dumps(request['location']), request['timestamp'])
#             )
#             conn.commit()

#             print(f"Requisição {request['id']} atribuída ao motorista {driver['id']}.")
#         else:
#             print(f"Sem motoristas disponíveis para a requisição {request['id']}.")

#         conn.close()

#     except Exception as e:
#         print(f"Erro ao processar requisição: {e}")

#     # Confirma que a mensagem foi processada
#     ch.basic_ack(delivery_tag=method.delivery_tag)

# def worker():
#     """Cria um consumidor RabbitMQ e processa mensagens."""
#     consumer = RabbitmqConsumer()
#     consumer.consume_messages(process_request)

# def main():
#     # Criar múltiplas threads para consumir mensagens simultaneamente
#     threads = []
#     for _ in range(6):  # Criar 6 threads
#         thread = threading.Thread(target=worker)
#         thread.start()
#         threads.append(thread)

#     for thread in threads:
#         thread.join()

# if __name__ == "__main__":
#     main()

#--------------------------------------------------------------------- aplica as 6 queues

# import pika
# import json
# import threading
# import psycopg2
# from psycopg2.extras import RealDictCursor

# class RabbitmqConsumer:
#     def __init__(self, queue_name: str) -> None:
#         self.__host = "192.168.1.2"
#         self.__port = 5672
#         self.__username = "user"
#         self.__password = "password"
#         self.__exchange = "data_exchange"
#         self.__queue = queue_name
#         self.__channel = self.__create_channel()

#     def __create_channel(self):
#         """Cria e retorna um canal de conexão com o RabbitMQ."""
#         connection_parameters = pika.ConnectionParameters(
#             host=self.__host,
#             port=self.__port,
#             credentials=pika.PlainCredentials(
#                 username=self.__username,
#                 password=self.__password
#             )
#         )
#         connection = pika.BlockingConnection(connection_parameters)
#         channel = connection.channel()
#         # channel.exchange_declare(exchange=self.__exchange, exchange_type='direct')
#         # channel.queue_declare(queue=self.__queue, durable=True)
#         channel.queue_bind(exchange=self.__exchange, queue=self.__queue, routing_key=self.__queue)
#         return channel

#     def consume_messages(self, callback):
#         """Consome mensagens da fila e as entrega ao callback."""
#         self.__channel.basic_qos(prefetch_count=1)
#         self.__channel.basic_consume(queue=self.__queue, on_message_callback=callback)
#         print(f"Consumidor aguardando mensagens na fila '{self.__queue}'...")
#         self.__channel.start_consuming()

# # Configuração do Banco de Dados
# DB_CONFIG = {
#     "dbname": "ride_app",
#     "user": "admin",
#     "password": "adminpass",
#     "host": "192.168.1.5",
#     "port": 5432
# }

# def process_request(ch, method, properties, body):
#     """Processa uma requisição de corrida."""
#     try:
#         request = json.loads(body)
#         print(f"Processando requisição da fila '{method.routing_key}': {request['id']}")

#         # Conectar ao banco de dados
#         conn = psycopg2.connect(**DB_CONFIG)
#         cursor = conn.cursor(cursor_factory=RealDictCursor)

#         # Verificar disponibilidade de motoristas
#         cursor.execute("SELECT id, latitude, longitude FROM drivers WHERE available = TRUE LIMIT 1;")
#         driver = cursor.fetchone()

#         if driver:
#             # Atualizar status do motorista no banco
#             cursor.execute("UPDATE drivers SET available = FALSE WHERE id = %s;", (driver['id'],))
#             conn.commit()

#             # Registrar a corrida no banco
#             cursor.execute(
#                 "INSERT INTO rides (ride_id, driver_id, passenger_location, timestamp) VALUES (%s, %s, %s, %s);",
#                 (request['id'], driver['id'], json.dumps(request['location']), request['timestamp'])
#             )
#             conn.commit()

#             print(f"Requisição {request['id']} atribuída ao motorista {driver['id']}.")
#         else:
#             print(f"Sem motoristas disponíveis para a requisição {request['id']}.")

#         conn.close()

#     except Exception as e:
#         print(f"Erro ao processar requisição: {e}")

#     # Confirma que a mensagem foi processada
#     ch.basic_ack(delivery_tag=method.delivery_tag)

# def worker(queue_name: str):
#     """Cria um consumidor RabbitMQ para processar mensagens de uma fila específica."""
#     consumer = RabbitmqConsumer(queue_name)
#     consumer.consume_messages(process_request)

# def main():
#     # Filas correspondentes às routing keys
#     queues = ['queue1', 'queue2', 'queue3', 'queue4', 'queue5', 'queue6']

#     threads = []
#     for queue in queues:
#         thread = threading.Thread(target=worker, args=(queue,))
#         thread.start()
#         threads.append(thread)

#     for thread in threads:
#         thread.join()

# if __name__ == "__main__":
#     main()

#-------------------------------------------------------------------------- lock ao acesar o banco 
# import pika
# import json
# import threading
# import psycopg2
# from psycopg2.extras import RealDictCursor

# # Lock para garantir acesso exclusivo ao banco de dados
# db_lock = threading.Lock()

# class RabbitmqConsumer:
#     def __init__(self, queue_name: str) -> None:
#         self.__host = "192.168.1.2"
#         self.__port = 5672
#         self.__username = "user"
#         self.__password = "password"
#         self.__exchange = "data_exchange"
#         self.__queue = queue_name
#         self.__channel = self.__create_channel()

#     def __create_channel(self):
#         """Cria e retorna um canal de conexão com o RabbitMQ."""
#         connection_parameters = pika.ConnectionParameters(
#             host=self.__host,
#             port=self.__port,
#             credentials=pika.PlainCredentials(
#                 username=self.__username,
#                 password=self.__password
#             )
#         )
#         connection = pika.BlockingConnection(connection_parameters)
#         channel = connection.channel()
#         # channel.exchange_declare(exchange=self.__exchange, exchange_type='direct')
#         # channel.queue_declare(queue=self.__queue, durable=True)
#         channel.queue_bind(exchange=self.__exchange, queue=self.__queue, routing_key=self.__queue)
#         return channel

#     def consume_messages(self, callback):
#         """Consome mensagens da fila e as entrega ao callback."""
#         self.__channel.basic_qos(prefetch_count=1)
#         self.__channel.basic_consume(queue=self.__queue, on_message_callback=callback)
#         print(f"Consumidor aguardando mensagens na fila '{self.__queue}'...")
#         self.__channel.start_consuming()

# # Configuração do Banco de Dados
# DB_CONFIG = {
#     "dbname": "ride_app",
#     "user": "admin",
#     "password": "adminpass",
#     "host": "192.168.1.5",
#     "port": 5432
# }

# def process_request(ch, method, properties, body):
#     """Processa uma requisição de corrida."""
#     with db_lock:  # Garante acesso exclusivo ao banco de dados
#         try:
#             request = json.loads(body)
#             print(f"Processando requisição da fila '{method.routing_key}': {request['id']}")

#             # Conectar ao banco de dados
#             conn = psycopg2.connect(**DB_CONFIG)
#             cursor = conn.cursor(cursor_factory=RealDictCursor)

#             # Verificar disponibilidade de motoristas
#             cursor.execute("SELECT id, latitude, longitude FROM drivers WHERE available = TRUE LIMIT 1;")
#             driver = cursor.fetchone()

#             if driver:
#                 # Atualizar status do motorista no banco
#                 cursor.execute("UPDATE drivers SET available = FALSE WHERE id = %s;", (driver['id'],))
#                 conn.commit()

#                 # Registrar a corrida no banco
#                 cursor.execute(
#                     "INSERT INTO rides (ride_id, driver_id, passenger_location, timestamp) VALUES (%s, %s, %s, %s);",
#                     (request['id'], driver['id'], json.dumps(request['location']), request['timestamp'])
#                 )
#                 conn.commit()

#                 print(f"Requisição {request['id']} atribuída ao motorista {driver['id']}.")
#             else:
#                 print(f"Sem motoristas disponíveis para a requisição {request['id']}.")

#             conn.close()

#         except Exception as e:
#             print(f"Erro ao processar requisição: {e}")

#         # Confirma que a mensagem foi processada
#         ch.basic_ack(delivery_tag=method.delivery_tag)

# def worker(queue_name: str):
#     """Cria um consumidor RabbitMQ para processar mensagens de uma fila específica."""
#     consumer = RabbitmqConsumer(queue_name)
#     consumer.consume_messages(process_request)

# def main():
#     # Filas correspondentes às routing keys
#     queues = ['queue1', 'queue2', 'queue3', 'queue4', 'queue5', 'queue6']

#     threads = []
#     for queue in queues:
#         thread = threading.Thread(target=worker, args=(queue,))
#         thread.start()
#         threads.append(thread)

#     for thread in threads:
#         thread.join()

# if __name__ == "__main__":
#     main()

#------------------------------------------------------------------------só consome se tiver carro livre
# import pika
# import json
# import threading
# import time
# import psycopg2
# from psycopg2.extras import RealDictCursor

# # Lock para garantir acesso exclusivo ao banco de dados
# db_lock = threading.Lock()

# class RabbitmqConsumer:
#     def __init__(self, queue_name: str) -> None:
#         self.__host = "192.168.1.2"
#         self.__port = 5672
#         self.__username = "user"
#         self.__password = "password"
#         self.__exchange = "data_exchange"
#         self.__queue = queue_name
#         self.__channel = self.__create_channel()

#     def __create_channel(self):
#         """Cria e retorna um canal de conexão com o RabbitMQ."""
#         connection_parameters = pika.ConnectionParameters(
#             host=self.__host,
#             port=self.__port,
#             credentials=pika.PlainCredentials(
#                 username=self.__username,
#                 password=self.__password
#             )
#         )
#         connection = pika.BlockingConnection(connection_parameters)
#         channel = connection.channel()
#         # channel.exchange_declare(exchange=self.__exchange, exchange_type='direct')
#         # channel.queue_declare(queue=self.__queue, durable=True)
#         channel.queue_bind(exchange=self.__exchange, queue=self.__queue, routing_key=self.__queue)
#         return channel

#     def consume_messages(self, callback):
#         """Consome mensagens da fila e as entrega ao callback."""
#         self.__channel.basic_qos(prefetch_count=1)
#         self.__channel.basic_consume(queue=self.__queue, on_message_callback=callback)
#         print(f"Consumidor aguardando mensagens na fila '{self.__queue}'...")
#         self.__channel.start_consuming()

# # Configuração do Banco de Dados
# DB_CONFIG = {
#     "dbname": "ride_app",
#     "user": "admin",
#     "password": "adminpass",
#     "host": "192.168.1.5",
#     "port": 5432
# }

# def get_available_driver():
#     """Verifica se há motoristas disponíveis e retorna um, ou None se nenhum estiver disponível."""
#     with db_lock:  # Garante acesso exclusivo ao banco de dados
#         try:
#             conn = psycopg2.connect(**DB_CONFIG)
#             cursor = conn.cursor(cursor_factory=RealDictCursor)

#             # Verificar disponibilidade de motoristas
#             cursor.execute("SELECT id, latitude, longitude FROM drivers WHERE available = TRUE LIMIT 1;")
#             driver = cursor.fetchone()

#             if driver:
#                 # Atualizar status do motorista no banco
#                 cursor.execute("UPDATE drivers SET available = FALSE WHERE id = %s;", (driver['id'],))
#                 conn.commit()

#             conn.close()
#             return driver
#         except Exception as e:
#             print(f"Erro ao acessar o banco de dados: {e}")
#             return None

# def process_request(ch, method, properties, body):
#     """Processa uma requisição de corrida."""
#     request = json.loads(body)
#     print(f"Requisição recebida da fila '{method.routing_key}': {request['id']}")

#     driver = None

#     # Tentar encontrar um motorista disponível
#     while driver is None:
#         print(f"Verificando disponibilidade de motoristas para a requisição {request['id']}...")
#         driver = get_available_driver()
#         if driver is None:
#             print(f"Sem motoristas disponíveis para a requisição {request['id']}. Retentando em 2 segundos...")
#             time.sleep(2)  # Aguarda antes de tentar novamente

#     # Registrar a corrida no banco
#     with db_lock:  # Garantir exclusividade ao registrar a corrida
#         try:
#             conn = psycopg2.connect(**DB_CONFIG)
#             cursor = conn.cursor(cursor_factory=RealDictCursor)

#             cursor.execute(
#                 "INSERT INTO rides (ride_id, driver_id, passenger_location, timestamp) VALUES (%s, %s, %s, %s);",
#                 (request['id'], driver['id'], json.dumps(request['location']), request['timestamp'])
#             )
#             conn.commit()

#             conn.close()

#             print(f"Requisição {request['id']} atribuída ao motorista {driver['id']}.")
#         except Exception as e:
#             print(f"Erro ao registrar a corrida: {e}")

#     # Confirma que a mensagem foi processada
#     ch.basic_ack(delivery_tag=method.delivery_tag)

# def worker(queue_name: str):
#     """Cria um consumidor RabbitMQ para processar mensagens de uma fila específica."""
#     consumer = RabbitmqConsumer(queue_name)
#     consumer.consume_messages(process_request)

# def main():
#     # Filas correspondentes às routing keys
#     queues = ['queue1', 'queue2', 'queue3', 'queue4', 'queue5', 'queue6']

#     threads = []
#     for queue in queues:
#         thread = threading.Thread(target=worker, args=(queue,))
#         thread.start()
#         threads.append(thread)

#     for thread in threads:
#         thread.join()

# if __name__ == "__main__":
#     main()


# Pq q zerou tudo?
# Pq ele consome a mensagfem mesmo sem processar ela

#------------------------------------------------------ Garantir prioridades pra previnir starvation

import pika
import json
import threading
import time
import psycopg2
from psycopg2.extras import RealDictCursor

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

# Dicionário para rastrear tentativas de cada thread
thread_attempts = {}

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

def process_request(ch, method, properties, body, thread_name):
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

            cursor.execute(
                "INSERT INTO rides (ride_id, driver_id, passenger_location, timestamp) VALUES (%s, %s, %s, %s);",
                (request['id'], driver['id'], json.dumps(request['location']), request['timestamp'])
            )
            conn.commit()

            conn.close()

            print(f"Thread {thread_name}: Requisição {request['id']} atribuída ao motorista {driver['id']}.")
        except Exception as e:
            print(f"Thread {thread_name}: Erro ao registrar a corrida: {e}")

    # Confirma que a mensagem foi processada
    ch.basic_ack(delivery_tag=method.delivery_tag)

def worker(queue_name: str, thread_name: str):
    """Cria um consumidor RabbitMQ para processar mensagens de uma fila específica."""
    consumer = RabbitmqConsumer(queue_name)

    def callback(ch, method, properties, body):
        process_request(ch, method, properties, body, thread_name)

    consumer.consume_messages(callback)

def main():
    # Filas correspondentes às routing keys
    queues = ['queue1', 'queue2', 'queue3', 'queue4', 'queue5', 'queue6']

    threads = []

    # Inicializa o contador de tentativas para cada thread
    for i, queue in enumerate(queues):
        thread_name = f"Thread-{i+1}"
        thread_attempts[thread_name] = 0

        thread = threading.Thread(target=worker, args=(queue, thread_name))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

class RabbitmqConsumer:
    def __init__(self, queue_name: str) -> None:
        self.__host = "192.168.1.2"
        self.__port = 5672
        self.__username = "user"
        self.__password = "password"
        self.__exchange = "data_exchange"
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
    main()
