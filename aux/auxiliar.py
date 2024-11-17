import pika
import psycopg2
import random
import time

# Configuração do Banco de Dados
DB_CONFIG = {
    "dbname": "ride_app",
    "user": "admin",
    "password": "adminpass",
    "host": "192.168.1.5",
    "port": 5432
}

def create_exchange_and_queues():
    """Cria o exchange e as filas no RabbitMQ."""
    try:
        # Definir o usuário e senha
        username = 'user'  
        password = 'password' 

        credentials = pika.PlainCredentials(username, password)

        # Conectar ao RabbitMQ com as credenciais
        connection = pika.BlockingConnection(
            pika.ConnectionParameters('192.168.1.2', 5672, '/', credentials)
        )
        channel = connection.channel()

        # Criar um exchange do tipo 'direct'
        exchange_name = 'data_exchange'
        channel.exchange_declare(exchange=exchange_name, exchange_type='direct')

        # Criar e vincular as filas (queues) com routing keys
        queues = ['queue1', 'queue2', 'queue3', 'queue4', 'queue5', 'queue6']
        routing_keys = ['a', 'b', 'c', 'd', 'e', 'f']

        for queue, routing_key in zip(queues, routing_keys):
            channel.queue_declare(queue=queue)

            # Vincular a queue ao exchange com a routing key correspondente
            channel.queue_bind(exchange=exchange_name, queue=queue, routing_key=routing_key)

        print(f"Exchange '{exchange_name}' e as filas foram configuradas com sucesso!")

        connection.close()
    
    except Exception as e:
        print(f"Erro ao configurar o RabbitMQ: {e}")


def update_driver_availability():
    """Atualiza a disponibilidade de motoristas periodicamente."""
    try:
        while True:
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()

            # Buscar todos os motoristas ocupados
            cursor.execute("SELECT id FROM drivers WHERE available = FALSE;")
            busy_drivers = cursor.fetchall()

            if busy_drivers:
                # Escolher aleatoriamente um motorista para tornar disponível
                driver_id = random.choice(busy_drivers)[0]
                cursor.execute("UPDATE drivers SET available = TRUE WHERE id = %s;", (driver_id,))
                conn.commit()

                print(f"Motorista {driver_id} agora está disponível.")

            conn.close()

            # Esperar um intervalo aleatório entre 2 e 6 segundos antes de repetir
            time.sleep(random.randint(2, 6))

    except Exception as e:
        print(f"Erro ao atualizar disponibilidade dos motoristas: {e}")
        time.sleep(10)  # Aguarda antes de tentar novamente


if __name__ == '__main__':
    time.sleep(15)
    # Cria o exchange e as filas no RabbitMQ
    create_exchange_and_queues()

    # Atualização periódica do banco de dados
    update_driver_availability()
