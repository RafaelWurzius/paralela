import pika

def create_exchange_and_queues():
    # Definir o usuário e senha
    username = 'user'  # Substitua por seu nome de usuário
    password = 'password'  # Substitua por sua senha

    # Criar as credenciais com o nome de usuário e senha
    credentials = pika.PlainCredentials(username, password)

    # Conectar ao RabbitMQ (localhost por padrão) com as credenciais
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
        # Declarar a queue
        channel.queue_declare(queue=queue)

        # Vincular a queue ao exchange com a routing key correspondente
        channel.queue_bind(exchange=exchange_name, queue=queue, routing_key=routing_key)

    print(f"Exchange '{exchange_name}' e as filas foram configuradas com sucesso!")

    # Fechar a conexão
    connection.close()


def send_messages():
    # Definir o usuário e senha
    username = 'user'  # Substitua por seu nome de usuário
    password = 'password'  # Substitua por sua senha

    # Criar as credenciais com o nome de usuário e senha
    credentials = pika.PlainCredentials(username, password)

    # Conectar ao RabbitMQ (localhost por padrão) com as credenciais
    connection = pika.BlockingConnection(
        pika.ConnectionParameters('192.168.1.2', 5672, '/', credentials)
    )
    channel = connection.channel()

    # Nome do exchange
    exchange_name = 'data_exchange'

    # Enviar 10 mensagens para o exchange
    for i in range(10):
        message = f"Mensagem {i+1}"
        channel.basic_publish(exchange=exchange_name,
                              routing_key='',  # Não especificar uma chave de roteamento
                              body=message)
        print(f"Enviada: {message}")

if __name__ == '__main__':
    create_exchange_and_queues()
    # send_messages()
