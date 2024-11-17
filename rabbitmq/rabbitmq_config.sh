#!/bin/bash

# Esperar o RabbitMQ estar completamente iniciado antes de configurar
# sleep 10

# rabbitmqctl add_vhost vhost1

# Definir um exchange do tipo 'direct'
# rabbitmqctl add_exchange data_exchange direct
# rabbitmqctl declare exchange --vhost=vhost1 name=data_exchange type=direct
# rabbitmqadmin declare exchange --vhost=vhost1 name=data_exchange type=direct

# Criar filas
# rabbitmqctl add_queue data_queue1
# rabbitmqctl add_queue data_queue2

# Vínculo entre exchange e filas
# rabbitmqctl set_binding data_exchange data_queue1 a
# rabbitmqctl set_binding data_exchange data_queue2 b
