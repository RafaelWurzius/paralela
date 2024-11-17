FROM rabbitmq:management

COPY ./rabbitmq_config.sh /etc/rabbitmq/

RUN chmod +x /etc/rabbitmq/rabbitmq_config.sh

# Executar o script de configuração quando o container iniciar
CMD ["sh", "-c", "/etc/rabbitmq/rabbitmq_config.sh && rabbitmq-server"]
