FROM rabbitmq:management

# Copiar scripts de configuração para o container
COPY ./rabbitmq_config.sh /etc/rabbitmq/

# Tornar o script executável
RUN chmod +x /etc/rabbitmq/rabbitmq_config.sh

# Executar o script de configuração quando o container iniciar
CMD ["sh", "-c", "/etc/rabbitmq/rabbitmq_config.sh && rabbitmq-server"]
