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

            # Esperar um intervalo aleatório entre 5 e 15 segundos antes de repetir
            time.sleep(random.randint(2, 6))

    except Exception as e:
        print(f"Erro ao atualizar disponibilidade dos motoristas: {e}")
        time.sleep(10)  # Aguarda antes de tentar novamente

if __name__ == "__main__":
    update_driver_availability()
