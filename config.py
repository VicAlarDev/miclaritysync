import logging

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

# URLs de las bases de datos
FUENTE_URL = "mysql+mysqlconnector://root:clave@127.0.0.1/database"
DESTINO_URL = "mysql+mysqlconnector://root:clave@127.0.0.1/database"
