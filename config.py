from datetime import datetime
import logging
import os
from dotenv import load_dotenv

# Cargar variables de entorno desde el archivo .env
load_dotenv()

FUENTE_URL = os.getenv("FUENTE_URL", "")
DESTINO_URL = os.getenv("DESTINO_URL", "")

def configurar_logger():
    """Configura el logger para escribir en un archivo con la fecha actual."""
    fecha_actual = datetime.now().strftime('%Y-%m-%d')
    archivo_log = f"sincronizacion_{fecha_actual}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(archivo_log),
            logging.StreamHandler()
        ]
    )
    return archivo_log
