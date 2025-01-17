import logging
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
from config import FUENTE_URL, DESTINO_URL, configurar_logger
from sync_manager import SyncManager
import hashlib
from datetime import datetime
import time

def calcular_hash(producto_origen):
    """Genera un hash de los campos relevantes del producto."""
    campos = (
        f"{producto_origen.nombre}{producto_origen.precio}{producto_origen.stock}"
        f"{producto_origen.pactivo}{producto_origen.codmarca}"
    )
    return hashlib.sha256(campos.encode('utf-8')).hexdigest()

def procesar_chunk(productos_chunk):
    """Función para procesar un chunk de productos."""
    sync_manager = SyncManager(FUENTE_URL, DESTINO_URL)
    session_destino = sync_manager.iniciar_sesion_destino()

    batched_productos_actualizados = []
    batched_productos_nuevos = []

    for producto_origen in productos_chunk:
        codprod_origen = producto_origen.codprod
        hash_fuente = calcular_hash(producto_origen)

        producto_destino = sync_manager.obtener_producto_destino(session_destino, codprod_origen)

        if producto_destino:
            if producto_destino.hash != hash_fuente:
                batched_productos_actualizados.append((producto_destino, producto_origen, hash_fuente))
        else:
            batched_productos_nuevos.append((producto_origen, hash_fuente))

    if batched_productos_actualizados:
        sync_manager.actualizar_productos_batch(session_destino, batched_productos_actualizados)

    if batched_productos_nuevos:
        sync_manager.insertar_productos_batch(session_destino, batched_productos_nuevos)

    session_destino.close()
    return len(batched_productos_actualizados), len(batched_productos_nuevos)

def sincronizar_productos():
    inicio = time.time()
    configurar_logger()

    try:
        logging.info("Iniciando la sincronización de productos...")

        sync_manager = SyncManager(FUENTE_URL, DESTINO_URL)
        session_fuente = sync_manager.iniciar_sesion_fuente()
        productos_origen = sync_manager.obtener_productos_origen(session_fuente)
        session_fuente.close()

        if not productos_origen:
            logging.warning("No se encontraron productos en la base de datos fuente.")
            return

        logging.info(f"Se encontraron {len(productos_origen)} productos en la base de datos fuente.")

        # Dividir productos en chunks para multiprocesamiento
        num_chunks = cpu_count()
        chunk_size = len(productos_origen) // num_chunks
        chunks = [productos_origen[i:i + chunk_size] for i in range(0, len(productos_origen), chunk_size)]

        # Procesar en paralelo
        with Pool(processes=num_chunks) as pool:
            resultados = list(tqdm(pool.imap(procesar_chunk, chunks), total=len(chunks), desc="Procesando en paralelo", unit="chunk"))

        # Resumir resultados
        total_actualizados = sum(r[0] for r in resultados)
        total_nuevos = sum(r[1] for r in resultados)

        fin = time.time()
        duracion = fin - inicio

        logging.info("Sincronización completada.")
        logging.info(f"  Productos actualizados: {total_actualizados}")
        logging.info(f"  Productos nuevos: {total_nuevos}")
        logging.info(f"  Hora de inicio: {datetime.fromtimestamp(inicio).strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"  Hora de finalización: {datetime.fromtimestamp(fin).strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"  Duración del proceso: {duracion:.2f} segundos")

    except Exception as e:
        logging.error(f"Error durante la sincronización: {str(e)}")

if __name__ == "__main__":
    sincronizar_productos()
