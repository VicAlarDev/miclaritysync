import logging

from tqdm import tqdm
from multiprocessing import Pool, cpu_count
from config import FUENTE_URL, DESTINO_URL, configurar_logger
from sync_manager import SyncManager
import hashlib
from datetime import datetime
import time

def calcular_hash(existencia_origen):
    """Genera un hash de los campos relevantes de la existencia."""
    campos = (
        f"{existencia_origen.codprod}{existencia_origen.codlin}{existencia_origen.stock}"
        f"{existencia_origen.precio_final}{existencia_origen.precio_divisas_final}"
        f"{existencia_origen.tasa_cambio}{existencia_origen.descuento_porcentual}"
    )
    return hashlib.sha256(campos.encode('utf-8')).hexdigest()

def procesar_chunk(datos_chunk):
    """Función para procesar un chunk de existencias."""
    sync_manager = SyncManager(FUENTE_URL, DESTINO_URL)
    session_destino = sync_manager.iniciar_sesion_destino()

    batched_existencias_actualizadas = []
    batched_existencias_nuevas = []

    for existencia_origen in datos_chunk:
        codprod_origen = existencia_origen.codprod
        codsede_origen = 2
        hash_fuente = calcular_hash(existencia_origen)

        existencia_destino = sync_manager.obtener_existencia_destino(session_destino, codprod_origen, codsede_origen)

        if existencia_destino:
            if existencia_destino.hash != hash_fuente:
                batched_existencias_actualizadas.append((existencia_destino, existencia_origen, hash_fuente))
        else:
            batched_existencias_nuevas.append((existencia_origen, hash_fuente))

    if batched_existencias_actualizadas:
        sync_manager.actualizar_existencias_batch(session_destino, batched_existencias_actualizadas)

    if batched_existencias_nuevas:
        sync_manager.insertar_existencias_batch(session_destino, batched_existencias_nuevas)

    session_destino.close()
    return len(batched_existencias_actualizadas), len(batched_existencias_nuevas)

def sincronizar_existencias():
    inicio = time.time()
    archivo_log = configurar_logger()

    try:
        logging.info("Iniciando la sincronización de existencias por sede...")

        sync_manager = SyncManager(FUENTE_URL, DESTINO_URL)
        session_fuente = sync_manager.iniciar_sesion_fuente()
        existencias_origen = sync_manager.obtener_existencias_origen(session_fuente)
        session_fuente.close()

        if not existencias_origen:
            logging.warning("No se encontraron existencias en la base de datos fuente.")
            return

        logging.info(f"Se encontraron {len(existencias_origen)} existencias en la base de datos fuente.")

        # Dividir existencias en chunks para multiprocesamiento
        num_chunks = cpu_count()
        chunk_size = len(existencias_origen) // num_chunks
        chunks = [existencias_origen[i:i + chunk_size] for i in range(0, len(existencias_origen), chunk_size)]

        # Procesar en paralelo
        with Pool(processes=num_chunks) as pool:
            resultados = list(tqdm(pool.imap(procesar_chunk, chunks), total=len(chunks), desc="Procesando en paralelo", unit="chunk"))

        # Resumir resultados
        total_actualizadas = sum(r[0] for r in resultados)
        total_nuevas = sum(r[1] for r in resultados)

        fin = time.time()
        duracion = fin - inicio

        logging.info("Sincronización completada.")
        logging.info(f"  Existencias actualizadas: {total_actualizadas}")
        logging.info(f"  Existencias nuevas: {total_nuevas}")
        logging.info(f"  Hora de inicio: {datetime.fromtimestamp(inicio).strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"  Hora de finalización: {datetime.fromtimestamp(fin).strftime('%Y-%m-%d %H:%M:%S')}")
        logging.info(f"  Duración del proceso: {duracion:.2f} segundos")

    except Exception as e:
        logging.error(f"Error durante la sincronización: {str(e)}")

if __name__ == "__main__":
    sincronizar_existencias()
