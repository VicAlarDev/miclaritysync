import logging
from tqdm import tqdm
from config import FUENTE_URL, DESTINO_URL
from sync_manager import SyncManager
import hashlib

def calcular_hash(existencia_origen):
    """Genera un hash de los campos relevantes de la existencia."""
    campos = f"{existencia_origen.codprod}{existencia_origen.codlin}{existencia_origen.stock}{existencia_origen.precio}{existencia_origen.precio_divisas}{existencia_origen.tasa_cambio}{existencia_origen.descuento}"
    return hashlib.sha256(campos.encode('utf-8')).hexdigest()

def sincronizar_existencias():
    try:
        logging.info("Iniciando la sincronización de existencias por sede...")

        # Crear instancia de SyncManager
        sync_manager = SyncManager(FUENTE_URL, DESTINO_URL)

        # Iniciar sesiones
        session_fuente = sync_manager.iniciar_sesion_fuente()
        session_destino = sync_manager.iniciar_sesion_destino()

        # Obtener existencias desde la fuente
        existencias_origen = sync_manager.obtener_existencias_origen(session_fuente)

        if not existencias_origen:
            logging.warning("No se encontraron existencias en la base de datos fuente.")
            return

        logging.info(f"Se encontraron {len(existencias_origen)} existencias en la base de datos fuente.")

        # Variables para el resumen
        existencias_actualizadas = 0
        existencias_no_cambiaron = 0
        existencias_nuevas = 0
        existencias_cambiadas = []

        # Barra de progreso
        with tqdm(total=len(existencias_origen), desc="Procesando existencias", unit="existencia") as pbar:
            for existencia_origen in existencias_origen:
                codprod_origen = existencia_origen[0]
                codsede_origen = 2
                hash_fuente = calcular_hash(existencia_origen)

                # Obtener existencia en destino
                existencia_destino = sync_manager.obtener_existencia_destino(session_destino, codprod_origen, codsede_origen)

                if existencia_destino:
                    # Si la existencia ya está en destino, actualizar si el hash es diferente
                    if existencia_destino.hash != hash_fuente:
                        sync_manager.actualizar_existencia(session_destino, existencia_destino, existencia_origen, hash_fuente)
                        existencias_actualizadas += 1
                        existencias_cambiadas.append(f"{codprod_origen}-{codsede_origen}")
                    else:
                        existencias_no_cambiaron += 1
                else:
                    # Si no existe, insertamos la nueva existencia
                    sync_manager.insertar_nueva_existencia(session_destino, existencia_origen, hash_fuente)
                    existencias_nuevas += 1

                pbar.update(1)

        # Resumen
        logging.info(f"Sincronización completada. Existencias actualizadas: {existencias_actualizadas}, Existencias no cambiaron: {existencias_no_cambiaron}, Existencias nuevas: {existencias_nuevas}")
        if existencias_cambiadas:
            logging.info(f"Existencias cambiadas: {', '.join(existencias_cambiadas)}")

    except Exception as e:
        logging.error(f"Error durante la sincronización: {str(e)}")

if __name__ == "__main__":
    sincronizar_existencias()
