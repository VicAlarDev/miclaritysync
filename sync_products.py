import logging
from tqdm import tqdm
from config import FUENTE_URL, DESTINO_URL
from sync_manager import SyncManager
from producto_origen import ProductoOrigen
import hashlib

def calcular_hash(producto_origen):
    """Genera un hash de los campos relevantes del producto."""
    campos = f"{producto_origen.nombre}{producto_origen.precio}{producto_origen.stock}{producto_origen.pactivo}{producto_origen.codmarca}"
    return hashlib.sha256(campos.encode('utf-8')).hexdigest()

def sincronizar_productos():
    try:
        logging.info("Iniciando la sincronización de productos...")

        # Crear instancia de SyncManager
        sync_manager = SyncManager(FUENTE_URL, DESTINO_URL)

        # Iniciar sesiones
        session_fuente = sync_manager.iniciar_sesion_fuente()
        session_destino = sync_manager.iniciar_sesion_destino()

        # Obtener productos de la fuente
        productos_origen = sync_manager.obtener_productos_origen(session_fuente)

        if not productos_origen:
            logging.warning("No se encontraron productos en la base de datos fuente.")
            return

        logging.info(f"Se encontraron {len(productos_origen)} productos en la base de datos fuente.")

        # Variables para el resumen
        productos_actualizados = 0
        productos_no_cambiaron = 0
        productos_nuevos = 0
        productos_cambiados = []

        # Barra de progreso
        with tqdm(total=len(productos_origen), desc="Procesando productos", unit="producto") as pbar:
            for producto_origen in productos_origen:
                codprod_origen = producto_origen.codprod
                hash_fuente = calcular_hash(producto_origen)

                # Obtener el producto en destino
                producto_destino = sync_manager.obtener_producto_destino(session_destino, codprod_origen)

                if producto_destino:
                    # Si el producto existe en destino, lo actualizamos si el hash es diferente
                    if producto_destino.hash != hash_fuente:
                        sync_manager.actualizar_producto(session_destino, producto_destino, producto_origen, hash_fuente)
                        productos_actualizados += 1
                        productos_cambiados.append(codprod_origen)
                    else:
                        productos_no_cambiaron += 1
                else:
                    # Si no existe, lo insertamos
                    sync_manager.insertar_nuevo_producto(session_destino, producto_origen, hash_fuente)
                    productos_nuevos += 1

                pbar.update(1)

        # Resumen final
        logging.info(f"Sincronización completada con éxito. Productos actualizados: {productos_actualizados} - Productos no cambiaron: {productos_no_cambiaron} - Nuevos productos insertados: {productos_nuevos}")

        if productos_cambiados:
            logging.info(f"Productos que cambiaron: {', '.join(map(str, productos_cambiados))}")

    except Exception as e:
        logging.error(f"Error durante la sincronización: {e}")
    finally:
        session_fuente.close()
        session_destino.close()

if __name__ == "__main__":
    sincronizar_productos()
