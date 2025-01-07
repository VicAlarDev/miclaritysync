import logging
from tqdm import tqdm
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

from existencia_sede import ExistenciaSede
from producto_destino import Producto
from producto_origen import ProductoOrigen

class SyncManager:
    def __init__(self, fuente_url, destino_url):
        self.fuente_url = fuente_url
        self.destino_url = destino_url
        self.engine_fuente = create_engine(self.fuente_url)
        self.engine_destino = create_engine(self.destino_url)
        self.Session_fuente = sessionmaker(bind=self.engine_fuente)
        self.Session_destino = sessionmaker(bind=self.engine_destino)

    def iniciar_sesion_fuente(self):
        return self.Session_fuente()

    def iniciar_sesion_destino(self):
        return self.Session_destino()

    def obtener_productos_origen(self, session_fuente):
        """Obtiene los productos de la base de datos fuente"""
        query_fuente = select(ProductoOrigen)
        return session_fuente.execute(query_fuente).scalars().all()

    def obtener_producto_destino(self, session_destino, codprod_origen):
        """Obtiene un producto de la base de datos destino"""
        return session_destino.query(Producto).filter(Producto.codprod == codprod_origen).first()

    def actualizar_producto(self, session_destino, producto_destino, producto_origen, hash_fuente):
        """Actualiza un producto en la base de datos destino"""
        logging.info(f"El producto {producto_origen.codprod} ha cambiado. Actualizando...")
        producto_destino.nombre = producto_origen.nombre
        producto_destino.precio = producto_origen.precio
        producto_destino.stock = producto_origen.stock
        producto_destino.pactivo = producto_origen.pactivo
        producto_destino.codmarca = 1  # Asumimos un valor estático
        producto_destino.hash = hash_fuente
        session_destino.commit()

    def insertar_nuevo_producto(self, session_destino, producto_origen, hash_fuente):
        """Inserta un nuevo producto en la base de datos destino"""
        logging.info(f"Producto {producto_origen.codprod} no encontrado en destino. Insertando nuevo producto...")
        nuevo_producto = Producto(
            codprod=producto_origen.codprod,
            nombre=producto_origen.nombre,
            precio=producto_origen.precio,
            stock=producto_origen.stock,
            pactivo=producto_origen.pactivo,
            codmarca=1,
            created_at="2024-11-18 13:59:37",
            updated_at="2024-11-18 13:59:37",
            codbarra01=producto_origen.CODBARRA01,
            hash=hash_fuente
        )
        session_destino.add(nuevo_producto)
        session_destino.commit()

    def obtener_existencias_origen(self, session_fuente):
        """Obtiene las existencias desde la base de datos fuente usando la consulta proporcionada"""
        consulta = """
        SELECT 
            codprod, 
            nombre, 
            ROUND(precio, 2) AS precio, 
            poriva, 
            ROUND(preciomasiva, 2) AS preciomasiva, 
            ROUND((preciomasiva - precio), 2) AS montoiva, 
            tasa_cambio, 
            ROUND((preciomasiva / tasa_cambio), 2) AS precio_divisas, 
            stock, 
            barras, 
            pactivo, 
            codlin, 
            lineas, 
            descuento
        FROM (
            SELECT 
                codprod, 
                nombre, 
                precio, 
                poriva, 
                (precio * (1 + (poriva / 100))) AS preciomasiva, 
                tasa_cambio, 
                stock,  
                barras, 
                pactivo, 
                codlin, 
                lineas, 
                descuento
            FROM (
                SELECT 
                    codprod, 
                    nombre, 
                    IF(encarte > 0 AND CURDATE() > inicio AND CURDATE() < final, desc_oferta, 
                    (precio * ((100 - descuento) / 100))) AS precio,
                    poriva,
                    descuento,
                    tasa_cambio,
                    stock,  
                    barras,
                    pactivo, 
                    codlin, 
                    lineas
                FROM (
                    SELECT 
                        w.*, 
                        l.descuento, 
                        (SELECT tasa_cambio FROM monedas WHERE esrefprecio LIMIT 1) AS tasa_cambio, 
                        CASE 
                            WHEN w.tipoiva = 'NORMAL' THEN (SELECT iva FROM areas LIMIT 1)    
                            WHEN w.tipoiva = 'REDUCIDO' THEN (SELECT ivareducido FROM areas LIMIT 1)    
                            WHEN w.tipoiva = 'TASA3' THEN (SELECT tasa3 FROM areas LIMIT 1)    
                            ELSE 0
                        END AS poriva
                    FROM (
                        SELECT 
                            p.keycodigo, 
                            p.nombre, 
                            p.precio, 
                            p.codprod, 
                            p.tipoiva, 
                            encarte, 
                            inicio, 
                            final, 
                            desc_oferta, 
                            codlin, 
                            p.stock, 
                            codbarra01 AS barras, 
                            pactivo, 
                            lineas 
                        FROM productos p 
                        WHERE p.stock > 0
                    ) w
                    LEFT JOIN lineas l ON w.codlin = l.keycodigo
                ) x
            ) y
        ) z;
        """
        result = session_fuente.execute(text(consulta))
        return result.fetchall()

    def obtener_existencia_destino(self, session_destino, codprod_origen, codsede_origen):
        """Obtiene una existencia por producto y sede desde la base de datos destino."""
        return session_destino.query(ExistenciaSede).filter(ExistenciaSede.product_codprod == codprod_origen,
                                                            ExistenciaSede.codsede == codsede_origen).first()

    def actualizar_existencia(self, session_destino, existencia_destino, existencia_origen, hash_fuente):
        """Actualiza una existencia en la base de datos destino."""
        logging.info(
            f"Existencia para el producto {existencia_origen.codprod} en sede 1 ha cambiado. Actualizando...")
        existencia_destino.existencia = existencia_origen.stock
        existencia_destino.precio_bs = existencia_origen.precio
        existencia_destino.precio_divisa = existencia_origen.precio_divisas
        existencia_destino.tasa_cambio = existencia_origen.tasa_cambio
        existencia_destino.descuento = existencia_origen.descuento
        existencia_destino.hash = hash_fuente
        session_destino.commit()

    def insertar_nueva_existencia(self, session_destino, existencia_origen, hash_fuente):
        """Inserta una nueva existencia en la base de datos destino."""
        logging.info(
            f"Existencia para el producto {existencia_origen.codprod} en sede 1 no encontrada en destino. Insertando nueva existencia...")
        nueva_existencia = ExistenciaSede(
            product_codprod=existencia_origen.codprod,
            codsede=2,
            existencia=existencia_origen.stock,
            precio_bs=existencia_origen.precio,
            precio_divisa=existencia_origen.precio_divisas,
            tasa_cambio=existencia_origen.tasa_cambio,
            descuento=existencia_origen.descuento,
            hash=hash_fuente
        )
        session_destino.add(nueva_existencia)
        session_destino.commit()