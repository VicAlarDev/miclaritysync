import logging
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

from models.existencia_origen import ExistenciaOrigen
from models.existencia_sede import ExistenciaSede
from models.producto_destino import Producto
from models.producto_origen import ProductoOrigen

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
    ROUND(precio_original, 2) AS precio_original, 
    ROUND(precio_final, 2) AS precio_final, 
    ROUND((precio_original / tasa_cambio), 2) AS precio_divisas_original, -- Nuevo cálculo
    ROUND((precio_final / tasa_cambio), 2) AS precio_divisas_final, -- Nuevo cálculo
    poriva, 
    ROUND(preciomasiva, 2) AS preciomasiva, 
    ROUND((preciomasiva - precio_final), 2) AS montoiva, 
    tasa_cambio, 
    stock,  
    barras, 
    pactivo, 
    codlin, 
    lineas,
    CASE 
        WHEN desc_oferta > 0 AND CURDATE() > inicio AND CURDATE() < final THEN 'Sí'
        WHEN descuento > 0 THEN 'Sí'
        ELSE 'No'
    END AS tiene_descuento,
    CASE 
        WHEN desc_oferta > 0 AND CURDATE() > inicio AND CURDATE() < final THEN ROUND(desc_oferta, 2)
        ELSE NULL
    END AS precio_oferta,
    CASE 
        WHEN desc_oferta > 0 AND CURDATE() > inicio AND CURDATE() < final THEN 
            ROUND(((precio_original - desc_oferta) / precio_original) * 100, 2)
        WHEN descuento > 0 THEN descuento
        ELSE NULL
    END AS descuento_porcentual
FROM (
    SELECT 
        codprod, 
        nombre, 
        precio_original, 
        CASE 
            WHEN desc_oferta > 0 AND CURDATE() > inicio AND CURDATE() < final THEN desc_oferta
            WHEN descuento > 0 THEN precio_original * (1 - (descuento / 100))
            ELSE precio_original
        END AS precio_final,
        poriva, 
        (precio_original * (1 + (poriva / 100))) AS preciomasiva, 
        tasa_cambio, 
        stock,  
        barras, 
        pactivo, 
        codlin, 
        lineas,
        descuento,
        desc_oferta,
        inicio,
        final
    FROM (
        SELECT 
            codprod, 
            nombre, 
            precio AS precio_original, 
            IF(encarte > 0 AND CURDATE() > inicio AND CURDATE() < final, desc_oferta, NULL) AS desc_oferta,
            poriva, 
            descuento, 
            inicio,
            final,
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
                FROM 
                    productos p 
                WHERE 
                    p.stock > 0  
            ) w 
            LEFT JOIN lineas l ON w.codlin = l.keycodigo
        ) x
    ) y
) z;
        """
        result = session_fuente.execute(text(consulta)).fetchall()
        existencias = []
        for row in result:
            existencia = ExistenciaOrigen(*row)  # Mapear los valores de la tupla a la clase
            existencias.append(existencia)
        return existencias

    def obtener_todas_existencias_destino(self, session_destino):
        """Obtiene todas las existencias de la base de datos destino."""
        logging.info("Cargando todas las existencias de la base de datos destino...")
        query = select(ExistenciaSede)
        return session_destino.execute(query).scalars().all()

    def obtener_existencia_destino(self, session_destino, codprod_origen, codsede_origen):
        """Obtiene una existencia por producto y sede desde la base de datos destino."""
        return session_destino.query(ExistenciaSede).filter(ExistenciaSede.product_codprod == codprod_origen,
                                                            ExistenciaSede.codsede == codsede_origen).first()

    def actualizar_existencias_batch(self, session_destino, existencias_batch):
        """Actualiza un lote de existencias en la base de datos destino."""
        for existencia_destino, existencia_origen, hash_fuente in existencias_batch:
            existencia_destino.existencia = existencia_origen.stock
            existencia_destino.precio_final = existencia_origen.precio_final
            existencia_destino.precio_original = existencia_origen.precio_original
            existencia_destino.precio_divisa_original = existencia_origen.precio_divisas_original
            existencia_destino.precio_divisa_final = existencia_origen.precio_divisas_final
            existencia_destino.tasa_cambio = existencia_origen.tasa_cambio
            existencia_destino.descuento = existencia_origen.descuento_porcentual
            existencia_destino.hash = hash_fuente
        session_destino.commit()

    def insertar_existencias_batch(self, session_destino, existencias_batch):
        """Inserta un lote de nuevas existencias en la base de datos destino."""
        nuevas_existencias = []
        for existencia_origen, hash_fuente in existencias_batch:
            nueva_existencia = ExistenciaSede(
                product_codprod=existencia_origen.codprod,
                codsede=2,
                existencia=existencia_origen.stock,
                precio_original=existencia_origen.precio_original,
                precio_final=existencia_origen.precio_final,
                precio_divisa_original=existencia_origen.precio_divisas_original,
                precio_divisa_final=existencia_origen.precio_divisas_final,
                tasa_cambio=existencia_origen.tasa_cambio,
                descuento=existencia_origen.descuento_porcentual,
                tiene_descuento=existencia_origen.descuento_porcentual > 0,
                hash=hash_fuente
            )
            nuevas_existencias.append(nueva_existencia)
        session_destino.bulk_save_objects(nuevas_existencias)
        session_destino.commit()