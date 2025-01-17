class ExistenciaOrigen:
    def __init__(self, *args):
        (
            self.codprod,
            self.nombre,
            self.precio_original,
            self.precio_final,
            self.precio_divisas_original,
            self.precio_divisas_final,
            self.poriva,
            self.preciomasiva,
            self.montoiva,
            self.tasa_cambio,
            self.stock,
            self.barras,
            self.pactivo,
            self.codlin,
            self.lineas,
            self.tiene_descuento,
            self.precio_oferta,
            self.descuento_porcentual,
            # Añade más atributos si es necesario
        ) = args
