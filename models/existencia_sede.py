from sqlalchemy import create_engine, Column, Integer, String, DECIMAL, BOOLEAN, FLOAT
from sqlalchemy.ext.declarative import declarative_base

# Declarative base para definir las clases ORM
Base = declarative_base()

class ExistenciaSede(Base):
    __tablename__ = 'existencias_sede'

    # Campos clave primaria
    product_codprod = Column(Integer, primary_key=True)
    codsede = Column(Integer, primary_key=True)

    # Otros campos
    existencia = Column(DECIMAL(10, 3), nullable=False)
    precio_final = Column(DECIMAL(10, 2), nullable=False)
    precio_original = Column(DECIMAL(10, 2), nullable=False)
    precio_divisa_original = Column(DECIMAL(10, 2), nullable=True)
    precio_divisa_final = Column(DECIMAL(10, 2), nullable=True)
    nombre_divisa = Column(String(20), nullable=True)
    tasa_cambio = Column(DECIMAL(10, 3), nullable=False)
    descuento = Column(FLOAT(10, 2), nullable=True)
    tiene_descuento = Column(BOOLEAN, default=False)
    hash = Column(String(64), nullable=False)

    def __repr__(self):
        return (
            f"<ExistenciaSede(product_codprod={self.product_codprod}, "
            f"codsede={self.codsede}, existencia={self.existencia}, "
            f"precio_bs={self.precio_final}, precio_divisa_final={self.precio_divisa_final}, "
            f"tiene_descuento={self.tiene_descuento})>"
        )
