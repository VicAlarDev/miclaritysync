from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, DECIMAL, FLOAT
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

# Declarative base para definir las clases ORM
Base = declarative_base()

class ExistenciaSede(Base):
    __tablename__ = 'existencias_sede'

    # Campos clave primaria
    product_codprod = Column(Integer, primary_key=True)
    codsede = Column(Integer, primary_key=True)

    # Otros campos
    existencia = Column(DECIMAL(10, 3))
    precio_bs = Column(DECIMAL(10, 3))
    precio_divisa = Column(DECIMAL(10, 3))
    nombre_divisa = Column(String(20))
    tasa_cambio = Column(DECIMAL(10, 3))
    descuento = Column(FLOAT(10, 2))
    hash = Column(String(64))

    def __repr__(self):
        return f"<ExistenciaSede(product_codprod={self.product_codprod}, codsede={self.codsede}, existencia={self.existencia}, precio_bs={self.precio_bs})>"