from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey, DECIMAL
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

# Declarative base para definir las clases ORM
Base = declarative_base()

class Producto(Base):
    __tablename__ = 'productos'

    codprod = Column(Integer, primary_key=True, autoincrement=True)
    nombre = Column(String(80))
    precio = Column(Float)
    stock = Column(DECIMAL(10, 3))
    codpactivo = Column(Integer)
    pactivo = Column(String(110))
    codmarca = Column(Integer)
    codbarra01 = Column(String(20))
    created_at = Column(DateTime)
    updated_at = Column(DateTime)
    hash = Column(String(64))


    def __repr__(self):
        return f"<Producto(codprod={self.codprod}, nombre={self.nombre}, precio={self.precio}, stock={self.stock})>"
