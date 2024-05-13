from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Municipio(Base):
    __tablename__ = 'municipio'
    id = Column(Integer, primary_key=True)
    cod_municipio = Column(Integer)
    nome_municipio = Column(String)

empresa = relationship("Empresa", back_populates="municipio")