from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class paises(Base):
    __tablename__ = 'paises'
    id = Column(Integer, primary_key=True)
    cod_pais = Column(Integer)
    nome_pais = Column(String)

empresa = relationship("Empresa", back_populates="paises")