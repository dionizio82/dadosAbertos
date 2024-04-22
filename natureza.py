from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Natureza(Base):
    __tablename__ = 'natureza'
    id = Column(Integer, primary_key=True)
    cod_natureza = Column(Integer)
    desc_natureza = Column(String)

empresa = relationship("Empresa", back_populates="natureza")