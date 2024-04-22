from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Qualifica(Base):
    __tablename__ = 'qualifica'
    id = Column(Integer, primary_key=True)
    cod_qualifica = Column(Integer)
    desc_qualifica = Column(String)

empresa = relationship("Empresa", back_populates="qualifica")