from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class cnae(Base):
    __tablename__ = 'cnae'
    id = Column(Integer, primary_key=True)
    cod_cnae = Column(Integer)
    desc_cnae = Column(String)

empresa = relationship("Empresa", back_populates="cnae")