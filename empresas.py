from sqlalchemy import Column, Integer, String, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Empresa(Base):
    __tablename__ = 'empresas'
    cnpj_basico = Column(String, primary_key=True)
    razao_social = Column(String)
    natureza_juridica = Column(String)
    qualificacao_responsavel = Column(Integer)
    capital_social = Column(Float)
    porte_empresa = Column(Integer)
  
    estabelecimentos = relationship("Estabelecimento", back_populates="empresa")
    socio = relationship("Socio", back_populates="empresa")
    dadosSimples = relationship("DadosSimples", back_populates="empresa")
    pais = relationship("pais", back_populates="empresa")
    municipio = relationship("municipio", back_populates="empresa")
    qualifica = relationship("qualifica", back_populates="empresa")
    natureza = relationship("natureza", back_populates="empresa")
    cnae = relationship("cnae", back_populates="empresa")