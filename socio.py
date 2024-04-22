from sqlalchemy import Column, Integer, String, Date
from sqlalchemy import ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Socio(Base):
    __tablename__ = 'socio'
    id = Column(Integer, primary_key=True)
    cnpj_basico = Column(String, ForeignKey('empresas.cnpj_basico'))
    id_socio = Column(Integer)
    nome_socio = Column(String)
    cpf_CNPJ_socio = Column(String)
    qualificacao_socio = Column(String)
    data_ent_soc = Column(Date)
    pais_socio = Column(Integer)
    rep_socio = Column(Integer)
    nome_rep_socio = Column(Integer)
    qualificacao_rep_socio = Column(Integer)
    faixa_etaria_socio = Column(Integer)
    
    empresa = relationship("Empresa", back_populates="socio")
