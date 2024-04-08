from sqlalchemy import Column, Integer, String, Date
from sqlalchemy import ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class DadosSimples(Base):
    __tablename__ = 'dadosSimples'
    id = Column(Integer, primary_key=True)
    cnpj_basico = Column(String, ForeignKey('empresas.cnpj_basico'))
    opcao_SN = Column(String)
    data_opcao_SN = Column(Date)
    data_exclusao_SN = Column(Date)
    opcao_MEI = Column(String)
    data_opcao_MEI = Column(Date)
    data_exclusao_MEI = Column(Date)    
    
    empresa = relationship("Empresa", back_populates="dadosSimples")