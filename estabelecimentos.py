from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship

class Estabelecimento(Base):
    __tablename__ = 'estabelecimentos'
    id = Column(Integer, primary_key=True)
    cnpj_basico = Column(String, ForeignKey('empresas.cnpj_basico'))
    cnpj_ordem = Column(String)
    cnpj_dv = Column(String)
    matriz_fil = Column(Integer)
    nome_fan = Column(String)
    matriz_fil = Column(Integer)
    
    empresa = relationship("Empresa")
