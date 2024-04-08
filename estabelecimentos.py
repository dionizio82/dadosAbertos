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
    sit_cad = Column(Integer)
    data_sit_cad = Column(Date)
    motivo_sit_cad = Column(String)
    cidade_ex = Column(String)
    pais_cod = Column(String)
    data_inicio = Column(Date)
    cnae_pri = Column(String)
    cnae_sec = Column(String)
    tipo_log = Column(String)
    logradouro = Column(String)
    numero = Column(String)
    complemento = Column(String)
    bairro = Column(String)
    cep = Column(String)
    uf = Column(String)
    municipio = Column(String)
    ddd1 = Column(String)
    telefone1 = Column(String)
    ddd2 = Column(String)
    telefone2 = Column(String)
    dddFax = Column(String)
    telefoneFax = Column(String)
    email = Column(String)
    sit_especial = Column(String)
    data_sit_especial = Column(Date)
    
    
    empresa = relationship("Empresa", back_populates="estabelecimentos")
