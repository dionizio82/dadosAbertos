class Socio(Base):
    __tablename__ = 'socios'
    id = Column(Integer, primary_key=True)
    cnpj_basico = Column(String, ForeignKey('empresas.cnpj_basico'))
    nome_socio = Column(String)
    cpf_socio = Column(String)
    qualificacao_socio = Column(String)    
    

    empresa = relationship("Empresa", back_populates="socios")
