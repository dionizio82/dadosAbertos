const fs = require('fs');
const csv = require('csv-parser');
const { Pool } = require('pg');

const pool = new Pool({
  user: 'postgres',
  host: 'localhost',
  database: 'dados_abertos',
  password: '852518',
  port: 5432,
  client_encoding: 'utf8'
});

const logStream = fs.createWriteStream('estabelecimentosImportacao.log', { flags: 'a' });

const logError = (identifier, error) => {
  const message = `${new Date().toISOString()} - Erro no Estabelecimento ${identifier}: ${error}\n`;
  logStream.write(message);
  console.error(message);
};

const processRecord = async (data) => {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      const {
        cnpj_basico, cnpj_ordem, cnpj_dv, id_matriz_filial, nome_fantasia, situacao_cadastral, data_situacao_cadastral,
        motivo_situacao_cadastral, nome_cidade_exterior, pais, data_inicio_atividade, cnae_fiscal_principal, cnae_secundario,
        tipo_logradouro, logradouro, numero, complemento, bairro, cep, uf, municipio, ddd1_logradouro, fone1_logradouro,
        ddd2_logradouro, fone2_logradouro, ddd_fax_logradouro, fone_fax_logradouro, email, situacao_especial, data_situacao_especial
      } = data;
  
      const queryParams = [
        cnpj_basico, cnpj_ordem, cnpj_dv, id_matriz_filial, nome_fantasia, situacao_cadastral, data_situacao_cadastral ? new Date(data_situacao_cadastral.substring(0, 4), data_situacao_cadastral.substring(4, 6) - 1, data_situacao_cadastral.substring(6, 8)) : null,
        motivo_situacao_cadastral, nome_cidade_exterior, pais, data_inicio_atividade ? new Date(data_inicio_atividade.substring(0, 4), data_inicio_atividade.substring(4, 6) - 1, data_inicio_atividade.substring(6, 8)) : null, cnae_fiscal_principal, cnae_secundario,
        tipo_logradouro, logradouro, numero, complemento, bairro, cep, uf, municipio, ddd1_logradouro, fone1_logradouro, ddd2_logradouro, fone2_logradouro, ddd_fax_logradouro, fone_fax_logradouro, email, situacao_especial, data_situacao_especial ? new Date(data_situacao_especial.substring(0, 4), data_situacao_especial.substring(4, 6) - 1, data_situacao_especial.substring(6, 8)) : null
      ];
  
      await client.query(`
        INSERT INTO estabelecimentos (cnpj_basico, cnpj_ordem, cnpj_dv, id_matriz_filial, nome_fantasia, situacao_cadastral, data_situacao_cadastral,
          motivo_situacao_cadastral, nome_cidade_exterior, pais, data_inicio_atividade, cnae_fiscal_principal, cnae_secundario,
          tipo_logradouro, logradouro, numero, complemento, bairro, cep, uf, municipio, ddd1_logradouro, fone1_logradouro,
          ddd2_logradouro, fone2_logradouro, ddd_fax_logradouro, fone_fax_logradouro, email, situacao_especial, data_situacao_especial, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        ON CONFLICT (cnpj_basico, cnpj_ordem) DO UPDATE SET
          nome_fantasia = EXCLUDED.nome_fantasia, id_matriz_filial = EXCLUDED.id_matriz_filial, situacao_cadastral = EXCLUDED.situacao_cadastral,
          data_situacao_cadastral = EXCLUDED.data_situacao_cadastral, motivo_situacao_cadastral = EXCLUDED.motivo_situacao_cadastral,
          nome_cidade_exterior = EXCLUDED.nome_cidade_exterior, pais = EXCLUDED.pais, data_inicio_atividade = EXCLUDED.data_inicio_atividade,
          cnae_fiscal_principal = EXCLUDED.cnae_fiscal_principal, cnae_secundario = EXCLUDED.cnae_secundario, tipo_logradouro = EXCLUDED.tipo_logradouro,
          logradouro = EXCLUDED.logradouro, numero = EXCLUDED.numero, complemento = EXCLUDED.complemento, bairro = EXCLUDED.bairro,
          cep = EXCLUDED.cep, uf = EXCLUDED.uf, municipio = EXCLUDED.municipio, ddd1_logradouro = EXCLUDED.ddd1_logradouro,
          fone1_logradouro = EXCLUDED.fone1_logradouro, ddd2_logradouro = EXCLUDED.ddd2_logradouro, fone2_logradouro = EXCLUDED.fone2_logradouro,
          ddd_fax_logradouro = EXCLUDED.ddd_fax_logradouro, fone_fax_logradouro = EXCLUDED.fone_fax_logradouro, email = EXCLUDED.email,
          situacao_especial = EXCLUDED.situacao_especial, data_situacao_especial = EXCLUDED.data_situacao_especial, updated_at = CURRENT_TIMESTAMP`, queryParams);
  
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      logError(`${data.cnpj_basico}/${data.cnpj_ordem}-${data.cnpj_dv}`, error.message);
    } finally {
      client.release();
    }
  };
  

const processCSV = async () => {
  const fileStream = fs.createReadStream('estabelecimentos1.csv', { encoding: 'utf8' })
    .pipe(csv({
      separator: ';',
      headers: ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'id_matriz_filial', 'nome_fantasia', 'situacao_cadastral', 'data_situacao_cadastral',
                'motivo_situacao_cadastral', 'nome_cidade_exterior', 'pais', 'data_inicio_atividade', 'cnae_fiscal_principal',
                'cnae_secundario', 'tipo_logradouro', 'logradouro', 'numero', 'complemento', 'bairro', 'cep', 'uf', 'municipio',
                'ddd1_logradouro', 'fone1_logradouro', 'ddd2_logradouro', 'fone2_logradouro', 'ddd_fax_logradouro', 'fone_fax_logradouro',
                'email', 'situacao_especial', 'data_situacao_especial']
    }));

  for await (const row of fileStream) {
    console.log(`Processando Estabelecimento: ${row.cnpj_basico}/${row.cnpj_ordem}-${row.cnpj_dv}`);
    await processRecord(row);
  }
  console.log('Importação de dados concluída.');
};

processCSV();
