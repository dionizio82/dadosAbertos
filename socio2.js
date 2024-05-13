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

const logStream = fs.createWriteStream('socioImportacao.log', { flags: 'a' });

const logError = (cnpj, cpfCnpj, error) => {
  const message = `${new Date().toISOString()} - Erro no CNPJ ${cnpj}, CPF/CNPJ do Sócio ${cpfCnpj}: ${error}\n`;
  logStream.write(message);
  console.error(message);
};

const processRecord = async (data) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const { cnpj_basico, identificador_socio, nome_socio, cpf_cnpj_socio, qualificacao_socio, data_entrada, pais_socio, rep_legal, nome_rep_legal, qualificacao_rep_legal, faixa_etaria } = data;
    const queryParams = [
      cnpj_basico, cpf_cnpj_socio, identificador_socio, nome_socio, qualificacao_socio,
      data_entrada ? new Date(data_entrada.substring(0, 4), data_entrada.substring(4, 6) - 1, data_entrada.substring(6, 8)) : null,
      pais_socio, rep_legal, nome_rep_legal, qualificacao_rep_legal, faixa_etaria
    ];

    await client.query(`
      INSERT INTO socio (cnpj_basico, cpf_cnpj_socio, identificador_socio, nome_socio, qualificacao_socio, data_entrada, pais_socio, rep_legal, nome_rep_legal, qualificacao_rep_legal, faixa_etaria, created_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      ON CONFLICT (cnpj_basico, cpf_cnpj_socio) DO UPDATE SET
      identificador_socio = EXCLUDED.identificador_socio, nome_socio = EXCLUDED.nome_socio, qualificacao_socio = EXCLUDED.qualificacao_socio, data_entrada = EXCLUDED.data_entrada, pais_socio = EXCLUDED.pais_socio, rep_legal = EXCLUDED.rep_legal, nome_rep_legal = EXCLUDED.nome_rep_legal, qualificacao_rep_legal = EXCLUDED.qualificacao_rep_legal, faixa_etaria = EXCLUDED.faixa_etaria, updated_at = CURRENT_TIMESTAMP`, queryParams);

    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    logError(cnpj_basico, cpf_cnpj_socio, error.message);
  } finally {
    client.release();
  }
};

const processCSV = async () => {
  const fileStream = fs.createReadStream('socio2.csv', { encoding: 'utf8' })
    .pipe(csv({
      separator: ';',
      headers: ['cnpj_basico', 'identificador_socio', 'nome_socio', 'cpf_cnpj_socio', 'qualificacao_socio', 'data_entrada', 'pais_socio', 'rep_legal', 'nome_rep_legal', 'qualificacao_rep_legal', 'faixa_etaria']
    }));

  for await (const row of fileStream) {
    console.log(`Processando CNPJ: ${row.cnpj_basico}, CPF/CNPJ do Sócio: ${row.cpf_cnpj_socio}`);
    await processRecord(row);
  }
  console.log('Importação de dados concluída.');
};

processCSV();
