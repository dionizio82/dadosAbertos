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

const logStream = fs.createWriteStream('empresasImportacao.log', { flags: 'a' });

const logError = (cnpj, error) => {
  const message = `${new Date().toISOString()} - Erro no CNPJ ${cnpj}: ${error}\n`;
  logStream.write(message);
  console.error(message);
};

const convertEmptyToZero = (value) => {
  return value.trim() === "" ? 0 : value;
};

const processRecord = async (data) => {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const { cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa } = data;
    const queryParams = [
      cnpj_basico.trim(),
      razao_social.trim(),
      convertEmptyToZero(natureza_juridica),
      convertEmptyToZero(qualificacao_responsavel),
      parseFloat(convertEmptyToZero(capital_social).replace(',', '.')),
      convertEmptyToZero(porte_empresa)
    ];

    const existing = await client.query('SELECT * FROM empresas WHERE cnpj_basico = $1', [cnpj_basico]);
    if (existing.rowCount > 0) {
      await client.query(`
        INSERT INTO empresas_history (cnpj_basico, modified_field, modified_at)
        VALUES ($1, 'update', CURRENT_TIMESTAMP)`, [cnpj_basico]);
    }

    await client.query(`
      INSERT INTO empresas (cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa, created_at, updated_at)
      VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
      ON CONFLICT (cnpj_basico) DO UPDATE SET
      razao_social = EXCLUDED.razao_social, natureza_juridica = EXCLUDED.natureza_juridica, qualificacao_responsavel = EXCLUDED.qualificacao_responsavel, capital_social = EXCLUDED.capital_social, porte_empresa = EXCLUDED.porte_empresa, updated_at = CURRENT_TIMESTAMP`, queryParams);

    await client.query('COMMIT');
  } catch (error) {
    await client.query('ROLLBACK');
    logError(cnpj_basico, error.message);
  } finally {
    client.release();
  }
};

const processCSV = async () => {
  const fileStream = fs.createReadStream('novo6.csv', { encoding: 'utf8' })
    .pipe(csv({
      separator: ';',
      headers: ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa']
    }));

  for await (const row of fileStream) {
    console.log(`Processando CNPJ: ${row.cnpj_basico}`);
    await processRecord(row);
  }
  console.log('Importação de dados concluída.');
};

processCSV();
