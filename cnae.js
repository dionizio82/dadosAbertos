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

async function importCSV() {
  const client = await pool.connect();
  try {
    const tableName = 'cnae';
    const csvFilePath = 'cnaes.csv';

    const fileStream = fs.createReadStream(csvFilePath, { encoding: 'utf8' })
      .pipe(csv({
        separator: ';',
        quote: '"',
        escape: '"',
        headers: ['codigo', 'descricao']
      }));

    for await (const row of fileStream) {
      await client.query(
        `INSERT INTO ${tableName} (codigo, descricao)
         VALUES ($1, $2)
         ON CONFLICT (codigo) DO UPDATE SET
         descricao = EXCLUDED.descricao, updated_at = CURRENT_TIMESTAMP`,
        [row.codigo, row.descricao]
      );
    }
    console.log('Importação de dados concluída.');
  } catch (error) {
    console.error('Erro ao importar CSV:', error.message);
  } finally {
    client.release();
  }
}

importCSV();
