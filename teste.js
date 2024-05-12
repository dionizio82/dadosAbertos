const fs = require('fs');
const csv = require('csv-parser');
const { Pool } = require('pg');
const copyFrom = require('pg-copy-streams').from;
const Transform = require('stream').Transform;

const pool = new Pool({
  user: 'postgres',
  host: 'localhost',
  database: 'dados_abertos',
  password: '852518',
  port: 5432,
});

async function importCSV() {
  const client = await pool.connect();
  try {
    const tableName = 'empresas';
    const csvFilePath = 'arquivo.csv';

    const copyQuery = `COPY ${tableName} (cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa, ente_fed) FROM STDIN WITH (FORMAT csv, DELIMITER ';', HEADER TRUE, QUOTE '"', NULL '')`;
    const stream = client.query(copyFrom(copyQuery));

    const transformer = new Transform({
      writableObjectMode: true,
      readableObjectMode: false,
      transform(data, encoding, callback) {
        data.capital_social = data.capital_social.replace(',', '.');
        const line = `${data.cnpj_basico};${data.razao_social};${data.natureza_juridica};${data.qualificacao_responsavel};${data.capital_social};${data.porte_empresa};${data.ente_fed || ''}\n`;
        console.log(`Processing line: ${line}`); // Log each line being processed
        this.push(line);
        callback();
      }
    });

    const fileStream = fs.createReadStream(csvFilePath)
      .pipe(csv({
        separator: ';',
        quote: '"',
        escape: '"',
        headers: ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa', 'ente_fed'],
        skipLines: 1 // Skip header row
      }))
      .pipe(transformer);

    fileStream.on('error', (error) => {
      console.error('Erro ao ler o arquivo:', error.message);
    });

    stream.on('error', (error) => {
      console.error('Erro ao executar COPY:', error.message);
    });

    stream.on('finish', () => {
      console.log('Importação de dados concluída.');
      client.release();
    });

    fileStream.pipe(stream);

  } catch (error) {
    console.error('Erro ao importar CSV:', error.message);
    client.release();
  }
}

importCSV();
