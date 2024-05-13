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

// Configuração do arquivo de log
const logStream = fs.createWriteStream('erroresImportacao.log', { flags: 'a' });

const logError = (error) => {
  const message = `${new Date().toISOString()} - ${error}\n`;
  logStream.write(message);
  console.error(error);
};

const convertDate = (date) => {
  return date && date !== '00000000' ? `${date.substring(0, 4)}-${date.substring(4, 6)}-${date.substring(6, 8)}` : null;
};

const importCSV = async () => {
  const client = await pool.connect();
  try {
    const csvFilePath = 'dadosSimples.csv';
    const fileStream = fs.createReadStream(csvFilePath, { encoding: 'utf8' })
      .pipe(csv({
        separator: ';',
        headers: ['cnpj_basico', 'optante_simples', 'data_opcao', 'data_exclusao', 'optante_mei', 'data_opcao_mei', 'data_exclusao_mei']
      }));

    for await (const row of fileStream) {
      console.log(`Processando CNPJ: ${row.cnpj_basico}`);
      try {
        const currentData = await client.query(
          `SELECT * FROM dados_simples WHERE cnpj_basico = $1`,
          [row.cnpj_basico]
        );

        if (currentData.rows.length) {
          await client.query(
            `INSERT INTO dados_simples_history (cnpj_basico, optante_simples, data_opcao, data_exclusao, optante_mei, data_opcao_mei, data_exclusao_mei, modified_field)
             VALUES ($1, $2, $3, $4, $5, $6, $7, 'update')
            `,
            [row.cnpj_basico, currentData.rows[0].optante_simples, currentData.rows[0].data_opcao, currentData.rows[0].data_exclusao, currentData.rows[0].optante_mei, currentData.rows[0].data_opcao_mei, currentData.rows[0].data_exclusao_mei]
          );
        }

        await client.query(
          `INSERT INTO dados_simples (cnpj_basico, optante_simples, data_opcao, data_exclusao, optante_mei, data_opcao_mei, data_exclusao_mei)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (cnpj_basico) DO UPDATE SET
           optante_simples = EXCLUDED.optante_simples, data_opcao = EXCLUDED.data_opcao, data_exclusao = EXCLUDED.data_exclusao, optante_mei = EXCLUDED.optante_mei, data_opcao_mei = EXCLUDED.data_opcao_mei, data_exclusao_mei = EXCLUDED.data_exclusao_mei`,
          [row.cnpj_basico, row.optante_simples === 'S', convertDate(row.data_opcao), convertDate(row.data_exclusao), row.optante_mei === 'S', convertDate(row.data_opcao_mei), convertDate(row.data_exclusao_mei)]
        );
      } catch (error) {
        logError(`Erro ao processar CNPJ ${row.cnpj_basico}: ${error}`);
      }
    }
    console.log('Importação de dados concluída.');
  } catch (error) {
    logError('Erro crítico no processo de importação: ' + error.message);
  } finally {
    client.release();
  }
}

importCSV();
