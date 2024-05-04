const fs = require('fs');
const csv = require('csv-parser');
const { Client } = require('pg');

// Função principal para processar os dados do CSV
async function processCSV() {
  const batchSize = 10;
  let batch = [];

  const client = new Client({
    user: 'postgres',
    host: 'localhost',
    database: 'dados_abertos',
    password: '852518',
    port: 5432,
  });

  await client.connect();

  const processBatch = async (batch) => {
    for (const data of batch) {
      const [cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa] = data;

      try {
        const existing = await client.query('SELECT * FROM empresas WHERE cnpj_basico = $1', [cnpj_basico]);

        if (existing.rowCount > 0) {
          await client.query(`
            UPDATE empresas
            SET razao_social = $2, natureza_juridica = $3, qualificacao_responsavel = $4, capital_social = $5, porte_empresa = $6, updated_at = CURRENT_TIMESTAMP
            WHERE cnpj_basico = $1`,
            [cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa]
          );
        } else {
          await client.query(`
            INSERT INTO empresas (cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
            [cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa]
          );
        }
      } catch (error) {
        console.error(`Erro ao processar ${cnpj_basico}:`, error.message);
      }
    }
    console.log(`Processado lote de ${batch.length} registros`);
  };

  try {
    fs.createReadStream('arquivo.csv')
      .pipe(csv({
        headers: ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa'],
        skipLines: 1,
        separator: ';',
        mapValues: ({ value }) => typeof value === 'string' ? value.replace(/"/g, '') : value
      }))
      .on('data', async (data) => {
        const sanitizedData = [
          data.cnpj_basico, data.razao_social, data.natureza_juridica,
          parseInt(data.qualificacao_responsavel, 10) || 0,
          parseFloat(data.capital_social.replace(',', '.')) || 0.0,
          parseInt(data.porte_empresa, 10) || 0
        ];
        batch.push(sanitizedData);

        if (batch.length === batchSize) {
          await processBatch(batch);
          batch = [];
        }
      })
      .on('end', async () => {
        if (batch.length > 0) await processBatch(batch);
        console.log("Processamento do CSV concluído.");
        await client.end();
      });
  } catch (error) {
    console.error('Erro ao processar CSV:', error.message);
    await client.end();
  }
}

processCSV();
