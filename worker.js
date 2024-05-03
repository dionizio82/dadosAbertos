const { workerData, parentPort } = require('worker_threads');
const fs = require('fs');
const csv = require('csv-parser');
const { Client } = require('pg');

async function processChunk(batch) {
  const client = new Client({
    user: 'postgres',
    host: 'localhost',
    database: 'dados_abertos',
    password: '852518',
    port: 5432,
  });

  try {
    await client.connect();

    for (const data of batch) {
      const [cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa] = data;

      try {
        // Checar se a empresa já existe
        const existing = await client.query('SELECT * FROM empresas WHERE cnpj_basico = $1', [cnpj_basico]);

        if (existing.rowCount > 0) {
          // Atualizar registro existente
          await client.query(`
            UPDATE empresas
            SET razao_social = $2, natureza_juridica = $3, qualificacao_responsavel = $4, capital_social = $5, porte_empresa = $6, updated_at = CURRENT_TIMESTAMP
            WHERE cnpj_basico = $1`,
            [cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa]
          );
        } else {
          // Inserir novo registro
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
  } catch (error) {
    console.error('Erro ao conectar ao banco de dados:', error.message);
  } finally {
    await client.end();
  }
}

async function processFile() {
  const { filename, workerIndex, totalWorkers } = workerData;
  let lineNumber = 0;
  let batch = [];

  fs.createReadStream(filename)
    .pipe(csv({
      headers: ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa'],
      separator: ';',
    }))
    .on('data', (data) => {
      if (lineNumber % totalWorkers === workerIndex) {
        const sanitizedData = [
          data.cnpj_basico, data.razao_social, data.natureza_juridica,
          parseInt(data.qualificacao_responsavel), parseFloat(data.capital_social), parseInt(data.porte_empresa)
        ];
        batch.push(sanitizedData);

        if (batch.length === 5) {
          processChunk(batch);
          batch = [];
        }
      }
      lineNumber++;
    })
    .on('end', () => {
      if (batch.length > 0) {
        processChunk(batch);
      }
      console.log(`Worker ${workerIndex} completou processamento.`);
      parentPort.postMessage('done');
    });
}

processFile().catch((error) => console.error('Erro ao processar o arquivo:', error.message));
