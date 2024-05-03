const { workerData, parentPort } = require('worker_threads');
const fs = require('fs');
const csv = require('csv-parser');
const { Client } = require('pg');

const client = new Client({
  user: 'postgres',
  host: 'localhost',
  database: 'dados_abertos',
  password: '852518',
  port: 5432,
});

// Função auxiliar para remover aspas duplas dos valores
function sanitizeValue(value) {
  return typeof value === 'string' ? value.replace(/"/g, '') : '';
}

async function processChunk(batch) {
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
  }
  

async function processFile() {
  const batchSize = 1;
  let batch = [];
  const { filename, workerIndex, totalWorkers } = workerData;

  await client.connect();

  let lineNumber = 0;
  fs.createReadStream(filename)
    .pipe(csv({
      headers: ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa'],
      separator: ';',
      mapValues: ({ value }) => sanitizeValue(value)
    }))
    .on('data', (data) => {
      if (lineNumber % totalWorkers === workerIndex) {
        // Processar apenas linhas para este worker
        const sanitizedData = [/*...*/];
        batch.push(sanitizedData);

        if (batch.length === batchSize) {
          processChunk(batch);
          batch = [];          
        }
      }
      lineNumber++;
    })
    .on('end', () => {
        if (batch.length > 0) {
          processChunk(batch);
          batch = [];          
        }
        console.log(`Worker ${workerIndex} completou processamento.`);
        client.end();
        parentPort.postMessage('done');
      });
    }
processFile().catch((e) => console.error(e));
