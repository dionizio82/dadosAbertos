const fs = require('fs');
const csv = require('csv-parser');
const { Client } = require('pg');
const { Worker } = require('worker_threads');

// Configurações do banco de dados
const client = new Client({
  user: 'postgres',
  host: 'localhost',
  database: 'dados_abertos',
  password: '852518',
  port: 5432,
});

// Iniciar um worker para processar o arquivo
function runService(workerData) {
  return new Promise((resolve, reject) => {
    const worker = new Worker('./worker.js', { workerData });
    worker.on('message', resolve);
    worker.on('error', reject);
    worker.on('exit', (code) => {
      if (code !== 0) reject(new Error(`Worker stopped with exit code ${code}`));
    });
  });
}

async function processFile() {
  const numWorkers = 4; // Use um número de workers baseado em núcleos
  const promises = [];
  for (let i = 0; i < numWorkers; i++) {
    promises.push(runService({ filename: 'arquivo.csv', workerIndex: i, totalWorkers: numWorkers }));
  }
  await Promise.all(promises);
  console.log('Processamento do CSV concluído.');
}

processFile().catch((e) => console.error(e));


// Função auxiliar para validar e converter para inteiro
function parseIntOrDefault(value, defaultValue = 0) {
  const parsed = parseInt(value, 10);
  return isNaN(parsed) ? defaultValue : parsed;
}

// Função auxiliar para validar e converter para float
function parseFloatOrDefault(value, defaultValue = 0.0) {
  if (!value) return defaultValue;
  const parsed = parseFloat(value.replace(',', '.'));
  return isNaN(parsed) ? defaultValue : parsed;
}

// Função auxiliar para remover aspas duplas dos valores
function sanitizeValue(value) {
  return typeof value === 'string' ? value.replace(/"/g, '') : '';
}

// Função para salvar um lote de registros no banco de dados e adicionar histórico
async function saveBatchToDatabase(batch) {
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
}


// Função principal para ler o arquivo CSV e salvar os dados no banco de dados
async function processCSV() {
  const batchSize = 5;
  let batch = [];

  try {
    await client.connect();

    fs.createReadStream('arquivo.csv')
      .pipe(csv({
        headers: ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa'],
        skipLines: 1,
        separator: ';',
        mapValues: ({ value }) => sanitizeValue(value)
      }))
      .on('data', (data) => {
        const sanitizedData = [
          data.cnpj_basico, data.razao_social, data.natureza_juridica,
          parseIntOrDefault(data.qualificacao_responsavel),
          parseFloatOrDefault(data.capital_social),
          parseIntOrDefault(data.porte_empresa)
        ];
        batch.push(sanitizedData);

        if (batch.length === batchSize) {
          saveBatchToDatabase(batch);
          batch = [];          
        }
      })
      .on('end', () => {
        if (batch.length > 0) {
          saveBatchToDatabase(batch);
          batch = [];          
        }
        console.log("Processamento do CSV concluído.");
        client.end();
      });
  } catch (error) {
    console.error('Erro ao processar CSV:', error.message);
    await client.end();
  }
}

processCSV();
