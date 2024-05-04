const fs = require('fs');
const csv = require('csv-parser');
const { Pool } = require('pg');

let lastProcessedLine = 0; // Linha processada por último

if (fs.existsSync('progress.txt')) {
  lastProcessedLine = parseInt(fs.readFileSync('progress.txt', 'utf8'), 10) || 0;
}

const pool = new Pool({
  user: 'postgres',
  host: 'localhost',
  database: 'dados_abertos',
  password: '852518',
  port: 5432,
  max: 10, // Número máximo de conexões
});

async function processBatch(batch) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN'); // Iniciar transação

    for (const data of batch) {
      const [cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa] = data;

      const existing = await client.query('SELECT * FROM empresas WHERE cnpj_basico = $1', [cnpj_basico]);

      if (existing.rowCount > 0) {
        const oldRecord = existing.rows[0];
        const hasChanges = 
          razao_social !== oldRecord.razao_social ||
          natureza_juridica !== oldRecord.natureza_juridica ||
          qualificacao_responsavel !== oldRecord.qualificacao_responsavel ||
          capital_social !== parseFloat(oldRecord.capital_social) ||
          porte_empresa !== oldRecord.porte_empresa;

        if (hasChanges) {
          await client.query(`
            INSERT INTO empresas_history (cnpj_basico, modified_field, modified_at)
            VALUES ($1, 'razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa', CURRENT_TIMESTAMP)`,
            [cnpj_basico]
          );

          await client.query(`
            UPDATE empresas
            SET razao_social = $2, natureza_juridica = $3, qualificacao_responsavel = $4, capital_social = $5, porte_empresa = $6, updated_at = CURRENT_TIMESTAMP
            WHERE cnpj_basico = $1`,
            [cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa]
          );
        }
      } else {
        await client.query(`
          INSERT INTO empresas (cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa, created_at, updated_at)
          VALUES ($1, $2, $3, $4, $5, $6, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
          [cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa]
        );
      }
    }

    await client.query('COMMIT'); // Finalizar transação
  } catch (error) {
    await client.query('ROLLBACK'); // Reverter transação em caso de erro
    console.error('Erro ao processar lote:', error.message);
  } finally {
    client.release();
  }

  console.log(`Processado lote de ${batch.length} registros`);
}

async function processCSV() {
  const batchSize = 5000; // Aumentando o tamanho do lote
  let batch = [];
  let currentLine = 0;

  try {
    fs.createReadStream('arquivo.csv')
      .pipe(csv({
        headers: ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa'],
        skipLines: 1 + lastProcessedLine,
        separator: ';',
        mapValues: ({ value }) => typeof value === 'string' ? value.replace(/"/g, '') : value
      }))
      .on('data', async (data) => {
        currentLine++;
        if (currentLine <= lastProcessedLine) return;

        // Validação de dados
        if (!data.cnpj_basico || !data.razao_social) return;

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

        fs.writeFileSync('progress.txt', currentLine.toString());
      })
      .on('end', async () => {
        if (batch.length > 0) await processBatch(batch);
        console.log("Processamento do CSV concluído.");
        fs.unlinkSync('progress.txt'); // Removendo arquivo de progresso
        await pool.end(); // Encerrando todas as conexões
      });
  } catch (error) {
    console.error('Erro ao processar CSV:', error.message);
    await pool.end(); // Encerrando todas as conexões em caso de erro
  }
}

processCSV();
