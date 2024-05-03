const fs = require('fs');
const csv = require('csv-parser');
const { Client } = require('pg');

// Configurações do banco de dados
const client = new Client({
  user: 'postgres',
  host: 'localhost',
  database: 'dados_abertos',
  password: '852518',
  port: 5432,
});

// Função auxiliar para validar e converter para inteiro
function parseIntOrDefault(value, defaultValue = 0) {
  const parsed = parseInt(value, 10);
  return isNaN(parsed) ? defaultValue : parsed;
}

// Função auxiliar para validar e converter para float
function parseFloatOrDefault(value, defaultValue = 0.0) {
  if (!value) return defaultValue; // Verificar se o valor é undefined ou null
  const parsed = parseFloat(value.replace(',', '.'));
  return isNaN(parsed) ? defaultValue : parsed;
}

// Função para salvar um registro no banco de dados
async function saveToDatabase(record) {
  const query = `
    INSERT INTO empresas (cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa)
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (cnpj_basico) DO NOTHING
  `;

  const values = [
    record.cnpj_basico,
    record.razao_social,
    record.natureza_juridica,
    parseIntOrDefault(record.qualificacao_responsavel),
    parseFloatOrDefault(record.capital_social),
    parseIntOrDefault(record.porte_empresa),
  ];

  try {
    await client.query(query, values);
    console.log(`Registro inserido: ${record.cnpj_basico}`);
  } catch (error) {
    console.error(`Erro ao inserir registro: ${record.cnpj_basico}`, error.message);
  }
}

// Função principal para ler o arquivo CSV e salvar os dados no banco de dados
async function processCSV() {
  try {
    await client.connect();

    // Ler o arquivo CSV e processar os dados
    fs.createReadStream('arquivo.csv')
      .pipe(csv({
        headers: ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte_empresa'],
        skipLines: 1 // Ignora a primeira linha (cabeçalho) caso esteja presente no arquivo
      }))
      .on('data', async (data) => await saveToDatabase(data))
      .on('end', () => {
        console.log("Processamento do CSV concluído.");
        client.end(); // Fechar a conexão ao finalizar
      });
  } catch (error) {
    console.error('Erro ao processar CSV:', error.message);
    await client.end(); // Garantir fechamento da conexão em caso de erro
  }
}

// Executar a função principal
processCSV();
