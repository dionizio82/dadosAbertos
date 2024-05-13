const { Pool } = require('pg');

const pool = new Pool({
    user: 'postgres',
    host: 'localhost',
    database: 'dados_abertos',
    password: '852518',
    port: 5432,
});

async function consultarCNPJ(cnpj) {
  const client = await pool.connect();
  try {
    const res = await client.query('SELECT * FROM empresas WHERE cnpj_basico = $1', [cnpj]);
    if (res.rows.length > 0) {
      console.log('Detalhes do CNPJ:', res.rows[0]);
    } else {
      console.log('CNPJ não encontrado.');
    }
  } catch (err) {
    console.error('Erro ao consultar o CNPJ:', err);
  } finally {
    client.release();
  }
}

// Substitua '12345678' pelo CNPJ que você deseja consultar
consultarCNPJ('20765798');
