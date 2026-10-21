const sql = require('mssql');

// Configuração do Servidor 1 (Principal)
const dbConfig1 = {
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  server: process.env.DB_SERVER, 
  database: process.env.DB_NAME,
  options: {
    encrypt: true,
    trustServerCertificate: true,
    // ✅ ADICIONADO: Timeout de 10 minutos para queries pesadas (ex: TIM)
    requestTimeout: 600000,
    cancelTimeout: 600000,
    connectTimeout: 60000
  },
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000
  }
};

// Configuração do Servidor 2 (IP: 172.22.4.18)
const dbConfig2 = {
  user: process.env.DB2_USER,
  password: process.env.DB2_PASSWORD,
  server: process.env.DB2_SERVER, 
  database: process.env.DB2_NAME,
  options: {
    encrypt: true,
    trustServerCertificate: true,
    // ✅ ADICIONADO: Timeout de 10 minutos
    requestTimeout: 600000,
    cancelTimeout: 600000,
    connectTimeout: 60000
  },
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000
  }
};

// Configuração do Servidor 3 (IP: 172.22.4.109 - Autenticação do Windows)
const dbConfig3 = {
  server: process.env.DB3_SERVER || '172.22.4.109',
  database: process.env.DB3_NAME,
  authentication: {
    type: 'ntlm',
    options: {
      domain: process.env.DB3_DOMAIN,  // Domínio da rede (ex: 'MEUDOMINIO')
      userName: process.env.DB3_USER,  // Usuário do Windows (sem o domínio)
      password: process.env.DB3_PASSWORD // Senha do usuário do Windows
    }
  },
  options: {
    encrypt: true,
    trustServerCertificate: true,
    // ✅ ADICIONADO: Timeout de 10 minutos
    requestTimeout: 600000,
    cancelTimeout: 600000,
    connectTimeout: 60000
  },
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000
  }
};

// Configuração do Servidor 4 (IP: 172.22.4.108)
const dbConfig4 = {
  user: process.env.DB4_USER,
  password: process.env.DB4_PASSWORD,
  server: process.env.DB4_SERVER || '172.22.4.108', 
  database: process.env.DB4_NAME,
  options: {
    encrypt: true,
    trustServerCertificate: true,
    // ✅ ADICIONADO: Timeout de 10 minutos
    requestTimeout: 600000,
    cancelTimeout: 600000,
    connectTimeout: 60000
  },
  pool: {
    max: 10,
    min: 0,
    idleTimeoutMillis: 30000
  }
};

// Referências dos Pools
let pool1;
let pool2;
let pool3;
let pool4;

/**
 * Conexão com o Banco de Dados Principal
 */
async function conectarBD() {
  try {
    if (!pool1) {
      pool1 = await new sql.ConnectionPool(dbConfig1).connect();
      console.log('✅ Conexão com o Servidor Principal estabelecida.');
      
      // Monitoramento de erros no pool
      pool1.on('error', err => {
        console.error('❌ Erro no pool do Servidor Principal:', err);
        pool1 = null; // Força reconexão na próxima chamada
      });
    }
    return pool1;
  } catch (error) {
    console.error('❌ Erro ao conectar ao Servidor Principal:', error);
    pool1 = null;
    throw error;
  }
}

/**
 * Conexão com o Banco de Dados Secundário (172.22.4.18)
 */
async function conectarBD2() {
  try {
    if (!pool2) {
      pool2 = await new sql.ConnectionPool(dbConfig2).connect();
      console.log('✅ Conexão com o Servidor 2 (172.22.4.18) estabelecida.');
      
      pool2.on('error', err => {
        console.error('❌ Erro no pool do Servidor 2:', err);
        pool2 = null;
      });
    }
    return pool2;
  } catch (error) {
    console.error('❌ Erro ao conectar ao Servidor 2 (172.22.4.18):', error);
    pool2 = null;
    throw error;
  }
}

/**
 * Conexão com o Banco de Dados Terciário (172.22.4.109 - Windows Auth)
 */
async function conectarBD3() {
  try {
    if (!pool3) {
      pool3 = await new sql.ConnectionPool(dbConfig3).connect();
      console.log('✅ Conexão com o Servidor 3 (172.22.4.109) estabelecida.');
      
      pool3.on('error', err => {
        console.error('❌ Erro no pool do Servidor 3:', err);
        pool3 = null;
      });
    }
    return pool3;
  } catch (error) {
    console.error('❌ Erro ao conectar ao Servidor 3 (172.22.4.109):', error);
    pool3 = null;
    throw error;
  }
}

/**
 * Conexão com o Banco de Dados Quarto (172.22.4.108)
 */
async function conectarBD4() {
  try {
    if (!pool4) {
      pool4 = await new sql.ConnectionPool(dbConfig4).connect();
      console.log('✅ Conexão com o Servidor 4 (172.22.4.108) estabelecida.');
      
      pool4.on('error', err => {
        console.error('❌ Erro no pool do Servidor 4:', err);
        pool4 = null;
      });
    }
    return pool4;
  } catch (error) {
    console.error('❌ Erro ao conectar ao Servidor 4 (172.22.4.108):', error);
    pool4 = null;
    throw error;
  }
}

module.exports = { 
  conectarBD, 
  conectarBD2, 
  conectarBD3,
  conectarBD4,
  sql 
};