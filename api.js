require('dotenv').config();
const express = require('express');
const cors = require('cors');
const path = require('path');
const router = express.Router();

// 💡 IMPORTANTE: Adicionado conectarBD4 para acessar o servidor onde está o BD_MIS
const { conectarBD, conectarBD2, conectarBD4, sql } = require('./db');

// ==========================================
// CONFIGURAÇÃO CENTRAL DE CUSTOS POR CANAL
// ==========================================
// Antes esse CASE WHEN estava duplicado 10x dentro das queries SQL.
// Agora existe um único ponto de verdade: para trocar um preço, muda-se
// aqui e a próxima consulta a qualquer carteira já usa o valor novo.
const CUSTO_POR_CANAL = {
    WHATSAPPMASSIVO: 0.160,
    SMSMASSIVO: 0.024,
    SMSPREVENTIVO: 0.024,
    EMAILMASSIVO: 0.005,
    EMAILPREVENTIVO: 0.005,
};

// Gera o bloco `CASE WHEN <alias> = 'X' THEN custo ... END` a partir da
// tabela acima, para ser interpolado dentro das queries SQL (template string).
function custoCanalCase(alias) {
    const linhas = Object.entries(CUSTO_POR_CANAL)
        .map(([evento, custo]) => `        WHEN ${alias} = '${evento}' THEN ${custo.toFixed(3)}`)
        .join('\n');
    return `CASE\n${linhas}\n        ELSE 0\n    END`;
}

// ==========================================
// CORS — restrito por variável de ambiente
// ==========================================
// Antes: cors() liberado para qualquer origem. Agora, se ORIGENS_PERMITIDAS
// estiver definida no .env (separada por vírgula), só essas origens passam.
// Sem a variável definida, mantém o comportamento antigo (aberto) para não
// quebrar ambientes de desenvolvimento — mas o ideal é sempre configurá-la em produção.
const origensPermitidas = (process.env.ORIGENS_PERMITIDAS || '')
    .split(',')
    .map(s => s.trim())
    .filter(Boolean);

const corsOptions = origensPermitidas.length
    ? {
        origin(origin, callback) {
            if (!origin || origensPermitidas.includes(origin)) return callback(null, true);
            callback(new Error('Origem não permitida pelo CORS'));
        },
    }
    : {};

// ==========================================
// RATE LIMIT SIMPLES (sem dependências externas)
// ==========================================
// Protege contra abuso/looping acidental no front. Para produção com múltiplas
// instâncias, prefira 'express-rate-limit' + um store compartilhado (Redis).
const JANELA_RATE_MS = 60 * 1000;
const LIMITE_REQUISICOES_JANELA = 30;
const contadorRequisicoes = new Map();

function limitarTaxa(req, res, next) {
    const chave = req.ip;
    const agora = Date.now();
    const registro = contadorRequisicoes.get(chave) || { inicio: agora, total: 0 };

    if (agora - registro.inicio > JANELA_RATE_MS) {
        registro.inicio = agora;
        registro.total = 0;
    }
    registro.total += 1;
    contadorRequisicoes.set(chave, registro);

    if (registro.total > LIMITE_REQUISICOES_JANELA) {
        return res.status(429).json({ status: 'erro', mensagem: 'Muitas requisições. Aguarde um instante e tente novamente.' });
    }
    next();
}

// ==========================================
// CACHE EM MEMÓRIA (TTL 5 minutos)
// ==========================================
const CACHE_TTL_MS = 5 * 60 * 1000;
const cacheAPI = new Map();

function cacheGet(chave) {
    const item = cacheAPI.get(chave);
    if (!item) return null;
    if (Date.now() - item.ts > CACHE_TTL_MS) { cacheAPI.delete(chave); return null; }
    return item.data;
}

function cacheSet(chave, data) {
    if (cacheAPI.size > 60) cacheAPI.clear();
    cacheAPI.set(chave, { ts: Date.now(), data });
}

// ==========================================
// PERÍODO DINÂMICO (?inicio=AAAA-MM-DD&fim=AAAA-MM-DD)
// ==========================================
// Limite máximo de dias por consulta — evita que alguém peça 10 anos de
// histórico de uma vez e derrube o SQL Server (as queries já são pesadas
// mesmo em janelas de 30-60 dias, por causa dos JOINs e tabelas temporárias).
const PERIODO_MAX_DIAS = 366;

function parsePeriodo(req) {
    const re = /^\d{4}-\d{2}-\d{2}$/;
    const hoje = new Date(); hoje.setHours(0, 0, 0, 0);
    
    // Padrão: Ontem
    let fim = new Date(hoje);
    fim.setDate(fim.getDate() - 1);

    // Padrão: 1º de Agosto do ano atual (mês 7 no JS)
    let inicio = new Date(hoje.getFullYear(), 7, 1);

    if (re.test(String(req.query?.inicio || ''))) {
        const d = new Date(`${req.query.inicio}T00:00:00`);
        if (!Number.isNaN(d.getTime())) inicio = d;
    }
    if (re.test(String(req.query?.fim || ''))) {
        const d = new Date(`${req.query.fim}T00:00:00`);
        if (!Number.isNaN(d.getTime())) fim = d;
    }

    if (inicio > fim) { const t = new Date(inicio); inicio = new Date(fim); fim = t; }

    let limitado = false;
    const diffDias = Math.round((fim - inicio) / (1000 * 60 * 60 * 24));
    if (diffDias > PERIODO_MAX_DIAS) {
        inicio = new Date(fim);
        inicio.setDate(inicio.getDate() - PERIODO_MAX_DIAS);
        limitado = true;
    }

    const fimExclusivo = new Date(fim);
    fimExclusivo.setDate(fimExclusivo.getDate() + 1);

    const fmt = d => `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
    return { inicio, fim, fimExclusivo, inicioStr: fmt(inicio), fimStr: fmt(fim), limitado };
}

// ==========================================
// HELPER DE EXECUÇÃO
// ==========================================
async function executarComRetry(conectarFn, query, nome, timeoutMs = 600000, tentativas = 2, periodo = null) {
    let ultimoErro = null;
    for (let tentativa = 1; tentativa <= tentativas; tentativa++) {
        try {
            const pool = await conectarFn();
            const request = pool.request();
            request.timeout = timeoutMs;

            if (periodo) {
                request.input('dataInicio', sql.Date, periodo.inicio);
                request.input('dataFim', sql.Date, periodo.fimExclusivo);
            }

            const result = await request.query(query);
            return result;
        } catch (erro) {
            ultimoErro = erro;
            // Marca se foi timeout — usado na rota para responder 504 em vez de 500 genérico.
            ultimoErro.foiTimeout = /timeout/i.test(erro.message || '') || erro.code === 'ETIMEOUT';
            console.error(`[${nome}] Tentativa ${tentativa}/${tentativas} falhou: ${erro.message}`);
            if (tentativa < tentativas) {
                console.log(`[${nome}] Aguardando 3s antes de tentar novamente...`);
                await new Promise(resolve => setTimeout(resolve, 3000));
            }
        }
    }
    throw ultimoErro;
}

// ==========================================
// 1. FUNÇÕES DE CONSULTA
// ==========================================

// --- SASCAR ---
async function obterDadosSascar(periodo) {
    const querySascar = `
    SET NOCOUNT ON;


    -- 1. CONSULTA A BASE MASSIVA
    DROP TABLE IF EXISTS #RETORNOS;
    SELECT
        CONVERT(DATE, A.DTAND_AND) AS ANDAMENTO,
        C.CONTR_CON AS CONTRATO,
        C.IDCON_CON,
        'SASCAR' AS CARTEIRA,
        D.NOME_DEV AS DEVEDOR,
        D.CGCPF_DEV AS DOCUMENTO,
        CONCAT(T.DDD_TEL, T.TELEF_TEL) AS TELEFONE,
        CASE 
    WHEN T.DDD_TEL IN ('68', '91', '92', '93', '94', '95', '96', '97') THEN 'NORTE'
    WHEN T.DDD_TEL IN ('71', '73', '74', '75', '77', '79', '81', '82', '83', '84', '85', '86', '87', '88', '89', '98', '99') THEN 'NORDESTE'
    WHEN T.DDD_TEL IN ('61', '62', '63', '64', '65', '66', '67', '69') THEN 'CENTRO-OESTE'
    WHEN T.DDD_TEL IN ('11', '12', '13', '14', '15', '16', '17', '18', '19', '21', '22', '24', '27', '28', '31', '32', '33', '34', '35', '37', '38', '78') THEN 'SUDESTE'
    WHEN T.DDD_TEL IN ('41', '42', '43', '44', '45', '46', '47', '48', '49', '51', '53', '54', '55') THEN 'SUL'
    ELSE 'NÃO ENCONTRADO / INVÁLIDO'
END AS REGIÃO,
        CASE
            WHEN DATEDIFF(YEAR, D.DTNAS_DEV, GETDATE()) < 18 THEN 'Menor de 18'
            WHEN DATEDIFF(YEAR, D.DTNAS_DEV, GETDATE()) BETWEEN 18 AND 25 THEN '18-25 anos'
            WHEN DATEDIFF(YEAR, D.DTNAS_DEV, GETDATE()) BETWEEN 26 AND 35 THEN '26-35 anos'
            WHEN DATEDIFF(YEAR, D.DTNAS_DEV, GETDATE()) BETWEEN 36 AND 45 THEN '36-45 anos'
            WHEN DATEDIFF(YEAR, D.DTNAS_DEV, GETDATE()) BETWEEN 46 AND 55 THEN '46-55 anos'
            WHEN DATEDIFF(YEAR, D.DTNAS_DEV, GETDATE()) BETWEEN 56 AND 65 THEN '56-65 anos'
        ELSE 'Acima de 65' END AS FAIXA_ETARIA,
        CASE WHEN D.TPPES_DEV = 1 THEN 'PJ' ELSE 'PF' END AS TIPO_PESSOA,
        CONCAT(EMA.USUAR_EMA, '@', EMA.DOMIN_EMA, '.', EMA.EXTEN_EMA) AS EMAIL,
        CONVERT(MONEY, C.VLSAL_CON) AS VALOR_TOTAL,
        O.DESCR_OCO AS OCORRENCIA_BASE,
        O.EVENT_OCO AS CANAL,
        O.EVENT_OCO AS EVENTO_OCORRENCIA,
        DATEDIFF(DAY, C.DTATR_CON, GETDATE()) AS DIAS_ATRASO,
        ROW_NUMBER() OVER(PARTITION BY C.CONTR_CON ORDER BY A.DTAND_AND DESC) AS RN,
        ${custoCanalCase('O.EVENT_OCO')} AS CUSTO_TOTAL,
        CASE
            WHEN DATEDIFF(DAY, C.DTATR_CON, GETDATE()) BETWEEN 1 AND 15 THEN '1. 1 A 15 DIAS'
            WHEN DATEDIFF(DAY, C.DTATR_CON, GETDATE()) BETWEEN 16 AND 30 THEN '2. 16 A 30 DIAS'
            WHEN DATEDIFF(DAY, C.DTATR_CON, GETDATE()) BETWEEN 31 AND 60 THEN '3. 31 A 60 DIAS'
            WHEN DATEDIFF(DAY, C.DTATR_CON, GETDATE()) BETWEEN 61 AND 90 THEN '4. 61 A 90 DIAS'
            WHEN DATEDIFF(DAY, C.DTATR_CON, GETDATE()) BETWEEN 91 AND 120 THEN '5. 91 A 120 DIAS'
            WHEN DATEDIFF(DAY, C.DTATR_CON, GETDATE()) BETWEEN 121 AND 150 THEN '6. 121 A 150 DIAS'
            WHEN DATEDIFF(DAY, C.DTATR_CON, GETDATE()) BETWEEN 151 AND 180 THEN '7. 151 A 180 DIAS'
            ELSE '8. > 180'
        END AS FAIXA_ATRASO
    INTO #RETORNOS
    FROM IMAPSNECTAR_DEVEDORES D WITH(NOLOCK)
    INNER JOIN IMAPSNECTAR_CONTRATOS C WITH(NOLOCK) ON D.IDDEV_DEV = C.IDDEV_CON
    INNER JOIN IMAPSNECTAR_OCORRENCIASENVIOS A WITH(NOLOCK) ON C.IDCON_CON = A.IDCON_AND
    INNER JOIN IMAPSNECTAR_OCORRENCIAS O WITH(NOLOCK) ON A.IDOCO_AND = O.IDOCO_OCO
    INNER JOIN IMAPSNECTAR_USUARIOS U WITH(NOLOCK) ON U.IDPES_PES = A.IDPES_AND
    OUTER APPLY (
        SELECT TOP (1) TEMPT.DDD_TEL, TEMPT.TELEF_TEL
        FROM TB_TELEFONE TEMPT WITH(NOLOCK)
        WHERE TEMPT.IDORI_TEL IN (C.IDCON_CON, D.IDDEV_DEV)
        ORDER BY TEMPT.IDTEL_TEL DESC
    ) T
    OUTER APPLY (
        SELECT TOP (1) TEMPE.USUAR_EMA, TEMPE.DOMIN_EMA, TEMPE.EXTEN_EMA
        FROM IMAPSNECTAR_EMAIL TEMPE WITH(NOLOCK)
        WHERE TEMPE.IDORI_EMA = C.IDCON_CON
        ORDER BY TEMPE.IDEMA_EMA DESC
    ) EMA
    WHERE C.IDEMP_CON = 33
    AND U.USUAR_PES NOT IN ('DISCADOR','DISCADOROLOS')
    AND A.DTAND_AND >= @DATAINICIO
    AND A.DTAND_AND < @DATAFIM
    AND O.EVENT_OCO IN ('WHATSAPPMASSIVO', 'SMSMASSIVO', 'EMAILMASSIVO', 'SMSPREVENTIVO', 'EMAILPREVENTIVO');

    -- NOVO: ESTATÍSTICAS DE DISPAROS E INTERAÇÕES POR CLIENTE
    DROP TABLE IF EXISTS #ESTATISTICAS_CLIENTE;
    SELECT 
        R.IDCON_CON,
        COUNT(R.IDCON_CON) AS TOTAL_DISPAROS,
        ISNULL(MAX(I.TOTAL_INTERACOES), 0) AS TOTAL_INTERACOES
    INTO #ESTATISTICAS_CLIENTE
    FROM #RETORNOS R
    LEFT JOIN (
        SELECT A.IDCON_AND, COUNT(*) AS TOTAL_INTERACOES
        FROM IMAPSNECTAR_OCORRENCIASENVIOS A WITH(NOLOCK)
        INNER JOIN IMAPSNECTAR_OCORRENCIAS B WITH(NOLOCK) ON A.IDOCO_AND = B.IDOCO_OCO
        WHERE B.IMAPS_OCO = '1'
        AND A.DTAND_AND >= @DATAINICIO
        AND A.DTAND_AND < @DATAFIM
        GROUP BY A.IDCON_AND
    ) I ON I.IDCON_AND = R.IDCON_CON
    GROUP BY R.IDCON_CON;

    -- 2. RETORNOS
    DROP TABLE IF EXISTS #PROMESSAS;
    SELECT
        R.IDCON_CON,
        CONVERT(DATE, R.ANDAMENTO) AS ANDAMENTO,
        R.CARTEIRA,
        R.DEVEDOR,
        R.DOCUMENTO,
        R.TELEFONE,
        R.REGIÃO,
        R.FAIXA_ETARIA,
        R.TIPO_PESSOA,
        R.EMAIL,
        R.VALOR_TOTAL,
        R.OCORRENCIA_BASE AS OCORRÊNCIA,
        CASE WHEN NEG.IMAPS_OCO IN ('1','2','3') THEN 1 ELSE 0 END AS CPC,
        CASE WHEN NEG.IMAPS_OCO IN ('1','2') THEN 1 ELSE 0 END AS CPCA,
        CASE WHEN NEG.IMAPS_OCO = '1' AND (
        NEG.DESCR_OCO LIKE '%ACORDO%' 
        OR NEG.DESCR_OCO LIKE '%PROMESSA%' 
        OR NEG.DESCR_OCO LIKE '%PAGAMENTO%' 
        OR NEG.DESCR_OCO LIKE '%NEGOCIARAM%'
        )
        AND NEG.DESCR_OCO NOT LIKE '%NÃO NEGOCIARAM%'
        AND NEG.DESCR_OCO NOT LIKE '%SEM NEGOCIAÇÃO%'
        THEN 'NEGOCIOU'
        WHEN NEG.IMAPS_OCO IN ('1','2') THEN 'CPCA'
        WHEN NEG.IMAPS_OCO IN ('1','2','3') THEN 'CPC'
        ELSE ''
         END AS TIPO_NEGOCIAÇÃO,
        R.DIAS_ATRASO,
        R.FAIXA_ATRASO,
        R.CUSTO_TOTAL,
        E.TOTAL_DISPAROS,
        E.TOTAL_INTERACOES
    INTO #PROMESSAS
    FROM #RETORNOS R
    LEFT JOIN #ESTATISTICAS_CLIENTE E ON E.IDCON_CON = R.IDCON_CON
    OUTER APPLY (
        SELECT TOP 1 B.DESCR_OCO, B.IMAPS_OCO
        FROM IMAPSNECTAR_OCORRENCIASENVIOS A WITH(NOLOCK)
        INNER JOIN IMAPSNECTAR_OCORRENCIAS B WITH(NOLOCK) ON A.IDOCO_AND = B.IDOCO_OCO
        WHERE A.IDCON_AND = R.IDCON_CON
        AND B.IMAPS_OCO IN ('1', '2', '3')
        AND A.DTAND_AND >= R.ANDAMENTO
        ORDER BY A.DTAND_AND ASC
    ) NEG
    WHERE RN = 1
    ORDER BY R.ANDAMENTO ASC;
    
    -- ✅ RETORNO DAS AÇÕES
    SELECT * FROM #PROMESSAS ORDER BY ANDAMENTO ASC;

    -- 3. PAGAMENTOS
    WITH CTE_PAGAMENTO AS (
        SELECT
            B.IDCON_CON,
            A.NOME_DEV,
            A.CGCPF_DEV,
            P.VLPAG_PAG AS VALOR_PAGO,
            CONVERT(DATE, P.DTPAG_PAG) AS DATA_PAGAMENTO,
            P.IDPAG_PAG,
            C.IDACO_ACO
        FROM IMAPSNECTAR_DEVEDORES A WITH(NOLOCK)
        INNER JOIN IMAPSNECTAR_CONTRATOS B WITH(NOLOCK) ON A.IDDEV_DEV = B.IDDEV_CON
        INNER JOIN IMAPSNECTAR_ACORDOS C WITH(NOLOCK) ON C.IDCON_ACO = B.IDCON_CON
        LEFT JOIN TB_PAGAMENTO P WITH(NOLOCK) ON P.IDACO_PAG = C.IDACO_ACO
        WHERE P.PAGAM_PAG = 1
        AND P.STPAG_PAG = 1
        AND B.IDEMP_CON = '33'
    )
    SELECT DISTINCT
        R.CARTEIRA,
        CASE
            WHEN O_TIPO.IMAPS_OCO = '1' THEN 'PAGAMENTO DIRETO'
            ELSE 'PAGAMENTO INDIRETO'
        END AS TIPO_PAGAMENTO,
        P.NOME_DEV AS CLIENTE,
        P.CGCPF_DEV AS DOCUMENTO,
        CONVERT(MONEY, P.VALOR_PAGO) AS VALOR_PAGO,
        P.DATA_PAGAMENTO
    FROM CTE_PAGAMENTO P
    INNER JOIN #RETORNOS R ON R.IDCON_CON = P.IDCON_CON
    INNER JOIN #PROMESSAS X ON X.IDCON_CON = R.IDCON_CON
    OUTER APPLY (
        SELECT TOP 1 B.IMAPS_OCO
        FROM IMAPSNECTAR_OCORRENCIASENVIOS A WITH(NOLOCK)
        INNER JOIN IMAPSNECTAR_OCORRENCIAS B WITH(NOLOCK) ON B.IDOCO_OCO = A.IDOCO_AND
        WHERE A.IDCON_AND = P.IDCON_CON
        AND A.DTAND_AND >= R.ANDAMENTO
        AND B.IMAPS_OCO = '1'
        ORDER BY A.DTAND_AND ASC
    ) O_TIPO
    WHERE R.RN = 1
    AND P.DATA_PAGAMENTO >= R.ANDAMENTO
    ORDER BY P.DATA_PAGAMENTO ASC;

    -- 4. QUANTIDADE E CUSTO POR ENVIO DIÁRIO
    SELECT
        CONVERT(DATE, B.DTAND_AND) AS DATA_ENVIO,
        C.EVENT_OCO AS CANAL,
        COUNT(A.IDCON_CON) AS QUANTIDADE_ENVIOS,
        SUM(${custoCanalCase('C.EVENT_OCO')}) AS CUSTO_TOTAL
    FROM IMAPSNECTAR_CONTRATOS A WITH(NOLOCK)
    INNER JOIN IMAPSNECTAR_OCORRENCIASENVIOS B WITH(NOLOCK) ON A.IDCON_CON = B.IDCON_AND
    INNER JOIN IMAPSNECTAR_OCORRENCIAS C WITH(NOLOCK) ON B.IDOCO_AND = C.IDOCO_OCO
    WHERE A.IDEMP_CON = '33'
    AND B.DTAND_AND >= @DATAINICIO
    AND B.DTAND_AND < @DATAFIM
    AND C.EVENT_OCO IN ('WHATSAPPMASSIVO', 'SMSMASSIVO', 'SMSPREVENTIVO', 'EMAILMASSIVO', 'EMAILPREVENTIVO')
    GROUP BY
        CONVERT(DATE, B.DTAND_AND),
        C.EVENT_OCO
    ORDER BY
        DATA_ENVIO ASC,
        CANAL ASC;
    `;

    try {
        const result = await executarComRetry(conectarBD, querySascar, 'SASCAR', 600000, 2, periodo);
        return {
            acoes: result.recordsets[0] || [],
            pagamentos: result.recordsets[1] || [],
            volumetria: result.recordsets[2] || []
        };
    } catch (error) {
        console.error("Erro definitivo ao buscar dados da Sascar:", error);
        throw error;
    }
}

// --- VERISURE ---
async function obterDadosVerisure(periodo) {
    const queryVerisure = `
    
SET NOCOUNT ON;
    DROP TABLE IF EXISTS #RETORNOS;
    DROP TABLE IF EXISTS #PROMESSAS;
    DROP TABLE IF EXISTS #ESTATISTICAS_CLIENTE;
    DROP TABLE IF EXISTS #CRUZAMENTO_FINAL;

    -- 1. RETORNO MASSIVO
    SELECT
      A.DTAND_AND AS ANDAMENTO,
      C.CONTR_CON AS CONTRATO,
      C.CONTA_CON AS CONTA,
      C.IDCON_CON,
      CA.DESCR_CAR AS CARTEIRA,
      D.NOME_DEV AS DEVEDOR,
      D.CGCPF_DEV AS DOCUMENTO,
      CONCAT(T.DDD_TEL, T.TELEF_TEL) AS TELEFONE,
      CASE 
    WHEN T.DDD_TEL IN ('68', '91', '92', '93', '94', '95', '96', '97') THEN 'NORTE'
    WHEN T.DDD_TEL IN ('71', '73', '74', '75', '77', '79', '81', '82', '83', '84', '85', '86', '87', '88', '89', '98', '99') THEN 'NORDESTE'
    WHEN T.DDD_TEL IN ('61', '62', '63', '64', '65', '66', '67', '69') THEN 'CENTRO-OESTE'
    WHEN T.DDD_TEL IN ('11', '12', '13', '14', '15', '16', '17', '18', '19', '21', '22', '24', '27', '28', '31', '32', '33', '34', '35', '37', '38', '78') THEN 'SUDESTE'
    WHEN T.DDD_TEL IN ('41', '42', '43', '44', '45', '46', '47', '48', '49', '51', '53', '54', '55') THEN 'SUL'
    ELSE 'NÃO ENCONTRADO / INVÁLIDO'
END AS REGIÃO,
      CONCAT(EMA.USUAR_EMA, '@', EMA.DOMIN_EMA, '.', EMA.EXTEN_EMA) AS EMAIL,
      CASE
        WHEN DATEDIFF(YEAR, DTNAS_DEV, GETDATE()) < 18 THEN 'Menor de 18'
        WHEN DATEDIFF(YEAR, DTNAS_DEV, GETDATE()) BETWEEN 18 AND 25 THEN '18-25 anos'
        WHEN DATEDIFF(YEAR, DTNAS_DEV, GETDATE()) BETWEEN 26 AND 35 THEN '26-35 anos'
        WHEN DATEDIFF(YEAR, DTNAS_DEV, GETDATE()) BETWEEN 36 AND 45 THEN '36-45 anos'
        WHEN DATEDIFF(YEAR, DTNAS_DEV, GETDATE()) BETWEEN 46 AND 55 THEN '46-55 anos'
        WHEN DATEDIFF(YEAR, DTNAS_DEV, GETDATE()) BETWEEN 56 AND 65 THEN '56-65 anos'
    ELSE 'Acima de 65'  END AS FAIXA_ETARIA,
     CASE WHEN D.TPPES_DEV = 1 THEN 'PJ' ELSE 'PF' END AS TIPO_PESSOA, 
      CONVERT(MONEY, C.VLSAL_CON) AS VALOR_TOTAL,
      O.DESCR_OCO AS OCORRENCIA_BASE,
      O.EVENT_OCO AS CANAL,
      O.EVENT_OCO AS EVENTO_OCORRENCIA,
      DATEDIFF(DAY, C.DTATR_CON, GETDATE()) AS DIAS_ATRASO,
      ROW_NUMBER() OVER (PARTITION BY C.CONTR_CON ORDER BY A.DTAND_AND DESC) AS RN,
      ${custoCanalCase('O.EVENT_OCO')} AS CUSTO_TOTAL,
      CASE
        WHEN DATEDIFF(DAY, C.DTATR_CON, GETDATE()) <= 120 THEN 'A. 0 A 120'
        WHEN DATEDIFF(DAY, C.DTATR_CON, GETDATE()) BETWEEN 121 AND 179 THEN 'B. 121 A 179'
        WHEN DATEDIFF(DAY, C.DTATR_CON, GETDATE()) BETWEEN 180 AND 209 THEN 'C. 180 A 209'
        WHEN DATEDIFF(DAY, C.DTATR_CON, GETDATE()) BETWEEN 210 AND 360 THEN 'D. 210 A 360'
        WHEN DATEDIFF(DAY, C.DTATR_CON, GETDATE()) BETWEEN 361 AND 520 THEN 'E. 361 A 520'
        WHEN DATEDIFF(DAY, C.DTATR_CON, GETDATE()) BETWEEN 521 AND 780 THEN 'F. 521 A 780'
        WHEN DATEDIFF(DAY, C.DTATR_CON, GETDATE()) BETWEEN 781 AND 1000 THEN 'G. 781 A 1000'
        WHEN DATEDIFF(DAY, C.DTATR_CON, GETDATE()) BETWEEN 1001 AND 1500 THEN 'H. 1001 A 1500'
        WHEN DATEDIFF(DAY, C.DTATR_CON, GETDATE()) BETWEEN 1501 AND 2000 THEN 'I. 1501 A 2000'
        WHEN DATEDIFF(DAY, C.DTATR_CON, GETDATE()) BETWEEN 2001 AND 2500 THEN 'K. 2001 A 2500'
        WHEN DATEDIFF(DAY, C.DTATR_CON, GETDATE()) BETWEEN 2501 AND 99999 THEN 'L. > 2500'
        ELSE ''
      END AS FAIXA_ATRASO
    INTO #RETORNOS
    FROM IMAPSNECTAR_DEVEDORES D WITH(NOLOCK)
    INNER JOIN IMAPSNECTAR_CONTRATOS C WITH(NOLOCK) ON D.IDDEV_DEV = C.IDDEV_CON
    INNER JOIN IMAPSNECTAR_OCORRENCIASENVIOS A WITH(NOLOCK) ON C.IDCON_CON = A.IDCON_AND
    INNER JOIN IMAPSNECTAR_OCORRENCIAS O WITH(NOLOCK) ON A.IDOCO_AND = O.IDOCO_OCO
    LEFT JOIN TB_PESSOAL PE WITH(NOLOCK) ON PE.IDPES_PES = C.IDPES_CON
    LEFT JOIN IMAPSNECTAR_EMAIL EMA WITH(NOLOCK) ON EMA.IDORI_EMA = C.IDCON_CON
    INNER JOIN IMAPSNECTAR_CARTEIRAS CA WITH(NOLOCK) ON CA.IDEMP_CAR = CAST(C.IDEMP_CON AS VARCHAR(50))
    INNER JOIN IMAPSNECTAR_USUARIOS U WITH (NOLOCK) ON U.IDPES_PES = A.IDPES_AND
    OUTER APPLY (
      SELECT TOP (1) TEMPT.DDD_TEL, TEMPT.TELEF_TEL
      FROM TB_TELEFONE TEMPT WITH(NOLOCK)
      WHERE TEMPT.IDORI_TEL IN (C.IDCON_CON, D.IDDEV_DEV)
      ORDER BY TEMPT.IDTEL_TEL DESC
    ) T
    WHERE C.IDEMP_CON = 25
      AND U.USUAR_PES NOT IN ('DISCADOR','DISCADOROLOS')
      AND A.DTAND_AND >= @dataInicio
      AND A.DTAND_AND < @dataFim
      AND O.EVENT_OCO IN ('WHATSAPPMASSIVO', 'SMSMASSIVO', 'EMAILMASSIVO', 'SMSPREVENTIVO', 'EMAILPREVENTIVO');

    -- NOVO: ESTATÍSTICAS DE DISPAROS E INTERAÇÕES POR CLIENTE
    DROP TABLE IF EXISTS #ESTATISTICAS_CLIENTE;
    SELECT
      R.IDCON_CON,
      COUNT(R.IDCON_CON) AS TOTAL_DISPAROS,
      ISNULL(MAX(I.TOTAL_INTERACOES), 0) AS TOTAL_INTERACOES
    INTO #ESTATISTICAS_CLIENTE
    FROM #RETORNOS R
    LEFT JOIN (
      SELECT A.IDCON_AND, COUNT(*) AS TOTAL_INTERACOES
      FROM IMAPSNECTAR_OCORRENCIASENVIOS A WITH(NOLOCK)
      INNER JOIN IMAPSNECTAR_OCORRENCIAS B WITH(NOLOCK) ON A.IDOCO_AND = B.IDOCO_OCO
      WHERE B.IMAPS_OCO = '1'
        AND A.DTAND_AND >= @dataInicio
        AND A.DTAND_AND < @dataFim
      GROUP BY A.IDCON_AND
    ) I ON I.IDCON_AND = R.IDCON_CON
    GROUP BY R.IDCON_CON;

    -- 2. SELEÇÃO E CRUZAMENTO (DOCUMENTO ADICIONADO AQUI)
    SELECT
      R.IDCON_CON,
      R.CONTRATO,
      R.CONTA,
      CONVERT(DATE, R.ANDAMENTO) AS ANDAMENTO,
      R.CARTEIRA,
      R.DEVEDOR,
      R.DOCUMENTO,
      R.TELEFONE,
      R.EMAIL,
      R.REGIÃO,
      R.FAIXA_ETARIA,
      R.TIPO_PESSOA,
      R.VALOR_TOTAL,
      R.OCORRENCIA_BASE AS OCORRENCIA,
      R.DIAS_ATRASO,
      R.FAIXA_ATRASO,
      NEG.DATA_NEGOCIACAO,
      CASE WHEN NEG.IMAPS_OCO IN ('1','2','3') THEN 1 ELSE 0 END AS CPC,
      CASE WHEN NEG.IMAPS_OCO IN ('1','2') THEN 1 ELSE 0 END AS CPCA,
      CASE WHEN NEG.IMAPS_OCO = '1' AND (
        NEG.DESCR_OCO LIKE '%ACORDO%'
        OR NEG.DESCR_OCO LIKE '%PROMESSA%'
        OR NEG.DESCR_OCO LIKE '%PAGAMENTO%'
        OR NEG.DESCR_OCO LIKE '%NEGOCIARAM%'
      )
      AND NEG.DESCR_OCO NOT LIKE '%NÃO NEGOCIARAM%'
      AND NEG.DESCR_OCO NOT LIKE '%SEM NEGOCIAÇÃO%'
      THEN 'NEGOCIOU'
      WHEN NEG.IMAPS_OCO IN ('1','2') THEN 'CPCA'
      WHEN NEG.IMAPS_OCO IN ('1','2','3') THEN 'CPC'
      ELSE ''
      END AS TIPO_NEGOCIACAO,
      E.TOTAL_DISPAROS,
      E.TOTAL_INTERACOES
    INTO #PROMESSAS
    FROM #RETORNOS R
    LEFT JOIN #ESTATISTICAS_CLIENTE E ON E.IDCON_CON = R.IDCON_CON
    OUTER APPLY (
      SELECT TOP 1
        B.DESCR_OCO,
        B.IMAPS_OCO,
        A.DTAND_AND AS DATA_NEGOCIACAO
      FROM IMAPSNECTAR_OCORRENCIASENVIOS A WITH(NOLOCK)
      INNER JOIN IMAPSNECTAR_OCORRENCIAS B WITH(NOLOCK) ON A.IDOCO_AND = B.IDOCO_OCO
      WHERE A.IDCON_AND = R.IDCON_CON
        AND B.IMAPS_OCO IN ('1','2','3')
        AND A.DTAND_AND >= R.ANDAMENTO
      ORDER BY A.DTAND_AND ASC
    ) NEG
    WHERE R.RN = 1;

    -- ✅ RETORNO DAS AÇÕES
    SELECT * FROM #PROMESSAS;

    -- 3. PAGAMENTOS DOS QUE NEGOCIARAM
    SELECT 
    P.CARTEIRA,
    P.ANDAMENTO AS DATA_ENVIO,
    V.CODINTCLIE AS CONTA_CONTRATO,
    V.CLIENTE AS CLIENTE_PAGAMENTO,
    P.DEVEDOR,
    P.CONTA,
    SUM (V.VALORPAGAMENTO) AS VALOR_PAGO,
    V.DATAPAGAMENTO AS DATA_PAGAMENTO,
    P.OCORRENCIA,
    CASE
        WHEN V.USUARIO = 'FULLTIME' THEN 'PAGAMENTO DIRETO'
        ELSE 'PAGAMENTO INDIRETO'
    END AS TIPO_PAGAMENTO
INTO #CRUZAMENTO_FINAL
FROM #PROMESSAS P
INNER JOIN [172.25.0.4].[BD_ACOES_MASSIVAS].[DBO].[PAGAMENTO_VERISURE] V WITH (NOLOCK)
    ON V.CODINTCLIE = P.CONTA
GROUP BY 
P.CARTEIRA,
    P.ANDAMENTO,
    V.CODINTCLIE,
    V.CLIENTE,
    P.DEVEDOR,
    P.CONTA,
    V.DATAPAGAMENTO,
    P.OCORRENCIA,
    CASE
        WHEN V.USUARIO = 'FULLTIME' THEN 'PAGAMENTO DIRETO'
        ELSE 'PAGAMENTO INDIRETO'
    END;

    -- ✅ RETORNO DOS PAGAMENTOS
    SELECT * FROM #CRUZAMENTO_FINAL
    ORDER BY DATA_ENVIO ASC;

    -- 4. QUANTIDADE E CUSTO POR ENVIO DIÁRIO
    SELECT
      CONVERT(DATE, B.DTAND_AND) AS DATA_ENVIO,
      C.EVENT_OCO AS CANAL,
      COUNT(A.IDCON_CON) AS QUANTIDADE_ENVIOS,
      SUM(${custoCanalCase('C.EVENT_OCO')}) AS CUSTO_TOTAL
    FROM IMAPSNECTAR_CONTRATOS A WITH(NOLOCK)
    INNER JOIN IMAPSNECTAR_OCORRENCIASENVIOS B WITH(NOLOCK) ON A.IDCON_CON = B.IDCON_AND
    INNER JOIN IMAPSNECTAR_OCORRENCIAS C WITH(NOLOCK) ON B.IDOCO_AND = C.IDOCO_OCO
    WHERE A.IDEMP_CON = 25
      AND B.DTAND_AND >= @dataInicio
      AND B.DTAND_AND < @dataFim
      AND C.EVENT_OCO IN ('WHATSAPPMASSIVO', 'SMSMASSIVO', 'EMAILMASSIVO', 'SMSPREVENTIVO', 'EMAILPREVENTIVO')
    GROUP BY
      CONVERT(DATE, B.DTAND_AND),
      C.EVENT_OCO
    ORDER BY
      DATA_ENVIO ASC,
      CANAL ASC;
    `;

    try {
      const result = await executarComRetry(conectarBD, queryVerisure, 'VERISURE', 600000, 2, periodo);
      return {
        acoes: result.recordsets[0] || [],
        pagamentos: result.recordsets[1] || [],
        disparos: result.recordsets[2] || [],
        volumetria: result.recordsets[2] || []
      };
    } catch (error) {
      console.error("Erro definitivo ao buscar dados da Verisure:", error);
      throw error;
    }
}

// --- SERVIDOR 2 (LINX) ---
async function obterDadosCarteiraServidor2(periodo) {
    const queryCarteira = `
    SET NOCOUNT ON;
    
    DROP TABLE IF EXISTS #RETORNOS;
    DROP TABLE IF EXISTS #ESTATISTICAS_CLIENTE;

    SELECT
        A.DTAND_AND AS ANDAMENTO,
        C.CONTR_CON AS CONTRATO,
        C.IDCON_CON,
        CASE
            WHEN CA.IDEMP_CAR IN ('40', '49') THEN 'Linx'
            ELSE CA.DESCR_CAR
        END AS CARTEIRA,
        D.NOME_DEV AS DEVEDOR,
        D.CGCPF_DEV AS DOCUMENTO,
        CONCAT (T.DDD_TEL, T.TELEF_TEL) AS TELEFONE,
        CASE 
    WHEN T.DDD_TEL IN ('68', '91', '92', '93', '94', '95', '96', '97') THEN 'NORTE'
    WHEN T.DDD_TEL IN ('71', '73', '74', '75', '77', '79', '81', '82', '83', '84', '85', '86', '87', '88', '89', '98', '99') THEN 'NORDESTE'
    WHEN T.DDD_TEL IN ('61', '62', '63', '64', '65', '66', '67', '69') THEN 'CENTRO-OESTE'
    WHEN T.DDD_TEL IN ('11', '12', '13', '14', '15', '16', '17', '18', '19', '21', '22', '24', '27', '28', '31', '32', '33', '34', '35', '37', '38', '78') THEN 'SUDESTE'
    WHEN T.DDD_TEL IN ('41', '42', '43', '44', '45', '46', '47', '48', '49', '51', '53', '54', '55') THEN 'SUL'
    ELSE 'NÃO ENCONTRADO / INVÁLIDO'
END AS REGIÃO,
        CASE
            WHEN DATEDIFF(YEAR, D.DTNAS_DEV, GETDATE()) < 18 THEN 'Menor de 18'
            WHEN DATEDIFF(YEAR, D.DTNAS_DEV, GETDATE()) BETWEEN 18 AND 25 THEN '18-25 anos'
            WHEN DATEDIFF(YEAR, D.DTNAS_DEV, GETDATE()) BETWEEN 26 AND 35 THEN '26-35 anos'
            WHEN DATEDIFF(YEAR, D.DTNAS_DEV, GETDATE()) BETWEEN 36 AND 45 THEN '36-45 anos'
            WHEN DATEDIFF(YEAR, D.DTNAS_DEV, GETDATE()) BETWEEN 46 AND 55 THEN '46-55 anos'
            WHEN DATEDIFF(YEAR, D.DTNAS_DEV, GETDATE()) BETWEEN 56 AND 65 THEN '56-65 anos'
        ELSE 'Acima de 65'  END AS FAIXA_ETARIA,
        CONCAT (EMA.USUAR_EMA, '@', EMA.DOMIN_EMA, '.', EMA.EXTEN_EMA) AS EMAIL,
        CASE WHEN D.TPPES_DEV = 1 THEN 'PJ' ELSE 'PF' END AS TIPO_PESSOA,
        CONVERT (MONEY, C.VLSAL_CON) AS VALOR_TOTAL,
        O.DESCR_OCO AS OCORRENCIA_BASE,
        O.EVENT_OCO AS CANAL,
        O.EVENT_OCO AS EVENTO_OCORRENCIA,
        DATEDIFF (DAY, C.DTATR_CON, GETDATE()) AS DIAS_ATRASO,
        ROW_NUMBER() OVER (PARTITION BY C.CONTR_CON ORDER BY A.DTAND_AND DESC) AS RN,
        ${custoCanalCase('O.EVENT_OCO')} AS CUSTO_TOTAL,
        CASE
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) <= 30 THEN 'A. 0 A 30'
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 31 AND 60 THEN 'B. 31 A 60'
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 61 AND 90 THEN 'C. 61 A 90'
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 91 AND 120 THEN 'D. 91 A 120'
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 121 AND 150 THEN 'E. 121 A 150'
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 151 AND 180 THEN 'F. 151 A 180'
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 181 AND 360 THEN 'G. 181 A 360'
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 361 AND 730 THEN 'H. 361 A 730'
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 731 AND 1095 THEN 'I. 731 A 1095'
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 1096 AND 1460 THEN 'K. 1096 A 1460'
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 1461 AND 1825 THEN 'L. 1461 A 1825'
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) > 1825 THEN 'Q. > 1825'
            ELSE ''
        END AS FAIXA_ATRASO
    INTO #RETORNOS
    FROM IMAPSNECTAR_DEVEDORES D WITH(NOLOCK)
    INNER JOIN IMAPSNECTAR_CONTRATOS C WITH(NOLOCK) ON D.IDDEV_DEV = C.IDDEV_CON
    INNER JOIN IMAPSNECTAR_OCORRENCIASENVIOS A WITH(NOLOCK) ON C.IDCON_CON = A.IDCON_AND
    INNER JOIN IMAPSNECTAR_OCORRENCIAS O WITH(NOLOCK) ON A.IDOCO_AND = O.IDOCO_OCO
    LEFT JOIN TB_PESSOAL PE WITH(NOLOCK) ON PE.IDPES_PES = C.IDPES_CON
    LEFT JOIN IMAPSNECTAR_EMAIL EMA WITH(NOLOCK) ON EMA.IDORI_EMA = C.IDCON_CON
    INNER JOIN IMAPSNECTAR_CARTEIRAS CA WITH(NOLOCK) ON CA.IDEMP_CAR = C.IDEMP_CON 
    CROSS APPLY (
        SELECT TOP (1) TEMPT.DDD_TEL, TEMPT.TELEF_TEL
        FROM TB_TELEFONE TEMPT WITH(NOLOCK)
        WHERE TEMPT.IDORI_TEL IN (C.IDCON_CON, D.IDDEV_DEV)
        ORDER BY TEMPT.IDTEL_TEL DESC
    ) T
    WHERE CA.IDEMP_CAR IN ('40', '49')
    AND A.DTAND_AND >= @dataInicio
    AND A.DTAND_AND < @dataFim
    AND PE.USUAR_PES NOT IN ('DISCADOR','DISCADOROLOS')
    AND O.EVENT_OCO IN ('WHATSAPPMASSIVO', 'SMSMASSIVO', 'EMAILMASSIVO','SMSPREVENTIVO','EMAILPREVENTIVO')
    ORDER BY ANDAMENTO ASC;

    -- NOVO: ESTATÍSTICAS DE DISPAROS E INTERAÇÕES POR CLIENTE
    DROP TABLE IF EXISTS #ESTATISTICAS_CLIENTE;
    SELECT 
        R.IDCON_CON,
        COUNT(R.IDCON_CON) AS TOTAL_DISPAROS,
        ISNULL(MAX(I.TOTAL_INTERACOES), 0) AS TOTAL_INTERACOES
    INTO #ESTATISTICAS_CLIENTE
    FROM #RETORNOS R
    LEFT JOIN (
        SELECT A.IDCON_AND, COUNT(*) AS TOTAL_INTERACOES
        FROM IMAPSNECTAR_OCORRENCIASENVIOS A WITH(NOLOCK)
        INNER JOIN IMAPSNECTAR_OCORRENCIAS B WITH(NOLOCK) ON A.IDOCO_AND = B.IDOCO_OCO
        WHERE B.IMAPS_OCO = '1' 
        AND A.DTAND_AND >= @dataInicio
        AND A.DTAND_AND < @dataFim
        GROUP BY A.IDCON_AND
    ) I ON I.IDCON_AND = R.IDCON_CON
    GROUP BY R.IDCON_CON;

    SELECT
        CONVERT(DATE, R.ANDAMENTO) AS ANDAMENTO,
        R.CARTEIRA,
        R.DEVEDOR,
        R.TELEFONE,
        R.FAIXA_ETARIA,
        R.TIPO_PESSOA,
        R.REGIÃO,
        R.EMAIL,
        R.VALOR_TOTAL,
        R.OCORRENCIA_BASE AS OCORRÊNCIA,
        R.DIAS_ATRASO,
        R.FAIXA_ATRASO,
        CASE WHEN NEG.IMAPS_OCO IN ('1','2','3') THEN 1 ELSE 0 END AS CPC,
        CASE WHEN NEG.IMAPS_OCO IN ('1','2') THEN 1 ELSE 0 END AS CPCA,
        CASE WHEN NEG.IMAPS_OCO = '1' AND (
            NEG.DESCR_OCO LIKE '%ACORDO%'
            OR NEG.DESCR_OCO LIKE '%PROMESSA%'
            OR NEG.DESCR_OCO LIKE '%PAGAMENTO%'
            OR NEG.DESCR_OCO LIKE '%NEGOCIARAM%'
        )
        AND NEG.DESCR_OCO NOT LIKE '%NÃO NEGOCIARAM%'
        AND NEG.DESCR_OCO NOT LIKE '%SEM NEGOCIAÇÃO%'
        THEN 'NEGOCIOU'
        WHEN NEG.IMAPS_OCO IN ('1','2') THEN 'CPCA'
        WHEN NEG.IMAPS_OCO IN ('1','2','3') THEN 'CPC'
        ELSE ''
        END AS TIPO_NEGOCIACAO,
        R.CUSTO_TOTAL,
        E.TOTAL_DISPAROS,
        E.TOTAL_INTERACOES
    FROM #RETORNOS R
    LEFT JOIN #ESTATISTICAS_CLIENTE E ON E.IDCON_CON = R.IDCON_CON
    OUTER APPLY (
        SELECT TOP 1 B.DESCR_OCO, B.IMAPS_OCO
        FROM IMAPSNECTAR_OCORRENCIASENVIOS A WITH(NOLOCK)
        INNER JOIN IMAPSNECTAR_OCORRENCIAS B WITH(NOLOCK) ON A.IDOCO_AND = B.IDOCO_OCO
        WHERE A.IDCON_AND = R.IDCON_CON
        AND B.IMAPS_OCO IN ('1','2','3')
        AND A.DTAND_AND >= CAST(R.ANDAMENTO AS DATE)
        ORDER BY A.DTAND_AND ASC
    ) NEG
    WHERE RN = 1
    ORDER BY R.ANDAMENTO ASC;

    SELECT
        CONVERT (DATE, B.DTAND_AND) AS DATA_ENVIO,
        C.EVENT_OCO AS CANAL,
        COUNT(A.IDCON_CON) AS QUANTIDADE_ENVIOS,
        SUM(${custoCanalCase('C.EVENT_OCO')}) AS CUSTO_TOTAL
    FROM IMAPSNECTAR_CONTRATOS A WITH(NOLOCK)
    INNER JOIN IMAPSNECTAR_OCORRENCIASENVIOS B WITH(NOLOCK) ON A.IDCON_CON = B.IDCON_AND
    INNER JOIN IMAPSNECTAR_OCORRENCIAS C WITH(NOLOCK) ON B.IDOCO_AND = C.IDOCO_OCO
    WHERE A.IDEMP_CON IN (40, 49)
    AND B.DTAND_AND >= @dataInicio
    AND B.DTAND_AND < @dataFim
    AND C.EVENT_OCO IN ('WHATSAPPMASSIVO', 'SMSMASSIVO', 'EMAILMASSIVO','SMSPREVENTIVO','EMAILPREVENTIVO')
    GROUP BY CONVERT(DATE, B.DTAND_AND), C.EVENT_OCO
    ORDER BY DATA_ENVIO ASC, CANAL ASC;
    `;

    try {
        const result = await executarComRetry(conectarBD2, queryCarteira, 'SERVIDOR2/LINX', 600000, 2, periodo);
        return {
            acoes: result.recordsets[0] || [],
            disparos: result.recordsets[1] || []
        };
    } catch (error) {
        console.error("Erro definitivo ao buscar dados do Servidor 2 (Linx):", error);
        throw error;
    }
}

// --- TIM ---
async function obterDadosTIM(periodo) {
    const queryTIM = `
    SET NOCOUNT ON;
    DROP TABLE IF EXISTS #RETORNOS;
    DROP TABLE IF EXISTS #PROMESSAS;
    DROP TABLE IF EXISTS #ESTATISTICAS_CLIENTE;

    -- 1. RETORNO MASSIVO
    SELECT
        A.DTREF_AND AS ANDAMENTO,
        C.CONTR_CON AS CONTRATO,
        C.IDCON_CON,
        CASE WHEN C.IDEMP_CON = 5 THEN 'TIM' END AS CARTEIRA,
        D.NOME_DEV AS DEVEDOR,
        D.CGCPF_DEV AS DOCUMENTO,
        CONCAT(T.DDD_TEL, T.TELEF_TEL) AS TELEFONE,
        CASE 
    WHEN T.DDD_TEL IN ('68', '91', '92', '93', '94', '95', '96', '97') THEN 'NORTE'
    WHEN T.DDD_TEL IN ('71', '73', '74', '75', '77', '79', '81', '82', '83', '84', '85', '86', '87', '88', '89', '98', '99') THEN 'NORDESTE'
    WHEN T.DDD_TEL IN ('61', '62', '63', '64', '65', '66', '67', '69') THEN 'CENTRO-OESTE'
    WHEN T.DDD_TEL IN ('11', '12', '13', '14', '15', '16', '17', '18', '19', '21', '22', '24', '27', '28', '31', '32', '33', '34', '35', '37', '38', '78') THEN 'SUDESTE'
    WHEN T.DDD_TEL IN ('41', '42', '43', '44', '45', '46', '47', '48', '49', '51', '53', '54', '55') THEN 'SUL'
    ELSE 'NÃO ENCONTRADO / INVÁLIDO'
END AS REGIÃO,
        CASE
            WHEN DATEDIFF(YEAR, DTNAS_DEV, GETDATE()) < 18 THEN 'Menor de 18'
            WHEN DATEDIFF(YEAR, DTNAS_DEV, GETDATE()) BETWEEN 18 AND 25 THEN '18-25 anos'
            WHEN DATEDIFF(YEAR, DTNAS_DEV, GETDATE()) BETWEEN 26 AND 35 THEN '26-35 anos'
            WHEN DATEDIFF(YEAR, DTNAS_DEV, GETDATE()) BETWEEN 36 AND 45 THEN '36-45 anos'
            WHEN DATEDIFF(YEAR, DTNAS_DEV, GETDATE()) BETWEEN 46 AND 55 THEN '46-55 anos'
            WHEN DATEDIFF(YEAR, DTNAS_DEV, GETDATE()) BETWEEN 56 AND 65 THEN '56-65 anos'
        ELSE 'Acima de 65'  END AS FAIXA_ETARIA,
        CONCAT(EMA.USUAR_EMA, '@', EMA.DOMIN_EMA, '.', EMA.EXTEN_EMA) AS EMAIL,
        CASE WHEN D.TPPES_DEV = 1 THEN 'PJ' ELSE 'PF' END AS TIPO_PESSOA,
        CONVERT(MONEY, C.VLSAL_CON) AS VALOR_TOTAL,
        O.EVENT_OCO AS CANAL,
        O.DESCR_OCO AS OCORRÊNCIA_BASE,
        O.EVENT_OCO AS EVENTO_OCORRENCIA,
        DATEDIFF(DAY, C.DTATR_CON, GETDATE()) AS DIAS_ATRASO,
        ROW_NUMBER() OVER (PARTITION BY C.CONTR_CON ORDER BY A.DTAND_AND DESC) AS RN,
        ${custoCanalCase('O.EVENT_OCO')} AS CUSTO_TOTAL,
        CASE
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 0 AND 4 THEN 'A. 0 A 4'
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 5 AND 30 THEN 'B. 5 A 30'
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 31 AND 60 THEN 'C. 31 A 60'
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 61 AND 90 THEN 'D. 61 A 90'
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 91 AND 120 THEN 'E. 91 A 120'
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 121 AND 150 THEN 'F. 121 A 150'
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 151 AND 180 THEN 'G. 151 A 180'
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 181 AND 210 THEN 'H. 181 A 210'
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 211 AND 270 THEN 'I. 211 A 270'
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 271 AND 360 THEN 'K. 271 A 360'
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 361 AND 720 THEN 'L. 361 A 720'
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 721 AND 990 THEN 'M. 721 A 990'
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 991 AND 1260 THEN 'N. 991 A 1260'
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 1261 AND 1530 THEN 'O. 1261 A 1530'
            WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 1531 AND 1800 THEN 'P. 1531 A 1800'
            ELSE 'Q. > 1800'
        END AS FAIXA_ATRASO
    INTO #RETORNOS
    FROM IMAPSNECTAR_DEVEDORES D WITH(NOLOCK)
    INNER JOIN IMAPSNECTAR_CONTRATOS C WITH(NOLOCK) ON D.IDDEV_DEV = C.IDDEV_CON
    INNER JOIN IMAPSNECTAR_OCORRENCIASENVIOS A WITH(NOLOCK) ON C.IDCON_CON = A.IDCON_AND
    INNER JOIN IMAPSNECTAR_OCORRENCIAS O WITH(NOLOCK) ON A.IDOCO_AND = O.IDOCO_OCO
    LEFT JOIN TB_PESSOAL PE WITH(NOLOCK) ON PE.IDPES_PES = C.IDPES_CON
    LEFT JOIN IMAPSNECTAR_EMAIL EMA WITH(NOLOCK) ON EMA.IDORI_EMA = C.IDCON_CON
    INNER JOIN IM_AJUSTE J WITH(NOLOCK) ON J.CONTR_AJU = C.CONTR_CON
    INNER JOIN IMAPSNECTAR_USUARIOS U WITH(NOLOCK) ON U.IDPES_PES = A.IDPES_AND
    CROSS APPLY (
        SELECT TOP (1) TEMPT.DDD_TEL, TEMPT.TELEF_TEL
        FROM TB_TELEFONE TEMPT WITH(NOLOCK)
        WHERE TEMPT.IDORI_TEL IN (C.IDCON_CON, D.IDDEV_DEV)
        ORDER BY TEMPT.IDTEL_TEL DESC
    ) T
    WHERE C.IDEMP_CON = 5
    AND U.USUAR_PES NOT IN ('DISCADOR','DISCADOROLOS')
    AND A.DTAND_AND >= @dataInicio
    AND A.DTAND_AND <  @dataFim
    AND O.EVENT_OCO IN ('WHATSAPPMASSIVO', 'SMSMASSIVO', 'EMAILMASSIVO','SMSPREVENTIVO','EMAILPREVENTIVO');

    -- NOVO: ESTATÍSTICAS DE DISPAROS E INTERAÇÕES POR CLIENTE
    DROP TABLE IF EXISTS #ESTATISTICAS_CLIENTE;
    SELECT 
        R.IDCON_CON,
        COUNT(R.IDCON_CON) AS TOTAL_DISPAROS,
        ISNULL(MAX(I.TOTAL_INTERACOES), 0) AS TOTAL_INTERACOES
    INTO #ESTATISTICAS_CLIENTE
    FROM #RETORNOS R
    LEFT JOIN (
        SELECT A.IDCON_AND, COUNT(*) AS TOTAL_INTERACOES
        FROM IMAPSNECTAR_OCORRENCIASENVIOS A WITH(NOLOCK)
        INNER JOIN IMAPSNECTAR_OCORRENCIAS B WITH(NOLOCK) ON A.IDOCO_AND = B.IDOCO_OCO
        WHERE B.IMAPS_OCO = '1'
        AND A.DTAND_AND >= @dataInicio
        AND A.DTAND_AND < @dataFim
        GROUP BY A.IDCON_AND
    ) I ON I.IDCON_AND = R.IDCON_CON
    GROUP BY R.IDCON_CON;

    -- 2. SELEÇÃO E CRUZAMENTO FINAL
    SELECT
        R.IDCON_CON,
        CONVERT (DATE, R.ANDAMENTO) AS ANDAMENTO,
        R.CARTEIRA,
        R.DEVEDOR,
        R.TELEFONE,
        R.FAIXA_ETARIA,
        R.TIPO_PESSOA,
        R.REGIÃO,
        R.EMAIL,
        R.VALOR_TOTAL,
        R.OCORRÊNCIA_BASE AS OCORRÊNCIA,
        R.DIAS_ATRASO,
        R.FAIXA_ATRASO,
        CASE WHEN NEG.IMAPS_OCO IN ('1','2','3') THEN 1 ELSE 0 END AS CPC,
        CASE WHEN NEG.IMAPS_OCO IN ('1','2') THEN 1 ELSE 0 END AS CPCA,
        CASE WHEN NEG.IMAPS_OCO = '1' AND (
            NEG.DESCR_OCO LIKE '%ACORDO%'
            OR NEG.DESCR_OCO LIKE '%PROMESSA%'
            OR NEG.DESCR_OCO LIKE '%PAGAMENTO%'
            OR NEG.DESCR_OCO LIKE '%NEGOCIARAM%'
        )
        AND NEG.DESCR_OCO NOT LIKE '%NÃO NEGOCIARAM%'
        AND NEG.DESCR_OCO NOT LIKE '%SEM NEGOCIAÇÃO%'
        THEN 'NEGOCIOU'
        WHEN NEG.IMAPS_OCO IN ('1','2') THEN 'CPCA'
        WHEN NEG.IMAPS_OCO IN ('1','2','3') THEN 'CPC'
        ELSE ''
        END AS TIPO_NEGOCIAÇÃO,
        E.TOTAL_DISPAROS,
        E.TOTAL_INTERACOES
    INTO #PROMESSAS
    FROM #RETORNOS R
    LEFT JOIN #ESTATISTICAS_CLIENTE E ON E.IDCON_CON = R.IDCON_CON
    OUTER APPLY (
        SELECT TOP 1
            B.DESCR_OCO,
            B.IMAPS_OCO,
            A.DTAND_AND AS DATA_NEGOCIACAO
        FROM IMAPSNECTAR_OCORRENCIASENVIOS A WITH(NOLOCK)
        INNER JOIN IMAPSNECTAR_OCORRENCIAS B WITH(NOLOCK) ON A.IDOCO_AND = B.IDOCO_OCO
        WHERE A.IDCON_AND = R.IDCON_CON
        AND B.IMAPS_OCO IN ('1','2','3')
        AND A.DTAND_AND >= R.ANDAMENTO
        ORDER BY A.DTAND_AND ASC
    ) NEG
    WHERE R.RN = 1
    ORDER BY R.ANDAMENTO ASC;

    -- ✅ RETORNO DAS AÇÕES
    SELECT * FROM #PROMESSAS ORDER BY ANDAMENTO ASC;

    -- 3. PAGAMENTOS
    WITH PAGAMENTO AS (
        SELECT DISTINCT
            B.IDEMP_CON,
            B.IDCON_CON,
            A.NOME_DEV,
            A.CGCPF_DEV,
            C.VLAJU_AJU,
            E.IMAPS_OCO,
            CONVERT(DATE, C.DTAJU_AJU) AS DATA_PAGAMENTO
        FROM IMAPSNECTAR_DEVEDORES A WITH (NOLOCK)
        JOIN IMAPSNECTAR_CONTRATOS B WITH (NOLOCK) ON A.IDDEV_DEV = B.IDDEV_CON
        JOIN IMAPSNECTAR_IM_AJUSTE C WITH (NOLOCK) ON C.CONTR_AJU = B.CONTR_CON
        JOIN iMapsNectar_OcorrenciasEnvios D WITH (NOLOCK) ON D.IDCON_AND = B.IDCON_CON
        JOIN iMapsNectar_Ocorrencias E WITH (NOLOCK) ON E.IDOCO_OCO = D.IDOCO_AND
        WHERE B.IDEMP_CON = 5
        AND C.CHEQU_AJU = 'PG'
        AND CONVERT(DATE, C.DTAJU_AJU) BETWEEN @dataInicio AND @dataFim
        AND CONVERT(DATE, D.DTAND_AND) BETWEEN @dataInicio AND @dataFim
    ) 

    SELECT
        CASE WHEN P.IDEMP_CON = 5 THEN 'TIM' END AS CARTEIRA,
        CASE WHEN O_TIPO.IMAPS_OCO = '1' THEN 'PAGAMENTO DIRETO' ELSE 'PAGAMENTO INDIRETO' END AS TIPO_PAGAMENTO,
        P.NOME_DEV AS CLIENTE,
        P.CGCPF_DEV AS DOCUMENTO,
        CONVERT(MONEY, P.VLAJU_AJU) AS VALOR_PAGO,
        P.DATA_PAGAMENTO
    FROM PAGAMENTO P
    INNER JOIN #RETORNOS R ON R.IDCON_CON = P.IDCON_CON
    INNER JOIN #PROMESSAS X ON X.IDCON_CON = R.IDCON_CON
    OUTER APPLY (
        SELECT TOP 1 B.IMAPS_OCO
        FROM IMAPSNECTAR_OCORRENCIASENVIOS A WITH (NOLOCK)
        INNER JOIN IMAPSNECTAR_OCORRENCIAS B WITH (NOLOCK) ON B.IDOCO_OCO = A.IDOCO_AND
        WHERE A.IDCON_AND = P.IDCON_CON
        AND A.DTAND_AND >= R.ANDAMENTO
        AND A.DTAND_AND <= P.DATA_PAGAMENTO
        AND B.IMAPS_OCO = '1'
        ORDER BY A.DTAND_AND ASC
    ) O_TIPO
    WHERE R.RN = 1
    AND P.DATA_PAGAMENTO >= R.ANDAMENTO
    ORDER BY P.DATA_PAGAMENTO ASC;

    -- 4. QUANTIDADE E CUSTO POR ENVIO DIÁRIO
    SELECT
        CONVERT (DATE, B.DTREF_AND) AS DATA_ENVIO,
        C.EVENT_OCO AS CANAL,
        COUNT(A.IDCON_CON) AS QUANTIDADE_ENVIOS,
        SUM(${custoCanalCase('C.EVENT_OCO')}) AS CUSTO_TOTAL
    FROM IMAPSNECTAR_CONTRATOS A WITH(NOLOCK)
    INNER JOIN IMAPSNECTAR_OCORRENCIASENVIOS B WITH(NOLOCK) ON A.IDCON_CON = B.IDCON_AND
    INNER JOIN IMAPSNECTAR_OCORRENCIAS C WITH(NOLOCK) ON B.IDOCO_AND = C.IDOCO_OCO
    WHERE A.IDEMP_CON = 5
    AND B.DTAND_AND >= @dataInicio
    AND B.DTAND_AND <  @dataFim
    AND C.EVENT_OCO IN ('WHATSAPPMASSIVO', 'SMSMASSIVO', 'EMAILMASSIVO','SMSPREVENTIVO','EMAILPREVENTIVO')
    GROUP BY
        CONVERT(DATE, B.DTREF_AND),
        C.EVENT_OCO
    ORDER BY
        DATA_ENVIO ASC,
        CANAL ASC;
    `;

    try {
        const result = await executarComRetry(conectarBD, queryTIM, 'TIM', 600000, 2, periodo);
        return {
            acoes: result.recordsets[0] || [],
            pagamentos: result.recordsets[1] || [],
            disparos: result.recordsets[2] || [],
            volumetria: result.recordsets[2] || []
        };
    } catch (error) {
        console.error("Erro definitivo ao buscar dados da TIM:", error);
        throw error;
    }
}

// --- SEM PARAR - WHATSAPP ---
async function obterDadosSemPararWhatsapp(periodo) {
    const querySemPararWhatsapp = `
SET NOCOUNT ON;

DROP TABLE IF EXISTS #ENVIOS;
DROP TABLE IF EXISTS #RETORNOS;
DROP TABLE IF EXISTS #ACORDOS;

---------------------------------------------------------------------
-- TEMP 1: BASE DE DISPAROS
---------------------------------------------------------------------
SELECT
    CONVERT(DATE, A.[DATA])              AS DATA_ENVIO,
    UPPER(LTRIM(RTRIM(A.ID_DEVEDOR)))    AS CANAL,
    CASE
        WHEN A.CARTEIRA LIKE '%31%60%'         THEN 'SEM PARAR - AMIGAVEL 31 A 60'
        WHEN A.CARTEIRA LIKE '%61%90%'         THEN 'SEM PARAR - AMIGAVEL 61 A 90'
        WHEN A.CARTEIRA LIKE '%0%30%'          THEN 'SEM PARAR - AMIGAVEL 0 A 30'
        WHEN A.CARTEIRA LIKE '%BSS%'           THEN 'SEM PARAR BSS'
        WHEN A.CARTEIRA = 'TAG CAIXA - B2B'    THEN 'TAG CAIXA - B2B'
        WHEN A.CARTEIRA = 'TAG CAIXA'          THEN 'TAG CAIXA'
        WHEN A.CARTEIRA = 'SEM PARAR RECUPERA' THEN 'APAC'
        ELSE A.CARTEIRA
    END                                  AS CARTEIRA,
    B.CLIENTEID                          AS ID_CLIENTE,
    B.NMNOME                             AS CLIENTE,
    CASE WHEN B.PESSOAID = 1 THEN 'PJ' ELSE 'PF' END AS TIPO_PESSOA,
    A.CELULAR                            AS TELEFONE,
    A.CPF_BIGINT                         AS DOCUMENTO,
    CASE
        WHEN TRY_CAST(
            CASE WHEN LEFT(REPLACE(A.CELULAR,' ',''),2) = '55'
                 THEN SUBSTRING(REPLACE(A.CELULAR,' ',''),3,2)
                 ELSE LEFT(REPLACE(A.CELULAR,' ',''),2)
            END AS BIGINT) IN (11,12,13,14,15,16,17,18,19,21,22,24,27,28,31,32,33,34,35,37,38) THEN 'SUDESTE'
        WHEN TRY_CAST(
            CASE WHEN LEFT(REPLACE(A.CELULAR,' ',''),2) = '55'
                 THEN SUBSTRING(REPLACE(A.CELULAR,' ',''),3,2)
                 ELSE LEFT(REPLACE(A.CELULAR,' ',''),2)
            END AS BIGINT) IN (41,42,43,44,45,46,47,48,49,51,53,54,55) THEN 'SUL'
        WHEN TRY_CAST(
            CASE WHEN LEFT(REPLACE(A.CELULAR,' ',''),2) = '55'
                 THEN SUBSTRING(REPLACE(A.CELULAR,' ',''),3,2)
                 ELSE LEFT(REPLACE(A.CELULAR,' ',''),2)
            END AS BIGINT) IN (61,62,64,65,66,67) THEN 'CENTRO-OESTE'
        WHEN TRY_CAST(
            CASE WHEN LEFT(REPLACE(A.CELULAR,' ',''),2) = '55'
                 THEN SUBSTRING(REPLACE(A.CELULAR,' ',''),3,2)
                 ELSE LEFT(REPLACE(A.CELULAR,' ',''),2)
            END AS BIGINT) IN (63,68,69,91,92,93,94,95,96,97) THEN 'NORTE'
        WHEN TRY_CAST(
            CASE WHEN LEFT(REPLACE(A.CELULAR,' ',''),2) = '55'
                 THEN SUBSTRING(REPLACE(A.CELULAR,' ',''),3,2)
                 ELSE LEFT(REPLACE(A.CELULAR,' ',''),2)
            END AS BIGINT) IN (71,73,74,75,77,79,81,82,83,84,85,86,87,88,89,98,99) THEN 'NORDESTE'
        ELSE 'NAO MAPEADO'
    END                                  AS REGIAO,
    CASE
        WHEN DATEDIFF(YEAR, B.DTNASCIMENTO, GETDATE()) IS NULL THEN '00. DESCONHECIDO'
        WHEN DATEDIFF(YEAR, B.DTNASCIMENTO, GETDATE()) < 25    THEN '01. ATE 24 ANOS'
        WHEN DATEDIFF(YEAR, B.DTNASCIMENTO, GETDATE()) BETWEEN 25  AND 35  THEN '02. 25 A 35 ANOS'
        WHEN DATEDIFF(YEAR, B.DTNASCIMENTO, GETDATE()) BETWEEN 36  AND 45  THEN '03. 36 A 45 ANOS'
        WHEN DATEDIFF(YEAR, B.DTNASCIMENTO, GETDATE()) BETWEEN 46  AND 60  THEN '04. 46 A 60 ANOS'
        ELSE '05. ACIMA DE 60 ANOS'
    END                                  AS FAIXA_ETARIA,
    DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) AS DIAS_ATRASO,
    CASE
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 0    AND 4    THEN '01. 0 A 4'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 5    AND 30   THEN '02. 5 A 30'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 31   AND 60   THEN '03. 31 A 60'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 61   AND 90   THEN '04. 61 A 90'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 91   AND 120  THEN '05. 91 A 120'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 121  AND 150  THEN '06. 121 A 150'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 151  AND 180  THEN '07. 151 A 180'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 181  AND 210  THEN '08. 181 A 210'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 211  AND 270  THEN '09. 211 A 270'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 271  AND 360  THEN '10. 271 A 360'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 361  AND 720  THEN '11. 361 A 720'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 721  AND 990  THEN '12. 721 A 990'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 991  AND 1260 THEN '13. 991 A 1260'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 1261 AND 1530 THEN '14. 1261 A 1530'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 1531 AND 1800 THEN '15. 1531 A 1800'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) > 1800                THEN '16. > 1800'
    END                                  AS FAIXA_ATRASO
INTO #ENVIOS
FROM TB_PLAN_ACOES_SMS A WITH (NOLOCK)
JOIN [172.25.0.15].[FULLLOAD].[DBO].[TB_CLIENTE] B WITH (NOLOCK)
    ON A.CPF_BIGINT = B.NUCPFCNPJ
WHERE A.[DATA] >= @dataInicio
  AND A.[DATA] <  @dataFim
  AND UPPER(LTRIM(RTRIM(A.ID_DEVEDOR))) = 'WHATSAPP';

---------------------------------------------------------------------
-- TEMP 2: RETORNOS WHATSAPP
---------------------------------------------------------------------
SELECT
    B.CARTEIRA,
    B.CANAL,
    B.DATA_ENVIO,
    CAST(A.[DATA] AS DATE)               AS DATA_RETORNO,
    B.ID_CLIENTE,
    B.CLIENTE,
    A.[IDENTIFICAÇÃO]                    AS DOCUMENTO,
    B.TELEFONE,
    B.REGIAO,
    B.FAIXA_ETARIA,
    B.TIPO_PESSOA,
    B.DIAS_ATRASO,
    B.FAIXA_ATRASO,
    CASE
        WHEN A.[ARQUIVO ORIGINAL] IS NOT NULL THEN 'CHATBOT'
        ELSE 'RECEPTIVO'
    END                                  AS ORIGEM_ATENDIMENTO
INTO #RETORNOS
FROM [BD_MIS].[DBO].[WHATSAPP_PANEAS] A WITH (NOLOCK)
JOIN #ENVIOS B WITH (NOLOCK)
    ON  A.[IDENTIFICAÇÃO] = B.DOCUMENTO
WHERE A.[DATA] >= @dataInicio
  AND A.[DATA] <  @dataFim
  AND A.[DATA] >= B.DATA_ENVIO
  AND A.[FLUXO OUT] = 'COMPLETO';

---------------------------------------------------------------------
-- TEMP 3: ACORDOS
---------------------------------------------------------------------
SELECT DISTINCT
    R.CARTEIRA,
    R.DATA_ENVIO,
    R.DATA_RETORNO,
    R.CANAL,
    R.ID_CLIENTE,
    UPPER(R.CLIENTE)                              AS CLIENTE,
    R.DOCUMENTO,
    R.TELEFONE,
    R.REGIAO,
    R.FAIXA_ETARIA,
    R.TIPO_PESSOA,
    R.DIAS_ATRASO,
    R.FAIXA_ATRASO,
    R.ORIGEM_ATENDIMENTO,
    CAST(A.DTACORDO AS DATE)                      AS DATA_ACORDO,
    CAST(A.VALORTOTALACORDO AS DECIMAL(18,2))     AS VALOR_ACORDO,
    CAST(A.VALORTOTALACORDO AS DECIMAL(18,2))     AS VALOR_TOTAL
INTO #ACORDOS
FROM #RETORNOS R WITH (NOLOCK)
JOIN [172.25.0.15].[FULLLOAD].[DBO].[TB_ACORDOS] A WITH (NOLOCK)
    ON  A.CLIENTEID = R.ID_CLIENTE
    AND CAST(A.DTACORDO AS DATE) >= R.DATA_ENVIO
    AND CAST(A.DTACORDO AS DATE) <= DATEADD(DAY, 5, R.DATA_ENVIO)
WHERE A.ORIGEMACORDO NOT IN ('1');

---------------------------------------------------------------------
-- ★ RESULT SET 1 → json.acoes
---------------------------------------------------------------------
SELECT
    CARTEIRA,
    DATA_ENVIO,
    DATA_RETORNO,
    CANAL,
    CLIENTE AS DEVEDOR,
    ID_CLIENTE,
    DOCUMENTO,
    TELEFONE,
    REGIAO,
    FAIXA_ETARIA,
    TIPO_PESSOA,
    DIAS_ATRASO,
    FAIXA_ATRASO,
    ORIGEM_ATENDIMENTO,
    DATA_ACORDO,
    VALOR_ACORDO,
    VALOR_TOTAL
FROM #ACORDOS
ORDER BY DATA_ENVIO ASC, CANAL ASC;

---------------------------------------------------------------------
-- ★ RESULT SET 2 → json.disparos
---------------------------------------------------------------------
SELECT
    CAST([DATA] AS DATE)                AS DATA_ENVIO,
    UPPER(LTRIM(RTRIM(ID_DEVEDOR)))     AS CANAL,
    CASE
        WHEN CARTEIRA LIKE '%31%60%'         THEN 'SEM PARAR - AMIGAVEL 31 A 60'
        WHEN CARTEIRA LIKE '%61%90%'         THEN 'SEM PARAR - AMIGAVEL 61 A 90'
        WHEN CARTEIRA LIKE '%0%30%'          THEN 'SEM PARAR - AMIGAVEL 0 A 30'
        WHEN CARTEIRA LIKE '%BSS%'           THEN 'SEM PARAR BSS'
        WHEN CARTEIRA = 'TAG CAIXA - B2B'    THEN 'TAG CAIXA - B2B'
        WHEN CARTEIRA = 'TAG CAIXA'          THEN 'TAG CAIXA'
        WHEN CARTEIRA = 'SEM PARAR RECUPERA' THEN 'APAC'
        ELSE CARTEIRA
    END                                 AS CARTEIRA,
    COUNT(*)                            AS TOTAL_DISPAROS,
    CAST(SUM(0.100) AS DECIMAL(18,2))  AS CUSTO_TOTAL
FROM TB_PLAN_ACOES_SMS WITH (NOLOCK)
WHERE [DATA] >= @dataInicio
  AND [DATA] <  @dataFim
  AND UPPER(LTRIM(RTRIM(ID_DEVEDOR))) = 'WHATSAPP'
GROUP BY
    CAST([DATA] AS DATE),
    UPPER(LTRIM(RTRIM(ID_DEVEDOR))),
    CASE
        WHEN CARTEIRA LIKE '%31%60%'         THEN 'SEM PARAR - AMIGAVEL 31 A 60'
        WHEN CARTEIRA LIKE '%61%90%'         THEN 'SEM PARAR - AMIGAVEL 61 A 90'
        WHEN CARTEIRA LIKE '%0%30%'          THEN 'SEM PARAR - AMIGAVEL 0 A 30'
        WHEN CARTEIRA LIKE '%BSS%'           THEN 'SEM PARAR BSS'
        WHEN CARTEIRA = 'TAG CAIXA - B2B'    THEN 'TAG CAIXA - B2B'
        WHEN CARTEIRA = 'TAG CAIXA'          THEN 'TAG CAIXA'
        WHEN CARTEIRA = 'SEM PARAR RECUPERA' THEN 'APAC'
        ELSE CARTEIRA
    END
ORDER BY DATA_ENVIO ASC;

---------------------------------------------------------------------
-- ★ RESULT SET 3 → json.pagamentos
---------------------------------------------------------------------
SELECT
    A.CARTEIRA,
    A.DATA_ENVIO,
    A.CANAL,
    A.CLIENTE           AS DEVEDOR,
    A.DOCUMENTO,
    A.TELEFONE,
    A.REGIAO,
    A.TIPO_PESSOA,
    A.FAIXA_ETARIA,
    A.DIAS_ATRASO,
    A.FAIXA_ATRASO,
    A.ORIGEM_ATENDIMENTO,
    A.DATA_ACORDO,
    A.VALOR_ACORDO,
    CAST(P.DTPAGAMENTO AS DATE)          AS DATA_PAGAMENTO,
    CAST(P.NUVALORPAGO AS DECIMAL(18,2)) AS VALOR_PAGO,
    CASE
        WHEN P.ORIGEMPAGID = '1' THEN 'PAGAMENTO DIRETO'
        ELSE 'PAGAMENTO INDIRETO'
    END                                  AS TIPO_PAGAMENTO
FROM #ACORDOS A WITH (NOLOCK)
JOIN [172.25.0.15].[FULLLOAD].[DBO].[TB_PAGAMENTOS] P WITH (NOLOCK)
    ON  P.CLIENTEID = A.ID_CLIENTE
    AND P.DTPAGAMENTO >= A.DATA_ACORDO
ORDER BY A.DATA_ENVIO ASC, P.DTPAGAMENTO ASC;

---------------------------------------------------------------------
-- ★ RESULT SET 4 → json.retornos
---------------------------------------------------------------------
-- Todo contato/resposta do cliente no WhatsApp dentro do período (#RETORNOS
-- completo), independente de ter virado acordo ou não. Diferente de
-- json.acoes (que só traz o subconjunto de #RETORNOS que casou com um
-- acordo em TB_ACORDOS em até 5 dias). É por isso que um dia sem disparo
-- pode aparecer com "retorno": o cliente recebeu a mensagem em um dia
-- anterior e só respondeu (DATA_RETORNO) dentro do período consultado.
SELECT
    CARTEIRA,
    CANAL,
    DATA_ENVIO,
    DATA_RETORNO,
    ID_CLIENTE,
    CLIENTE           AS DEVEDOR,
    DOCUMENTO,
    TELEFONE,
    REGIAO,
    FAIXA_ETARIA,
    TIPO_PESSOA,
    DIAS_ATRASO,
    FAIXA_ATRASO,
    ORIGEM_ATENDIMENTO
FROM #RETORNOS
ORDER BY DATA_RETORNO ASC;

---------------------------------------------------------------------
DROP TABLE IF EXISTS #ENVIOS;
DROP TABLE IF EXISTS #RETORNOS;
DROP TABLE IF EXISTS #ACORDOS;
`;

    try {
        const result = await executarComRetry(
            conectarBD4,
            querySemPararWhatsapp,
            'SEM PARAR - WHATSAPP',
            600000,
            2,
            periodo
        );

        // ✅ MAPEAMENTO CORRIGIDO: segue a ordem dos SELECTs na query
        const acoes       = result.recordsets?.[0] || [];
        const disparos    = result.recordsets?.[1] || [];   // ← ERA pagamentos
        const pagamentos  = result.recordsets?.[2] || [];   // ← ERA disparos
        // RESULT SET 4: todos os retornos (contatos WhatsApp), diretos ou não de acordo.
        const retornos    = result.recordsets?.[3] || [];

        return { acoes, pagamentos, disparos, retornos, volumetria: disparos };
    } catch (error) {
        console.error("Erro definitivo ao buscar dados da Sem Parar - WhatsApp:", error);
        throw error;
    }
}

// --- SEM PARAR - EMAIL ---
async function obterDadosSemPararEmail(periodo) {
    const querySemPararEmail = `
SET NOCOUNT ON;

DROP TABLE IF EXISTS #ENVIOS;
DROP TABLE IF EXISTS #ACORDOS;

---------------------------------------------------------------------
-- TEMP 1: BASE DE DISPAROS EMAIL
---------------------------------------------------------------------
SELECT
    CONVERT(DATE, A.[DATA])              AS DATA_ENVIO,
    UPPER(LTRIM(RTRIM(A.ID_DEVEDOR)))    AS CANAL,
    CASE
        WHEN A.CARTEIRA LIKE '%31%60%'       THEN 'SEM PARAR - AMIGAVEL 31 A 60'
        WHEN A.CARTEIRA LIKE '%61%90%'       THEN 'SEM PARAR - AMIGAVEL 61 A 90'
        WHEN A.CARTEIRA LIKE '%0%30%'        THEN 'SEM PARAR - AMIGAVEL 0 A 30'
        WHEN A.CARTEIRA LIKE '%BSS%'         THEN 'SEM PARAR BSS'
        WHEN A.CARTEIRA = 'TAG CAIXA - B2B'  THEN 'TAG CAIXA - B2B'
        WHEN A.CARTEIRA = 'TAG CAIXA'        THEN 'TAG CAIXA'
        WHEN A.CARTEIRA = 'SEM PARAR RECUPERA' THEN 'APAC'
        ELSE A.CARTEIRA
    END                                  AS CARTEIRA,
    B.CLIENTEID                          AS ID_CLIENTE,
    B.NMNOME                             AS CLIENTE,
    CASE WHEN B.PESSOAID = 1 THEN 'PJ' ELSE 'PF' END AS TIPO_PESSOA,
    A.CPF_BIGINT                         AS DOCUMENTO,
    C.NMEMAIL                            AS EMAIL,
    CASE
        WHEN DATEDIFF(YEAR, B.DTNASCIMENTO, GETDATE()) IS NULL THEN '00. DESCONHECIDO'
        WHEN DATEDIFF(YEAR, B.DTNASCIMENTO, GETDATE()) < 25   THEN '01. ATE 24 ANOS'
        WHEN DATEDIFF(YEAR, B.DTNASCIMENTO, GETDATE()) BETWEEN 25 AND 35  THEN '02. 25 A 35 ANOS'
        WHEN DATEDIFF(YEAR, B.DTNASCIMENTO, GETDATE()) BETWEEN 36 AND 45  THEN '03. 36 A 45 ANOS'
        WHEN DATEDIFF(YEAR, B.DTNASCIMENTO, GETDATE()) BETWEEN 46 AND 60  THEN '04. 46 A 60 ANOS'
        ELSE '05. ACIMA DE 60 ANOS'
    END                                  AS FAIXA_ETARIA,
    DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) AS DIAS_ATRASO,
    CASE
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 0 AND 4      THEN '01. 0 A 4'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 5 AND 30     THEN '02. 5 A 30'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 31 AND 60    THEN '03. 31 A 60'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 61 AND 90    THEN '04. 61 A 90'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 91 AND 120   THEN '05. 91 A 120'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 121 AND 150  THEN '06. 121 A 150'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 151 AND 180  THEN '07. 151 A 180'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 181 AND 210  THEN '08. 181 A 210'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 211 AND 270  THEN '09. 211 A 270'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 271 AND 360  THEN '10. 271 A 360'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 361 AND 720  THEN '11. 361 A 720'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 721 AND 990  THEN '12. 721 A 990'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 991 AND 1260 THEN '13. 991 A 1260'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 1261 AND 1530 THEN '14. 1261 A 1530'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) BETWEEN 1531 AND 1800 THEN '15. 1531 A 1800'
        WHEN DATEDIFF(DAY, TRY_CONVERT(DATE, A.PRAZO_DO_CLIENTE, 103), CONVERT(DATE, A.[DATA])) > 1800 THEN '16. > 1800'
    END                                  AS FAIXA_ATRASO
INTO #ENVIOS
FROM TB_PLAN_ACOES_SMS A WITH (NOLOCK)
JOIN [172.25.0.15].[FULLLOAD].[DBO].[TB_CLIENTE] B WITH (NOLOCK)
    ON A.CPF_BIGINT = B.NUCPFCNPJ
JOIN (
    SELECT CLIENTEID, NMEMAIL,
           ROW_NUMBER() OVER (PARTITION BY CLIENTEID ORDER BY DTINCLUSAO DESC) AS RN
    FROM [172.25.0.15].[FULLLOAD].[DBO].[TB_CLIENTEEMAIL] WITH (NOLOCK)
) C ON B.CLIENTEID = C.CLIENTEID AND C.RN = 1
WHERE A.[DATA] >= @dataInicio
  AND A.[DATA] <  @dataFim
  AND UPPER(LTRIM(RTRIM(A.ID_DEVEDOR))) = 'EMAIL';

---------------------------------------------------------------------
-- TEMP 2: ACORDOS (direto de #ENVIOS, sem #RETORNOS)
---------------------------------------------------------------------
SELECT DISTINCT
    B.CARTEIRA,
    B.DATA_ENVIO,
    B.CANAL,
    B.ID_CLIENTE,
    UPPER(B.CLIENTE)                     AS CLIENTE,
    B.DOCUMENTO,
    B.EMAIL,
    B.TIPO_PESSOA,
    B.FAIXA_ETARIA,
    B.DIAS_ATRASO,
    B.FAIXA_ATRASO,
    CAST(A.DTACORDO AS DATE)             AS DATA_ACORDO,
    CAST(A.DTACORDO AS DATE)             AS DATA_RETORNO,
    CAST(A.VALORTOTALACORDO AS DECIMAL(18,2)) AS VALOR_ACORDO,
    CAST(A.VALORTOTALACORDO AS DECIMAL(18,2)) AS VALOR_TOTAL,
    CONCAT('ESTIMADO VIA EMAIL (D+',
           DATEDIFF(DAY, B.DATA_ENVIO, CAST(A.DTACORDO AS DATE)), ')')
                                         AS ORIGEM_ATENDIMENTO
INTO #ACORDOS
FROM #ENVIOS B
JOIN [172.25.0.15].[FULLLOAD].[DBO].[TB_ACORDOS] A WITH (NOLOCK)
    ON  A.CLIENTEID = B.ID_CLIENTE
    AND CAST(A.DTACORDO AS DATE) >= B.DATA_ENVIO
    AND CAST(A.DTACORDO AS DATE) <= DATEADD(DAY, 5, B.DATA_ENVIO)
WHERE A.ORIGEMACORDO NOT IN ('1');

---------------------------------------------------------------------
-- ★ RESULT SET 1 → json.acoes
---------------------------------------------------------------------
SELECT
    CARTEIRA,
    DATA_ENVIO,
    DATA_RETORNO,
    CANAL,
    CLIENTE            AS DEVEDOR,
    ID_CLIENTE,
    EMAIL,
    DOCUMENTO,
    FAIXA_ETARIA,
    TIPO_PESSOA,
    DIAS_ATRASO,
    FAIXA_ATRASO,
    ORIGEM_ATENDIMENTO,
    DATA_ACORDO,
    VALOR_ACORDO,
    VALOR_TOTAL
FROM #ACORDOS
ORDER BY DATA_ENVIO ASC, CANAL ASC;

---------------------------------------------------------------------
-- ★ RESULT SET 2 → json.disparos
---------------------------------------------------------------------
SELECT
    CAST([DATA] AS DATE)                AS DATA_ENVIO,
    UPPER(LTRIM(RTRIM(ID_DEVEDOR)))     AS CANAL,
    CASE
        WHEN CARTEIRA LIKE '%31%60%'       THEN 'SEM PARAR - AMIGAVEL 31 A 60'
        WHEN CARTEIRA LIKE '%61%90%'       THEN 'SEM PARAR - AMIGAVEL 61 A 90'
        WHEN CARTEIRA LIKE '%0%30%'        THEN 'SEM PARAR - AMIGAVEL 0 A 30'
        WHEN CARTEIRA LIKE '%BSS%'         THEN 'SEM PARAR BSS'
        WHEN CARTEIRA = 'TAG CAIXA - B2B'  THEN 'TAG CAIXA - B2B'
        WHEN CARTEIRA = 'TAG CAIXA'        THEN 'TAG CAIXA'
        WHEN CARTEIRA = 'SEM PARAR RECUPERA' THEN 'APAC'
        ELSE CARTEIRA
    END                                 AS CARTEIRA,
    COUNT(*)                            AS TOTAL_DISPAROS,
    CAST(SUM(0.005) AS DECIMAL(18,2))  AS CUSTO_TOTAL
FROM TB_PLAN_ACOES_SMS WITH (NOLOCK)
WHERE [DATA] >= @dataInicio
  AND [DATA] <  @dataFim
  AND UPPER(LTRIM(RTRIM(ID_DEVEDOR))) = 'EMAIL'
GROUP BY
    CAST([DATA] AS DATE),
    UPPER(LTRIM(RTRIM(ID_DEVEDOR))),
    CASE
        WHEN CARTEIRA LIKE '%31%60%'       THEN 'SEM PARAR - AMIGAVEL 31 A 60'
        WHEN CARTEIRA LIKE '%61%90%'       THEN 'SEM PARAR - AMIGAVEL 61 A 90'
        WHEN CARTEIRA LIKE '%0%30%'        THEN 'SEM PARAR - AMIGAVEL 0 A 30'
        WHEN CARTEIRA LIKE '%BSS%'         THEN 'SEM PARAR BSS'
        WHEN CARTEIRA = 'TAG CAIXA - B2B'  THEN 'TAG CAIXA - B2B'
        WHEN CARTEIRA = 'TAG CAIXA'        THEN 'TAG CAIXA'
        WHEN CARTEIRA = 'SEM PARAR RECUPERA' THEN 'APAC'
        ELSE CARTEIRA
    END
ORDER BY DATA_ENVIO ASC;

---------------------------------------------------------------------
-- ★ RESULT SET 3 → json.pagamentos
---------------------------------------------------------------------
SELECT
    A.CARTEIRA,
    A.DATA_ENVIO,
    A.CANAL,
    A.CLIENTE           AS DEVEDOR,
    A.DOCUMENTO,
    A.EMAIL,
    A.TIPO_PESSOA,
    A.FAIXA_ETARIA,
    A.DIAS_ATRASO,
    A.FAIXA_ATRASO,
    A.ORIGEM_ATENDIMENTO,
    A.DATA_ACORDO,
    A.VALOR_ACORDO,
    CAST(P.DTPAGAMENTO AS DATE)          AS DATA_PAGAMENTO,
    CAST(P.NUVALORPAGO AS DECIMAL(18,2)) AS VALOR_PAGO,
    CASE
        WHEN P.ORIGEMPAGID = '1' THEN 'PAGAMENTO DIRETO'
        ELSE 'PAGAMENTO INDIRETO'
    END                                  AS TIPO_PAGAMENTO
FROM #ACORDOS A WITH (NOLOCK)
JOIN [172.25.0.15].[FULLLOAD].[DBO].[TB_PAGAMENTOS] P WITH (NOLOCK)
    ON  P.CLIENTEID = A.ID_CLIENTE
    AND P.DTPAGAMENTO >= A.DATA_ACORDO
ORDER BY A.DATA_ENVIO ASC, P.DTPAGAMENTO ASC;

---------------------------------------------------------------------
DROP TABLE IF EXISTS #ENVIOS;
DROP TABLE IF EXISTS #ACORDOS;
`;

    try {
        const result = await executarComRetry(
            conectarBD4,
            querySemPararEmail,
            'SEM PARAR - EMAIL',
            600000,
            2,
            periodo
        );

        // ✅ MAPEAMENTO CORRIGIDO
        const acoes       = result.recordsets?.[0] || [];
        const disparos    = result.recordsets?.[1] || [];
        const pagamentos  = result.recordsets?.[2] || [];

        return { acoes, pagamentos, disparos, volumetria: disparos };
    } catch (error) {
        console.error("Erro definitivo ao buscar dados da Sem Parar - Email:", error);
        throw error;
    }
}

// --- VIVO ---
async function obterDadosVivo(periodo) {
    const queryVivo = `
    
SET NOCOUNT ON;

DROP TABLE IF EXISTS #RETORNOS;
DROP TABLE IF EXISTS #ESTATISTICAS_CLIENTE;
DROP TABLE IF EXISTS #PAGAMENTOS;

-- 1. BASE MASSIVA - VIVO
SELECT
    A.DTAND_AND AS ANDAMENTO,
    C.CONTR_CON AS CONTRATO,
    C.IDCON_CON,
    CA.DESCR_CAR AS CARTEIRA,
    D.NOME_DEV AS DEVEDOR,
    D.CGCPF_DEV AS DOCUMENTO,
    CONCAT(T.DDD_TEL, T.TELEF_TEL) AS TELEFONE,
    CASE 
        WHEN T.DDD_TEL IN ('68', '91', '92', '93', '94', '95', '96', '97') THEN 'NORTE'
        WHEN T.DDD_TEL IN ('71', '73', '74', '75', '77', '79', '81', '82', '83', '84', '85', '86', '87', '88', '89', '98', '99') THEN 'NORDESTE'
        WHEN T.DDD_TEL IN ('61', '62', '63', '64', '65', '66', '67', '69') THEN 'CENTRO-OESTE'
        WHEN T.DDD_TEL IN ('11', '12', '13', '14', '15', '16', '17', '18', '19', '21', '22', '24', '27', '28', '31', '32', '33', '34', '35', '37', '38', '78') THEN 'SUDESTE'
        WHEN T.DDD_TEL IN ('41', '42', '43', '44', '45', '46', '47', '48', '49', '51', '53', '54', '55') THEN 'SUL'
        ELSE 'NÃO ENCONTRADO / INVÁLIDO'
    END AS REGIÃO,
    CASE
        WHEN DATEDIFF(YEAR, DTNAS_DEV, GETDATE()) < 18 THEN 'Menor de 18'
        WHEN DATEDIFF(YEAR, DTNAS_DEV, GETDATE()) BETWEEN 18 AND 25 THEN '18-25 anos'
        WHEN DATEDIFF(YEAR, DTNAS_DEV, GETDATE()) BETWEEN 26 AND 35 THEN '26-35 anos'
        WHEN DATEDIFF(YEAR, DTNAS_DEV, GETDATE()) BETWEEN 36 AND 45 THEN '36-45 anos'
        WHEN DATEDIFF(YEAR, DTNAS_DEV, GETDATE()) BETWEEN 46 AND 55 THEN '46-55 anos'
        WHEN DATEDIFF(YEAR, DTNAS_DEV, GETDATE()) BETWEEN 56 AND 65 THEN '56-65 anos'
        ELSE 'Acima de 65'  
    END AS FAIXA_ETARIA,
    CASE
        WHEN EMA.IDORI_EMA IS NULL THEN NULL
        ELSE CONCAT(EMA.USUAR_EMA, '@', EMA.DOMIN_EMA, '.', EMA.EXTEN_EMA) 
    END AS EMAIL,
    CASE WHEN D.TPPES_DEV = 1 THEN 'PJ' ELSE 'PF' END AS TIPO_PESSOA,
    CONVERT(MONEY, C.VLSAL_CON) AS VALOR_TOTAL,
    O.DESCR_OCO AS OCORRENCIA_BASE,
    O.EVENT_OCO AS CANAL,
    O.EVENT_OCO AS EVENTO_OCORRENCIA,
    DATEDIFF(DAY, C.DTATR_CON, GETDATE()) AS DIAS_ATRASO,
    ROW_NUMBER() OVER (PARTITION BY C.CONTR_CON, CA.DESCR_CAR ORDER BY A.DTAND_AND DESC) AS RN,

    ${custoCanalCase('O.EVENT_OCO')} AS CUSTO_TOTAL,

    CASE
        WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) <= 30 THEN 'M1'
        WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 31 AND 60 THEN 'M2'
        WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 61 AND 90 THEN 'M3'
        WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 91 AND 180 THEN 'M4'
        WHEN DATEDIFF(DAY, CONVERT(DATE, C.DTATR_CON), GETDATE()) BETWEEN 181 AND 210 THEN 'M5'
        ELSE '>210'
    END AS FAIXA_ATRASO
INTO #RETORNOS
FROM IMAPSNECTAR_DEVEDORES D WITH(NOLOCK)
INNER JOIN IMAPSNECTAR_CONTRATOS C WITH(NOLOCK)
    ON D.IDDEV_DEV = C.IDDEV_CON
INNER JOIN IMAPSNECTAR_OCORRENCIASENVIOS A WITH(NOLOCK)
    ON C.IDCON_CON = A.IDCON_AND
INNER JOIN IMAPSNECTAR_OCORRENCIAS O WITH(NOLOCK)
    ON A.IDOCO_AND = O.IDOCO_OCO
INNER JOIN TB_PESSOAL P WITH (NOLOCK)
    ON P.IDPES_PES = A.IDPES_AND
INNER JOIN IMAPSNECTAR_CARTEIRAS CA WITH (NOLOCK)
    ON CA.IDCAR_CAR = C.IDCAR_CON
LEFT JOIN (
    SELECT
        E.IDORI_EMA,
        E.USUAR_EMA,
        E.DOMIN_EMA,
        E.EXTEN_EMA,
        ROW_NUMBER() OVER (PARTITION BY E.IDORI_EMA ORDER BY E.IDEMA_EMA DESC) AS RN_EMA
    FROM IMAPSNECTAR_EMAIL E WITH(NOLOCK)
) EMA
    ON EMA.IDORI_EMA = C.IDCON_CON
   AND EMA.RN_EMA = 1
CROSS APPLY (
    SELECT TOP (1)
        TEMPT.DDD_TEL,
        TEMPT.TELEF_TEL
    FROM TB_TELEFONE TEMPT WITH(NOLOCK)
    WHERE TEMPT.IDORI_TEL IN (C.IDCON_CON, D.IDDEV_DEV)
    ORDER BY TEMPT.IDTEL_TEL DESC
) T
WHERE C.IDEMP_CON = 29
  AND P.USUAR_PES NOT IN ('DISCADOR','DISCADOROLOS')
  AND A.DTAND_AND >= @dataInicio
  AND A.DTAND_AND < @dataFim
  AND DESCR_CAR IN ('CLOUD - ONE', 'CLOUD - PME', 'ONE ATIVO CARTEIRIZADO B', 'ONE ATIVO DISCADOR B', 'ONE INATIVOS DISCADOR B')
  AND O.EVENT_OCO IN (
        'WHATSAPPMASSIVO',
        'SMSMASSIVO',
        'EMAILMASSIVO',
        'SMSPREVENTIVO',
        'EMAILPREVENTIVO'
  );

CREATE CLUSTERED INDEX CX_RETORNOS_IDCON ON #RETORNOS (IDCON_CON);


-- 2. ESTATÍSTICAS POR CLIENTE
SELECT
    R.IDCON_CON,
    COUNT(R.IDCON_CON) AS TOTAL_DISPAROS,
    ISNULL(MAX(I.TOTAL_INTERACOES), 0) AS TOTAL_INTERACOES
INTO #ESTATISTICAS_CLIENTE
FROM #RETORNOS R
LEFT JOIN (
    SELECT
        A.IDCON_AND,
        COUNT(*) AS TOTAL_INTERACOES
    FROM IMAPSNECTAR_OCORRENCIASENVIOS A WITH(NOLOCK)
    INNER JOIN IMAPSNECTAR_OCORRENCIAS B WITH(NOLOCK)
        ON A.IDOCO_AND = B.IDOCO_OCO
    WHERE B.IMAPS_OCO = '1'
      AND A.DTAND_AND >= @dataInicio
      AND A.DTAND_AND < @dataFim
    GROUP BY A.IDCON_AND
) I
    ON I.IDCON_AND = R.IDCON_CON
GROUP BY R.IDCON_CON;

CREATE CLUSTERED INDEX CX_ESTAT_IDCON ON #ESTATISTICAS_CLIENTE (IDCON_CON);


-- 3. BASE CONSOLIDADA PARA CRUZAMENTO
SELECT
    R.IDCON_CON,
    CONVERT(DATE, R.ANDAMENTO) AS ANDAMENTO,
    R.CARTEIRA,
    R.DEVEDOR,
    R.DOCUMENTO,
    R.TELEFONE,
    R.FAIXA_ETARIA,
    R.TIPO_PESSOA,
    R.REGIÃO,
    R.EMAIL,
    R.VALOR_TOTAL,
    R.OCORRENCIA_BASE,
    R.DIAS_ATRASO,
    R.FAIXA_ATRASO,
    CASE WHEN NEG.IMAPS_OCO IN ('1','2','3') THEN 1 ELSE 0 END AS CPC,
    CASE WHEN NEG.IMAPS_OCO IN ('1','2') THEN 1 ELSE 0 END AS CPCA,
    CASE WHEN NEG.IMAPS_OCO = '1' AND (
        NEG.DESCR_OCO LIKE '%ACORDO%'
        OR NEG.DESCR_OCO LIKE '%PROMESSA%'
        OR NEG.DESCR_OCO LIKE '%PAGAMENTO%'
        OR NEG.DESCR_OCO LIKE '%NEGOCIARAM%'
    )
    AND NEG.DESCR_OCO NOT LIKE '%NÃO NEGOCIARAM%'
    AND NEG.DESCR_OCO NOT LIKE '%SEM NEGOCIAÇÃO%'
    THEN 'NEGOCIOU'
    WHEN NEG.IMAPS_OCO IN ('1','2') THEN 'CPCA'
    WHEN NEG.IMAPS_OCO IN ('1','2','3') THEN 'CPC'
    ELSE ''
    END AS TIPO_NEGOCIACAO,
    R.CUSTO_TOTAL,
    ISNULL(E.TOTAL_DISPAROS, 0) AS TOTAL_DISPAROS,
    ISNULL(E.TOTAL_INTERACOES, 0) AS TOTAL_INTERACOES,
    CONVERT(DATE, NEG.DTAND_AND) AS DATA_PROMESSA
INTO #PAGAMENTOS
FROM #RETORNOS R
LEFT JOIN #ESTATISTICAS_CLIENTE E
    ON E.IDCON_CON = R.IDCON_CON
OUTER APPLY (
    SELECT TOP 1 B.DESCR_OCO, B.IMAPS_OCO, A.DTAND_AND
    FROM IMAPSNECTAR_OCORRENCIASENVIOS A WITH(NOLOCK)
    INNER JOIN IMAPSNECTAR_OCORRENCIAS B WITH(NOLOCK)
        ON A.IDOCO_AND = B.IDOCO_OCO
    WHERE A.IDCON_AND = R.IDCON_CON
      AND B.IMAPS_OCO IN ('1','2','3')
      AND A.DTAND_AND >= CAST(R.ANDAMENTO AS DATE)
    ORDER BY A.DTAND_AND ASC
) NEG
WHERE R.RN = 1;

CREATE CLUSTERED INDEX CX_PAGAMENTOS_IDCON ON #PAGAMENTOS (IDCON_CON);


-- 3.1 RETORNO DAS AÇÕES
SELECT
    ANDAMENTO,
    CARTEIRA,
    DEVEDOR,
    DOCUMENTO,
    TELEFONE,
    FAIXA_ETARIA,
    TIPO_PESSOA,
    REGIÃO,
    EMAIL,
    VALOR_TOTAL,
    OCORRENCIA_BASE AS OCORRÊNCIA,
    DIAS_ATRASO,
    FAIXA_ATRASO,
    CPC,
    CPCA,
    TIPO_NEGOCIACAO,
    CUSTO_TOTAL,
    TOTAL_DISPAROS,
    TOTAL_INTERACOES
FROM #PAGAMENTOS
ORDER BY ANDAMENTO ASC;

-- 4. PAGAMENTOS 
WITH PAGAMENTOS_OFICIAIS AS (
    SELECT DISTINCT
        C.IDCON_CON,
        C.CONTR_CON,
        D.NOME_DEV AS DEVEDOR,
        D.CGCPF_DEV AS DOCUMENTO,
        CONVERT(DATE, I.DTAJU_AJU) AS DATA_BAIXA,
        I.VLAJU_AJU AS VALOR_PAGO,
        B.IMAPS_OCO 
    FROM IMAPSNECTAR_OCORRENCIASENVIOS A WITH (NOLOCK)
    INNER JOIN IMAPSNECTAR_OCORRENCIAS B WITH (NOLOCK) ON A.IDOCO_AND = B.IDOCO_OCO
    INNER JOIN IMAPSNECTAR_CONTRATOS C WITH (NOLOCK) ON A.IDCON_AND = C.IDCON_CON
    INNER JOIN IMAPSNECTAR_DEVEDORES D WITH (NOLOCK) ON C.IDDEV_CON = D.IDDEV_DEV
    INNER JOIN IMAPSNECTAR_IM_AJUSTE I WITH (NOLOCK) ON C.CONTR_CON = I.CONTR_AJU
    WHERE C.IDEMP_CON = 29
      AND CONVERT(DATE, I.DTAJU_AJU) BETWEEN @dataInicio AND @dataFim
      AND CONVERT(DATE, A.DTAND_AND) BETWEEN @dataInicio AND @dataFim
)
SELECT DISTINCT
    PG.CARTEIRA AS CARTEIRA,
    CASE 
        WHEN P.IMAPS_OCO = '1' AND DATEDIFF(DAY, CONVERT(DATE, PG.DATA_PROMESSA), P.DATA_BAIXA) BETWEEN 0 AND 10 
            THEN 'PAGAMENTO DIRETO'
        ELSE 'PAGAMENTO INDIRETO'
    END AS TIPO_PAGAMENTO,
    P.DEVEDOR AS CLIENTE,
    P.DOCUMENTO AS CGCPF_DEV,
    CONVERT(MONEY, P.VALOR_PAGO) AS VALOR_PAGO,
    PG.REGIÃO,
    PG.TIPO_PESSOA,
    PG.FAIXA_ETARIA,
    P.DATA_BAIXA AS DATA_PAGAMENTO
FROM PAGAMENTOS_OFICIAIS P
INNER JOIN #PAGAMENTOS PG ON PG.IDCON_CON = P.IDCON_CON
ORDER BY P.DATA_BAIXA ASC;


-- 5. DISPAROS / VOLUMETRIA
SELECT
    CONVERT(DATE, B.DTAND_AND) AS DATA_ENVIO,
    CA.DESCR_CAR AS CARTEIRA,
    C.EVENT_OCO AS CANAL,
    COUNT(A.IDCON_CON) AS QUANTIDADE_ENVIOS,
    SUM(
        ${custoCanalCase('C.EVENT_OCO')}
    ) AS CUSTO_TOTAL
FROM IMAPSNECTAR_CONTRATOS A WITH(NOLOCK)
INNER JOIN IMAPSNECTAR_OCORRENCIASENVIOS B WITH(NOLOCK)
    ON A.IDCON_CON = B.IDCON_AND
INNER JOIN IMAPSNECTAR_OCORRENCIAS C WITH(NOLOCK)
    ON B.IDOCO_AND = C.IDOCO_OCO
INNER JOIN IMAPSNECTAR_CARTEIRAS CA WITH(NOLOCK)
    ON CA.IDCAR_CAR = A.IDCAR_CON
WHERE A.IDEMP_CON = '29'
  AND B.DTAND_AND >= @dataInicio
  AND B.DTAND_AND < @dataFim
  AND CA.DESCR_CAR IN ('CLOUD - ONE', 'CLOUD - PME', 'ONE ATIVO CARTEIRIZADO B', 'ONE ATIVO DISCADOR B', 'ONE INATIVOS DISCADOR B')
  AND C.EVENT_OCO IN (
        'WHATSAPPMASSIVO',
        'SMSMASSIVO',
        'EMAILMASSIVO',
        'SMSPREVENTIVO',
        'EMAILPREVENTIVO'
  )
GROUP BY
    CONVERT(DATE, B.DTAND_AND),
    CA.DESCR_CAR,
    C.EVENT_OCO
ORDER BY
    DATA_ENVIO ASC,
    CANAL ASC;
    `;

    try {
        const result = await executarComRetry(conectarBD2, queryVivo, 'VIVO', 600000, 2, periodo);
        return {
            acoes: result.recordsets[0] || [],
            pagamentos: result.recordsets[1] || [],
            disparos: result.recordsets[2] || [],
            volumetria: result.recordsets[2] || []
        };
    } catch (error) {
        console.error("Erro definitivo ao buscar dados da Vivo:", error);
        throw error;
    }
}

// ==========================================
// 2. ROTAS EXPRESS
// ==========================================

// Antes: 6 handlers praticamente idênticos (cache, refresh, try/catch,
// status 500 genérico) — só mudava o nome da carteira e o formato do payload.
// Agora existe uma única fábrica de rotas; cada carteira só declara o que é
// realmente diferente nela (o "mapPayload"), preservando o comportamento
// exato que cada uma já tinha (inclusive as pequenas inconsistências entre
// carteiras, como Sascar reaproveitar 'volumetria' como 'disparos').
function registrarRotaCarteira({ caminho, chaveCache, nomeCarteira, obterDadosFn, mapPayload }) {
    router.get(caminho, limitarTaxa, async (req, res) => {
        try {
            const periodo = parsePeriodo(req);
            const chave = `${chaveCache}|${periodo.inicioStr}|${periodo.fimStr}`;
            const forcar = ['1', 'true'].includes(String(req.query.refresh || ''));
            const emCache = forcar ? null : cacheGet(chave);

            if (emCache) return res.json(emCache);

            const dados = await obterDadosFn(periodo);
            const payload = {
                status: 'sucesso',
                carteira: nomeCarteira,
                ...mapPayload(dados),
                periodo: { inicio: periodo.inicioStr, fim: periodo.fimStr, limitado: periodo.limitado },
            };

            cacheSet(chave, payload);
            res.json(payload);
        } catch (err) {
            console.error(`Erro na rota ${nomeCarteira}:`, err);
            // Timeout de banco vira 504 (Gateway Timeout); qualquer outro erro, 500.
            const status = err.foiTimeout ? 504 : 500;
            res.status(status).json({ status: 'erro', mensagem: err.message });
        }
    });
}

registrarRotaCarteira({
    caminho: '/api/carteiras/sascar',
    chaveCache: 'sascar',
    nomeCarteira: 'Sascar',
    obterDadosFn: obterDadosSascar,
    mapPayload: dados => ({
        acoes: dados.acoes,
        pagamentos: dados.pagamentos,
        disparos: dados.volumetria,
        volumetria: dados.volumetria,
    }),
});

registrarRotaCarteira({
    caminho: '/api/carteiras/verisure',
    chaveCache: 'verisure',
    nomeCarteira: 'Verisure',
    obterDadosFn: obterDadosVerisure,
    mapPayload: dados => ({
        acoes: dados.acoes,
        pagamentos: dados.pagamentos,
        disparos: dados.disparos,
        volumetria: dados.volumetria,
    }),
});

registrarRotaCarteira({
    caminho: '/api/carteiras/servidor2-carteira',
    chaveCache: 'linx',
    nomeCarteira: 'Linx',
    obterDadosFn: obterDadosCarteiraServidor2,
    mapPayload: dados => ({
        acoes: dados.acoes,
        disparos: dados.disparos,
        // ⚠️ Gap conhecido: obterDadosCarteiraServidor2 nunca consultou pagamentos.
        // Mantido como estava para não mudar contrato do front sem confirmar com o time de dados;
        // ver observação no relatório de melhorias sobre isso.
        pagamentos: [],
    }),
});

registrarRotaCarteira({
    caminho: '/api/carteiras/tim',
    chaveCache: 'tim',
    nomeCarteira: 'Tim',
    obterDadosFn: obterDadosTIM,
    mapPayload: dados => ({
        acoes: dados.acoes,
        pagamentos: dados.pagamentos,
        disparos: dados.disparos,
        volumetria: dados.volumetria,
    }),
});

registrarRotaCarteira({
    caminho: '/api/carteiras/sem-parar-whatsapp',
    chaveCache: 'sem-parar-whatsapp',
    nomeCarteira: 'Sem Parar - WhatsApp',
    obterDadosFn: obterDadosSemPararWhatsapp,
    mapPayload: dados => ({
        acoes: dados.acoes,
        pagamentos: dados.pagamentos,
        disparos: dados.disparos,
        volumetria: dados.volumetria,
        // Retornos reais (todo contato no WhatsApp, com ou sem acordo) — ver
        // RESULT SET 4 da query. Distinto de "acoes", que só traz o
        // subconjunto que fechou acordo.
        retornos: dados.retornos,
    }),
});

registrarRotaCarteira({
    caminho: '/api/carteiras/sem-parar-email',
    chaveCache: 'sem-parar-email',
    nomeCarteira: 'Sem Parar - Email',
    obterDadosFn: obterDadosSemPararEmail,
    mapPayload: dados => ({
        acoes: dados.acoes,
        pagamentos: dados.pagamentos,
        disparos: dados.disparos,
        volumetria: dados.volumetria,
        // O canal Email não tem uma tabela de "resposta/abertura" equivalente
        // ao WHATSAPP_PANEAS (ver comentário "ESTIMADO VIA EMAIL" na query de
        // #ACORDOS) — não existe #RETORNOS de verdade aqui. Por isso não há
        // um retorno "puro" diferente do acordo; mantemos acoes como
        // aproximação para o front não precisar tratar esse campo como
        // ausente.
        retornos: dados.acoes,
    }),
});

registrarRotaCarteira({
    caminho: '/api/carteiras/vivo',
    chaveCache: 'vivo',
    nomeCarteira: 'Vivo',
    obterDadosFn: obterDadosVivo,
    mapPayload: dados => ({
        acoes: dados.acoes,
        pagamentos: dados.pagamentos,
        disparos: dados.disparos,
        volumetria: dados.volumetria,
    }),
});

// Health-check simples, útil para load balancer / monitoramento e para
// verificar rapidamente se a API está de pé sem bater no banco.
router.get('/api/health', (req, res) => {
    res.json({ status: 'ok', timestamp: new Date().toISOString(), cacheEntradas: cacheAPI.size });
});

// ==========================================
// 3. APP EXPRESS
// ==========================================
const app = express();
app.use(cors(corsOptions));
app.use(express.json());
app.use(express.static(__dirname));

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'teste2.html'));
});

app.use(router);

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`🚀 Servidor rodando em: http://localhost:${PORT}`);
});

module.exports = {
    router,
    obterDadosSascar,
    obterDadosVerisure,
    obterDadosCarteiraServidor2,
    obterDadosTIM,
    obterDadosSemPararWhatsapp,
    obterDadosSemPararEmail,
    obterDadosVivo
};