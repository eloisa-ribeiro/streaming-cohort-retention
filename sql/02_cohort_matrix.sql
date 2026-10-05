-- 02_cohort_matrix.sql | Matriz de Cohort de Retenção - KKBox
-- Projeto: Cohort Analysis de retenção de assinantes (streaming)
-- Plataforma: Databricks (Spark SQL)
-- Tabela de origem: transactions

-- DEFINIÇÕES DE NEGÓCIO
--  * Safra (cohort): mês da PRIMEIRA transação do cliente (início da base paga).
--  * Atividade: cada mês em que o cliente teve ao menos 1 transação.
--  * Mês relativo: meses decorridos entre a safra e o mês de atividade
--                  (0 = mês de entrada; 1 = um mês depois; ...).
--  * Retenção (%): clientes ativos no mês relativo N / clientes da safra (mês 0).

WITH CTE_SAFRA AS (
  -- 1 linha por cliente, com o mês/ano da primeira transação
  SELECT
      msno,
      YEAR(MIN(TO_DATE(CAST(transaction_date AS STRING), 'yyyyMMdd')))   AS ano_safra,
      MONTH(MIN(TO_DATE(CAST(transaction_date AS STRING), 'yyyyMMdd')))  AS mes_safra
  FROM transactions
  GROUP BY msno
),

CTE_ATIVIDADE AS (
  -- 1 linha por cliente x mês com atividade (várias transações no mesmo mês contam 1 vez)
  SELECT
      msno,
      YEAR(TO_DATE(CAST(transaction_date AS STRING), 'yyyyMMdd'))   AS ano_atividade,
      MONTH(TO_DATE(CAST(transaction_date AS STRING), 'yyyyMMdd'))  AS mes_atividade
  FROM transactions
  GROUP BY msno, ano_atividade, mes_atividade
),

CTE_COHORT AS (
  -- idade de cada atividade em relação à safra do cliente
  SELECT
      s.msno,
      s.ano_safra,
      s.mes_safra,
      (a.ano_atividade - s.ano_safra) * 12
        + (a.mes_atividade - s.mes_safra)  AS mes_relativo
  FROM CTE_SAFRA s
  JOIN CTE_ATIVIDADE a ON s.msno = a.msno
),

CTE_RELATIVO AS (
  -- clientes distintos ativos por safra e mês relativo
  SELECT
      ano_safra,
      mes_safra,
      mes_relativo,
      COUNT(DISTINCT msno) AS qtd_clientes
  FROM CTE_COHORT
  GROUP BY ano_safra, mes_safra, mes_relativo
),

CTE_COM_BASE AS (
  -- tamanho da safra (mês 0) repetido em cada linha da mesma safra
  SELECT
      ano_safra,
      mes_safra,
      mes_relativo,
      qtd_clientes,
      FIRST_VALUE(qtd_clientes) OVER (
        PARTITION BY ano_safra, mes_safra
        ORDER BY mes_relativo
      ) AS qtd_mes_zero
  FROM CTE_RELATIVO
)

SELECT
    ano_safra,
    mes_safra,
    mes_relativo,
    qtd_clientes,
    qtd_mes_zero,
    ROUND(CAST(qtd_clientes AS DOUBLE) / CAST(qtd_mes_zero AS DOUBLE), 4) AS pct_retencao
FROM CTE_COM_BASE
ORDER BY ano_safra, mes_safra, mes_relativo;


-- VALIDAÇÕES

-- 1) O mês relativo 0 de cada safra deve ter pct_retencao = 1.0.
-- 2) A retenção deve, em geral, cair conforme o mês relativo aumenta.
-- 3) Conferência de tamanho de safra (exemplo: jan/2016).
--    O resultado deve ser igual ao qtd_clientes do mês relativo 0 dessa safra.
SELECT COUNT(DISTINCT msno) AS qtd_clientes_jan_2016
FROM (
    SELECT
        msno,
        YEAR(MIN(TO_DATE(CAST(transaction_date AS STRING), 'yyyyMMdd')))   AS ano_safra,
        MONTH(MIN(TO_DATE(CAST(transaction_date AS STRING), 'yyyyMMdd')))  AS mes_safra
    FROM transactions
    GROUP BY msno
) sub
WHERE ano_safra = 2016 AND mes_safra = 1;
-- Status: validação 3 conferida (valores bateram).