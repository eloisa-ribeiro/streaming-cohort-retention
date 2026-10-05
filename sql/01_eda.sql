-- 01_eda.sql | Análise Exploratória (EDA) - KKBox
-- Projeto: Cohort Analysis de retenção de assinantes (streaming)
-- Plataforma: Databricks (Spark SQL)
-- Fonte: KKBox Churn Prediction Challenge (Kaggle)
--        tabelas: members_v3 e transactions_v2


-- 1. CARGA DOS DADOS (CSV -> tabelas no Unity Catalog)

CREATE TABLE transactions AS
SELECT * FROM read_files(
  '/Volumes/workspace/default/cohort_raw/transactions_v2.csv',
  format => 'csv', header => true
);

CREATE TABLE members AS
SELECT * FROM read_files(
  '/Volumes/workspace/default/cohort_raw/members_v3.csv',
  format => 'csv', header => true
);

-- Visão geral das tabelas
SELECT * FROM transactions LIMIT 20;
SELECT * FROM members LIMIT 20;


-- 2. TRANSACTIONS

-- 2.1 Grão da tabela: linhas x clientes distintos
SELECT
    COUNT(msno)            AS qtd_linhas,
    COUNT(DISTINCT msno)   AS qtd_clientes_distintos
FROM transactions;
-- Conclusão: msno se repete -> o grão é a transação, não o cliente.

-- 2.2 Nulos por coluna
SELECT
    COUNT(*)                                                 AS total_linhas,
    COUNT(*) - COUNT(payment_method_id)                      AS nulos_payment_method_id,
    COUNT(*) - COUNT(payment_plan_days)                      AS nulos_payment_plan_days,
    COUNT(*) - COUNT(plan_list_price)                        AS nulos_plan_list_price,
    COUNT(*) - COUNT(actual_amount_paid)                     AS nulos_actual_amount_paid,
    COUNT(*) - COUNT(is_auto_renew)                          AS nulos_is_auto_renew,
    COUNT(*) - COUNT(transaction_date)                       AS nulos_transaction_date,
    COUNT(*) - COUNT(membership_expire_date)                 AS nulos_membership_expire_date,
    COUNT(*) - COUNT(is_cancel)                              AS nulos_is_cancel,
    COUNT(*) - COUNT(_rescued_data)                          AS nulos_rescued_data
FROM transactions;
-- Conclusão: _rescued_data é toda nula (coluna técnica do read_files) -> descartar.

-- 2.3 Mínimos e máximos
SELECT
    MIN(payment_method_id)       AS min_payment_method_id,
    MAX(payment_method_id)       AS max_payment_method_id,
    MIN(payment_plan_days)       AS min_payment_plan_days,
    MAX(payment_plan_days)       AS max_payment_plan_days,
    MIN(plan_list_price)         AS min_plan_list_price,
    MAX(plan_list_price)         AS max_plan_list_price,
    MIN(actual_amount_paid)      AS min_actual_amount_paid,
    MAX(actual_amount_paid)      AS max_actual_amount_paid,
    MIN(transaction_date)        AS min_transaction_date,
    MAX(transaction_date)        AS max_transaction_date,
    MIN(membership_expire_date)  AS min_membership_expire_date,
    MAX(membership_expire_date)  AS max_membership_expire_date
FROM transactions;
-- Conclusões:
--  * transaction_date cobre 2015 a 2017.
--  * membership_expire_date vai de 2016 até 2036 (investigado abaixo).
--  * is_cancel e is_auto_renew são flags 0/1; payment_method_id é um código.
--  * Há planos que vão do gratuito até ~2 mil.

-- 2.4 Outliers em membership_expire_date
SELECT COUNT(*) AS qtd_apos_2020 FROM transactions WHERE membership_expire_date > 20200101;
SELECT COUNT(*) AS qtd_apos_2030 FROM transactions WHERE membership_expire_date > 20300101;
-- Conclusão: 2.971 registros expiram após 2020 e 7 após 2030.
-- Decisão: tratados como outliers (a base termina em 2017).


-- 3. MEMBERS

-- 3.1 Grão da tabela
SELECT
    COUNT(msno)            AS qtd_linhas,
    COUNT(DISTINCT msno)   AS qtd_clientes_distintos
FROM members;
-- Conclusão: sem duplicidade (cadastro com 1 linha por cliente).

-- 3.2 Nulos por coluna
SELECT
    COUNT(*)                                         AS total_linhas,
    COUNT(*) - COUNT(city)                           AS nulos_city,
    COUNT(*) - COUNT(bd)                             AS nulos_bd,
    COUNT(*) - COUNT(gender)                         AS nulos_gender,
    COUNT(*) - COUNT(registered_via)                 AS nulos_registered_via,
    COUNT(*) - COUNT(registration_init_time)         AS nulos_registration_init_time,
    COUNT(*) - COUNT(_rescued_data)                  AS nulos_rescued_data
FROM members;
-- Conclusões:
--  * _rescued_data vazia -> descartar.
--  * gender tem mais de 400 mil nulos (provável não preenchimento no cadastro).

-- 3.3 Mínimos e máximos
SELECT
    MIN(city)                     AS min_city,
    MAX(city)                     AS max_city,
    MIN(bd)                       AS min_bd,
    MAX(bd)                       AS max_bd,
    MIN(gender)                   AS min_gender,
    MAX(gender)                   AS max_gender,
    MIN(registered_via)           AS min_registered_via,
    MAX(registered_via)           AS max_registered_via,
    MIN(registration_init_time)   AS min_registration_init_time,
    MAX(registration_init_time)   AS max_registration_init_time
FROM members;
-- Conclusões:
--  * city: 22 cidades distintas (código).
--  * bd: idade declarada pelo usuário, campo de preenchimento livre -> validar
--    valores fora do esperado antes de qualquer uso; fora do escopo do cohort.


-- 4. CRUZAMENTO transactions x members

-- 4.1 Cobertura do cadastro
SELECT
    COUNT(t.msno)           AS qtd_transacoes,
    COUNT(DISTINCT t.msno)  AS qtd_clientes_com_transacao,
    COUNT(m.msno)           AS qtd_transacoes_com_cadastro
FROM transactions t
LEFT JOIN members m ON t.msno = m.msno;

-- 4.2 Totais gerais (clientes DISTINTOS)
WITH CTE_CLIENTES AS (
  SELECT COUNT(DISTINCT msno) AS qtd_clientes
  FROM members
  WHERE registration_init_time IS NOT NULL
),
CTE_CANCELAMENTO AS (
  SELECT COUNT(DISTINCT msno) AS qtd_cancelaram
  FROM transactions
  WHERE is_cancel = 1
),
CTE_RENOVACAO AS (
  SELECT COUNT(DISTINCT msno) AS qtd_com_auto_renovacao
  FROM transactions
  WHERE is_auto_renew = 1
)
SELECT
    CTE_CLIENTES.qtd_clientes,
    CTE_CANCELAMENTO.qtd_cancelaram,
    CTE_RENOVACAO.qtd_com_auto_renovacao
FROM CTE_CLIENTES
CROSS JOIN CTE_CANCELAMENTO      -- cada CTE retorna 1 linha, então não há duplicação
CROSS JOIN CTE_RENOVACAO;
-- Conclusões (clientes distintos):
--  * 34.551 já cancelaram pelo menos uma vez.
--  * 923.813 tiveram transação com renovação automática ligada.
--  * 6.769.473 clientes distintos na tabela members.
-- Atenção: is_auto_renew indica renovação automática ATIVA, não que o cliente renovou de fato.


-- 5. EVOLUÇÃO MENSAL (pela data do evento: transaction_date)

-- Decisão: usar transaction_date (quando o evento aconteceu) e não
-- membership_expire_date (até quando a assinatura vale), que gerava
-- uma leitura deslocada dos cancelamentos.
WITH CTE_CLIENTES AS (
  SELECT
      YEAR(TO_DATE(CAST(registration_init_time AS STRING), 'yyyyMMdd'))   AS ano,
      MONTH(TO_DATE(CAST(registration_init_time AS STRING), 'yyyyMMdd'))  AS mes,
      COUNT(DISTINCT msno)                                                AS qtd_clientes
  FROM members
  WHERE registration_init_time IS NOT NULL
  GROUP BY ano, mes
),
CTE_CANCELAMENTO AS (
  SELECT
      YEAR(TO_DATE(CAST(transaction_date AS STRING), 'yyyyMMdd'))   AS ano,
      MONTH(TO_DATE(CAST(transaction_date AS STRING), 'yyyyMMdd'))  AS mes,
      COUNT(DISTINCT msno)                                          AS qtd_cancelaram
  FROM transactions
  WHERE is_cancel = 1
  GROUP BY ano, mes
),
CTE_RENOVACAO AS (
  SELECT
      YEAR(TO_DATE(CAST(transaction_date AS STRING), 'yyyyMMdd'))   AS ano,
      MONTH(TO_DATE(CAST(transaction_date AS STRING), 'yyyyMMdd'))  AS mes,
      COUNT(DISTINCT msno)                                          AS qtd_com_auto_renovacao
  FROM transactions
  WHERE is_auto_renew = 1
  GROUP BY ano, mes
)
SELECT
    c.ano,
    c.mes,
    c.qtd_clientes,
    can.qtd_cancelaram,
    ren.qtd_com_auto_renovacao
FROM CTE_CLIENTES c
LEFT JOIN CTE_CANCELAMENTO can ON c.ano = can.ano AND c.mes = can.mes
LEFT JOIN CTE_RENOVACAO ren    ON c.ano = ren.ano AND c.mes = ren.mes
ORDER BY c.ano, c.mes;
-- Conclusões:
--  * Meses com mais cadastros: jan/2016, seguido de nov/2015 e out/2015.
--  * Maior volume de cancelamentos distintos: mar/2017, seguido de fev/2017.


-- 6. EXPIRAÇÃO x CANCELAMENTO (a nível de linha, sem JOIN)

-- is_cancel e membership_expire_date estão na mesma linha, então a relação
-- é direta. Juntar agregados de CTEs diferentes só "coincide no calendário"
-- e não prova relação entre os mesmos eventos.
SELECT
    YEAR(TO_DATE(CAST(membership_expire_date AS STRING), 'yyyyMMdd'))   AS ano,
    MONTH(TO_DATE(CAST(membership_expire_date AS STRING), 'yyyyMMdd'))  AS mes,
    COUNT(*)                                                            AS qtd_expiraram,
    SUM(is_cancel)                                                      AS qtd_terminou_em_cancelamento,
    SUM(CASE WHEN is_cancel = 0 THEN 1 ELSE 0 END)                      AS qtd_nao_cancelou
FROM transactions
WHERE membership_expire_date IS NOT NULL
GROUP BY ano, mes
ORDER BY ano, mes;
-- Conclusão: em mar/2017, cerca de 50% das assinaturas que expiraram
-- terminaram em cancelamento -> mês crítico da base.


-- CONCLUSÕES FINAIS DA EDA
-- * _rescued_data é vazia nas duas tabelas e será descartada.
-- * transactions: grão = transação (cliente se repete). Período: 2015-2017.
-- * membership_expire_date tem outliers (2.971 > 2020; 7 > 2030): tratados
--   como ruído, pois a base termina em 2017.
-- * gender tem alta taxa de nulos; bd é campo livre e não será usado.
-- * 34.551 clientes distintos já cancelaram ao menos uma vez.
-- * Mês crítico de cancelamento: março/2017.
-- * Próximo passo: construir a matriz de cohort (02_cohort_matrix.sql),
--   com a safra definida pela PRIMEIRA TRANSAÇÃO PAGA do cliente.