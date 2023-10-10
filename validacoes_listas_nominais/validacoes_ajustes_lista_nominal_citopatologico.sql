-- AJUSTES NO CÓDIGO DO TRANSMISSOR
-- Checar duplicações
, duplicados AS (
    SELECT 
        'backup - Juquitiba - SP' AS municipio_uf, -- Nome do backup testado
        COUNT(1) AS cont_linhas,
        COUNT(DISTINCT chave_mulher) AS cont_chaves_nominais_distintas
    FROM aux1 
    GROUP BY 1
)
-- Checar denominador e numerador
, dem_num AS (
    SELECT
        'backup - Juquitiba - SP' AS municipio_uf, -- Nome do backup testado
        count(DISTINCT l.chave_mulher) AS denominador_cito,
        count(DISTINCT CASE WHEN l.realizou_exame_ultimos_36_meses IS TRUE 
                                THEN l.chave_mulher
                        END) AS numerador_cito
    FROM aux1 l
    GROUP BY 1
)
SELECT 
    *
FROM dem_num

-- AJUSTES NA VIEW DO PAINEL 
-- Checar duplicações
, duplicados AS (
    SELECT 
        municipio_id_sus,
        COUNT(1) AS cont_linhas,
        COUNT(DISTINCT l.paciente_nome||l.cidadao_cpf_dt_nascimento||l.municipio_id_sus) AS cont_chaves_nominais_distintas
    FROM impulso_previne_dados_nominais.painel_citopatologico_lista_nominal   -- Adicionar CTE com o resultado final da consulta da lista nominal 
    GROUP BY 1
)
-- Checar denominador e numerador
, dem_num AS (
    SELECT
        concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
        count(DISTINCT l.paciente_nome||l.cidadao_cpf_dt_nascimento||l.municipio_id_sus) AS denominador_cito,
        count(DISTINCT CASE WHEN l.realizou_exame_ultimos_36_meses IS TRUE 
                                THEN l.paciente_nome||l.cidadao_cpf_dt_nascimento||municipio_id_sus
                        END) AS numerador_cito
    FROM impulso_previne_dados_nominais.painel_citopatologico_lista_nominal -- Adicionar CTE com o resultado final da consulta da lista nominal
    LEFT JOIN listas_de_codigos.municipios m 
        ON m.id_sus = l.municipio_id_sus
    GROUP BY 1
)
SELECT 
    *
FROM dem_num