-- Checar duplicações
, duplicados AS (
    SELECT 
        'backup - Juquitiba SP' AS municipio_uf,
        COUNT(1) AS cont_linhas,
        COUNT(DISTINCT chave_cidadao) AS cont_chaves_nominais_distintas
    FROM aux1 -- Adicionar CTE com o resultado final da consulta da lista nominal (transmissao, lista unificada ou painel)
    GROUP BY 1
)
-- Checar denominador e numerador
, dem_num AS (
        SELECT
        'backup - Juquitiba SP' AS municipio_uf,
        -- concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
        count(DISTINCT chave_cidadao) AS cont_criancas,
        count(DISTINCT CASE WHEN codigo_vacina ='22' THEN chave_cidadao END) AS cont_criancas_vacinas_polio,
        count(DISTINCT CASE WHEN codigo_vacina ='42' THEN chave_cidadao END) AS cont_criancas_vacinas_penta,
        count(DISTINCT co_seq_fat_vacinacao_vacina) AS cont_vacinas,
        count(DISTINCT CASE WHEN codigo_vacina ='22'  THEN co_seq_fat_vacinacao_vacina END) AS cont_vacinas_polio,
        count(DISTINCT CASE WHEN codigo_vacina ='42' THEN co_seq_fat_vacinacao_vacina END) AS cont_vacinas_penta
    FROM aux1 l
    --LEFT JOIN listas_de_codigos.municipios m 
    --    ON m.id_sus = l.municipio_id_sus-- Adicionar CTE com o resultado final da consulta (transmissao, lista unificada ou painel)
    GROUP BY 1
)
SELECT 
    *
FROM dem_num