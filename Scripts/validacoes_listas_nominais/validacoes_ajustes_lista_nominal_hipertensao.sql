-- Checar duplicações
, duplicados AS (
    SELECT 
        'backup - Xapuri - AC' AS municipio_uf,
        COUNT(1) AS cont_linhas,
        COUNT(DISTINCT cidadao_nome||dt_nascimento) AS cont_chaves_nominais_distintas
    FROM aux1 -- Adicionar CTE com o resultado final da consulta da lista nominal (transmissao, lista unificada ou painel)
    GROUP BY 1
)
-- Checar denominador e numerador
, dem_num AS (
        SELECT
        'backup - Xapuri - AC' AS municipio_uf,
        -- concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
        count(DISTINCT l.cidadao_nome||l.dt_nascimento) AS hipertensao_denominador,
        count(DISTINCT CASE WHEN l.possui_hipertensao_autorreferida IS TRUE THEN l.cidadao_nome||l.dt_nascimento END) AS cidadaos_autorreferidos,
        count(DISTINCT CASE WHEN l.possui_hipertensao_diagnosticada IS TRUE THEN l.cidadao_nome||l.dt_nascimento END) AS cidadaos_diagnosticados,
        count(DISTINCT CASE 
                            WHEN l.realizou_consulta_ultimos_6_meses IS TRUE AND realizou_afericao_ultimos_6_meses IS TRUE 
                                THEN l.cidadao_nome||l.dt_nascimento 
                        END) AS numerador_hipertensao,
        count(DISTINCT CASE WHEN l.dt_consulta_mais_recente IS NOT NULL THEN l.cidadao_nome||l.dt_nascimento END) AS cidadaos_com_consulta,
        count(DISTINCT CASE WHEN l.dt_consulta_mais_recente IS NOT NULL AND l.possui_hipertensao_diagnosticada IS FALSE THEN l.cidadao_nome||l.dt_nascimento END) AS cidadaos_com_consulta_sem_diagnotico
    FROM aux1 l
    --LEFT JOIN listas_de_codigos.municipios m 
    --    ON m.id_sus = l.municipio_id_sus-- Adicionar CTE com o resultado final da consulta (transmissao, lista unificada ou painel)
    GROUP BY 1
)
SELECT 
    *
FROM dem_num
