-- Checar duplicações
, duplicados AS (
SELECT 
    COUNT(1) AS cont_linhas,
    COUNT(DISTINCT chave_gestacao) AS cont_chaves_nominais_distintas
FROM aux1 -- Adicionar CTE com o resultado final da consulta da lista nominal (transmissao, lista unificada ou painel)
)
-- Checar denominador e numerador
, dem_num AS (
    SELECT
        l.gestacao_quadrimestre,
        count(DISTINCT l.chave_gestacao) AS gestantes_denominador,
            count(DISTINCT
                CASE
                    WHEN l.gestacao_idade_gestacional_primeiro_atendimento >= 0 AND l.gestacao_idade_gestacional_primeiro_atendimento <= 12 AND l.consultas_prenatal_total > 5 THEN l.chave_gestacao
                    ELSE NULL
                END) AS gestantes_6consultas_1consulta_em_12semanas,
            count(DISTINCT
                CASE
                    WHEN l.exame_sifilis_hiv_realizado IS TRUE THEN l.chave_gestacao::character varying
                    ELSE NULL::character varying
                END) AS gestantes_com_sifilis_hiv_realizado,
            count(DISTINCT
                CASE
                    WHEN l.atendimento_odontologico_realizado IS TRUE THEN l.chave_gestacao::character varying
                    ELSE NULL::character varying
                END) AS gestantes_odonto_realizado
    FROM aux1 l -- Adicionar CTE com o resultado final da consulta (transmissao, lista unificada ou painel)
    WHERE l.possui_registro_aborto = 'Não'::text 
        AND l.gestacao_quadrimestre = '2023.Q3'::text -- Adicionar quadimestre atual
    GROUP BY 1
)
