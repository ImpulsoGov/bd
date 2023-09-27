-- Checar duplicações
, duplicados AS (
SELECT 
    COUNT(1) AS cont_linhas,
    COUNT(DISTINCT chave_gestacao) AS cont_chaves_nominais_distintas
<<<<<<< HEAD
<<<<<<< HEAD
FROM aux1 -- Adicionar CTE com o resultado final da consulta da lista nominal (transmissao, lista unificada ou painel)
)
-- Checar denominador e numerador
, dem_num AS (
        SELECT
        concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
=======
FROM aux1 -- Adicionar CTE com o resultado final da consulta
=======
FROM aux1 -- Adicionar CTE com o resultado final da consulta da lista nominal (transmissao, lista unificada ou painel)
>>>>>>> fd470ac (Update validacoes_ajustes_lista_nominal_gestantes.sql)
)
-- Checar denominador e numerador
, dem_num AS (
    SELECT
>>>>>>> fa49bbb (renomeia arquivo)
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
<<<<<<< HEAD
<<<<<<< HEAD
    FROM aux1 l
    LEFT JOIN listas_de_codigos.municipios m 
        ON m.id_sus = l.municipio_id_sus-- Adicionar CTE com o resultado final da consulta (transmissao, lista unificada ou painel)
    WHERE l.possui_registro_aborto = 'Não'::text 
        AND l.gestacao_quadrimestre = '2023.Q3'::text -- Adicionar quadimestre atual
    GROUP BY 1, 2
)
=======
    FROM aux1 l -- Adicionar CTE com o resultado final da consulta
=======
    FROM aux1 l -- Adicionar CTE com o resultado final da consulta (transmissao, lista unificada ou painel)
>>>>>>> fd470ac (Update validacoes_ajustes_lista_nominal_gestantes.sql)
    WHERE l.possui_registro_aborto = 'Não'::text 
        AND l.gestacao_quadrimestre = '2023.Q3'::text -- Adicionar quadimestre atual
    GROUP BY 1
<<<<<<< HEAD
>>>>>>> fa49bbb (renomeia arquivo)
=======
)
>>>>>>> 670c74e (Update validacoes_ajustes_lista_nominal_gestantes.sql)