CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.dados_agregados_area_logada
TABLESPACE pg_default
as WITH diabeticos AS (
    SELECT 
        tbd.municipio_id_sus,
        tbd.equipe_ine_cadastro AS equipe_ine,
        'DIABETES' AS indicador,
        'SOLICITAR_HEMOGLOBINA' AS parametro_descricao,
        COUNT(*) AS total,
        SUM(CASE WHEN tbd.status_usuario = 'Consulta e solicitação de hemoglobina a fazer' THEN 1 ELSE 0 END) AS parametro_valor,
        CURRENT_TIMESTAMP AS atualizacao_data 
    FROM impulso_previne_dados_nominais_replica.painel_enfermeiras_lista_nominal_diabeticos tbd
    GROUP BY tbd.municipio_id_sus, tbd.equipe_ine_cadastro
),
hipertensos AS (
    SELECT 
        tbh.municipio_id_sus,
        tbh.equipe_ine_cadastro AS equipe_ine,
        'HIPERTENSOS' AS indicador,
        'AFERIR_PA' AS parametro_descricao,
        COUNT(*) AS total,
        SUM(CASE WHEN tbh.status_usuario = 'Consulta e aferição de PA a fazer' THEN 1 ELSE 0 END) AS parametro_valor,
        CURRENT_TIMESTAMP AS atualizacao_data 
    FROM impulso_previne_dados_nominais_replica.painel_enfermeiras_lista_nominal_hipertensos tbh
    GROUP BY tbh.municipio_id_sus, tbh.equipe_ine_cadastro
),
citopatologico AS (
    SELECT 
        tbc.municipio_id_sus,
        tbc.equipe_ine,
        'CITOPATOLOGICO' AS indicador,
        'COLETAR_EXAME_CITO' AS parametro_descricao,
        COUNT(*) AS total,
        SUM(CASE WHEN tbc.id_status_usuario <> 12 THEN 1 ELSE 0 END) AS parametro_valor, -- Quando status diferente de 12 (Em Dia)
        CURRENT_TIMESTAMP AS atualizacao_data 
    FROM impulso_previne_dados_nominais_replica.painel_citopatologico_lista_nominal tbc
    GROUP BY tbc.municipio_id_sus, tbc.equipe_ine
),
vacinacao AS (
    SELECT 
        tbv.municipio_id_sus,
        tbv.equipe_ine,
        'VACINACAO' AS indicador,
        'DOSE_ATRASADA' AS parametro_descricao,
        COUNT(*) AS total,
        SUM(CASE WHEN tbv.id_status_polio = 3 OR tbv.id_status_penta = 3 THEN 1 ELSE 0 END) AS parametro_valor, -- Quando status polio ou penta em atraso
        CURRENT_TIMESTAMP AS atualizacao_data 
    FROM impulso_previne_dados_nominais_replica.painel_vacinacao_lista_nominal tbv
    WHERE tbv.id_status_quadrimestre = 1 -- Quadrimestre atual
    GROUP BY tbv.municipio_id_sus, tbv.equipe_ine
),
gestantes_6_consultas AS (
    SELECT 
        tb6.municipio_id_sus,
        tb6.equipe_ine,
        'PRE_NATAL' AS indicador,
        'MENOS_6_CONSULTAS' AS parametro_descricao,
        COUNT(*) AS total,
        SUM(CASE 
                WHEN tb6.gestacao_idade_gestacional_primeiro_atendimento >= 0 
                AND tb6.gestacao_idade_gestacional_primeiro_atendimento <= 12 
                AND tb6.consultas_pre_natal_validas < 6 -- gestantes com menos de 6 consultas em 12 semanas
                THEN 1 
                ELSE 0 
            END) AS parametro_valor,
        CURRENT_TIMESTAMP AS atualizacao_data 
    FROM impulso_previne_dados_nominais_replica.painel_gestantes_lista_nominal tb6
    WHERE tb6.gestacao_quadrimestre = ( -- gestação quadrimestre = quadri atual OBS! não inclui gestantes sem dum
        CASE 
            WHEN DATE_PART('month', CURRENT_DATE) >= 1 AND DATE_PART('month', CURRENT_DATE) <= 4 THEN CONCAT(DATE_PART('year', CURRENT_DATE), '.Q1')
            WHEN DATE_PART('month', CURRENT_DATE) >= 5 AND DATE_PART('month', CURRENT_DATE) <= 8 THEN CONCAT(DATE_PART('year', CURRENT_DATE), '.Q2')
            WHEN DATE_PART('month', CURRENT_DATE) >= 9 AND DATE_PART('month', CURRENT_DATE) <= 12 THEN CONCAT(DATE_PART('year', CURRENT_DATE), '.Q3')
        END
    )                        
    GROUP BY tb6.municipio_id_sus, tb6.equipe_ine
),
gestantes_odonto_indetificado AS (
    SELECT 
        tbo.municipio_id_sus,
        tbo.equipe_ine,
        'PRE_NATAL' AS indicador,
        'ODONTO' AS parametro_descricao,
        COUNT(*) AS total,
        SUM(CASE 
                WHEN tbo.id_atendimento_odontologico <> 1 -- Atend. odontológico NÃO identificado
                THEN 1 
                ELSE 0 
            END) AS parametro_valor,
        CURRENT_TIMESTAMP AS atualizacao_data 
    FROM impulso_previne_dados_nominais_replica.painel_gestantes_lista_nominal tbo
    WHERE tbo.gestacao_quadrimestre = (
        CASE 
            WHEN DATE_PART('month', CURRENT_DATE) >= 1 AND DATE_PART('month', CURRENT_DATE) <= 4 THEN CONCAT(DATE_PART('year', CURRENT_DATE), '.Q1')
            WHEN DATE_PART('month', CURRENT_DATE) >= 5 AND DATE_PART('month', CURRENT_DATE) <= 8 THEN CONCAT(DATE_PART('year', CURRENT_DATE), '.Q2')
            WHEN DATE_PART('month', CURRENT_DATE) >= 9 AND DATE_PART('month', CURRENT_DATE) <= 12 THEN CONCAT(DATE_PART('year', CURRENT_DATE), '.Q3')
        END
    )                        
    GROUP BY tbo.municipio_id_sus, tbo.equipe_ine
),
gestantes_sifilis_hiv AS (
    SELECT 
        tbe.municipio_id_sus,
        tbe.equipe_ine,
        'PRE_NATAL' AS indicador,
        'SIFILIS_HIV' AS parametro_descricao,
        COUNT(*) AS total,
        SUM(CASE 
                WHEN tbe.id_exame_hiv_sifilis <> 4 -- Todos os demais status que não sejam os dois exames realizados
                THEN 1 
                ELSE 0 
            END) AS parametro_valor,
        CURRENT_TIMESTAMP AS atualizacao_data 
    FROM impulso_previne_dados_nominais_replica.painel_gestantes_lista_nominal tbe
    WHERE tbe.gestacao_quadrimestre = (
        CASE 
            WHEN DATE_PART('month', CURRENT_DATE) >= 1 AND DATE_PART('month', CURRENT_DATE) <= 4 THEN CONCAT(DATE_PART('year', CURRENT_DATE), '.Q1')
            WHEN DATE_PART('month', CURRENT_DATE) >= 5 AND DATE_PART('month', CURRENT_DATE) <= 8 THEN CONCAT(DATE_PART('year', CURRENT_DATE), '.Q2')
            WHEN DATE_PART('month', CURRENT_DATE) >= 9 AND DATE_PART('month', CURRENT_DATE) <= 12 THEN CONCAT(DATE_PART('year', CURRENT_DATE), '.Q3')
        END
    )                        
    GROUP BY tbe.municipio_id_sus, tbe.equipe_ine
)
SELECT * FROM gestantes_6_consultas
UNION ALL
SELECT * FROM gestantes_odonto_indetificado
UNION ALL
SELECT * FROM gestantes_sifilis_hiv
UNION ALL
SELECT * FROM diabeticos
UNION ALL
SELECT * FROM hipertensos
UNION ALL
SELECT * FROM citopatologico
UNION ALL
SELECT * FROM vacinacao
WITH DATA;
