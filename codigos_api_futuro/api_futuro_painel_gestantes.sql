-- impulso_previne_dados_nominais.api_futuro_painel_gestantes source

CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.api_futuro_painel_gestantes
TABLESPACE pg_default
AS SELECT l.municipio_id_sus,
    l.id_quadrimestre_atual,
    l.equipe_ine,
    l.equipe_nome,
    l.id_status_usuario,
    count(DISTINCT l.chave_id_gestacao) AS total_gestantes,
    count(DISTINCT
        CASE
            WHEN l.id_status_usuario = 10 THEN l.chave_id_gestacao
            ELSE NULL::text
        END) AS gestantes_ativas,
    count(DISTINCT
        CASE
            WHEN l.id_status_usuario = 10 AND l.consultas_pre_natal_validas <= 5 THEN l.chave_id_gestacao
            ELSE NULL::text
        END) AS gestantes_ativas_abaixo6consultas,
    count(DISTINCT
        CASE
            WHEN l.id_status_usuario = 10 AND l.gestacao_idade_gestacional_primeiro_atendimento >= 0 AND l.gestacao_idade_gestacional_primeiro_atendimento <= 12 AND l.consultas_pre_natal_validas <= 5 THEN l.chave_id_gestacao
            ELSE NULL::text
        END) AS gestantes_ativas_abaixo6consultas_1consulta_em_12semanas,
    count(DISTINCT
        CASE
            WHEN l.id_status_usuario = 10 AND l.id_erro_registro = 1 THEN l.chave_id_gestacao
            ELSE NULL::text
        END) AS total_gestantes_com_erro_registro,
    count(DISTINCT
        CASE
            WHEN l.id_status_usuario = 10 AND l.id_exame_hiv_sifiis = 4 THEN l.chave_id_gestacao
            ELSE NULL::text
        END) AS gestantes_ativas_sem_sifilis_hiv_realizado,
    count(DISTINCT
        CASE
            WHEN l.id_status_usuario = 10 AND (l.id_exame_hiv_sifiis = 1 OR l.id_exame_hiv_sifiis = 2) THEN l.chave_id_gestacao
            ELSE NULL::text
        END) AS total_gestantes_com_apenas_1_exame,
    count(DISTINCT
        CASE
            WHEN l.id_status_usuario = 10 AND l.id_atendimeno_odontologico = 2 THEN l.chave_id_gestacao
            ELSE NULL::text
        END) AS total_gestantes_sem_atendimento_odontologico,
    CURRENT_TIMESTAMP AS atualizacao_data,
    CURRENT_TIMESTAMP AS criacao_data
   FROM impulso_previne_dados_nominais.api_futuro_painel_gestantes_lista_nominal l
  GROUP BY l.municipio_id_sus, l.id_quadrimestre_atual, l.equipe_ine, l.equipe_nome, l.id_status_usuario
WITH DATA;