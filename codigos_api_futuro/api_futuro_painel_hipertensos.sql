
CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.api_futuro_painel_hipertensos
TABLESPACE pg_default
AS SELECT db.municipio_id_sus,
    db.equipe_ine,
    db.equipe_nome,
    db.id_faixa_etaria,
    db.id_tipo_de_diagnostico,
    db.acs_nome,
    db.id_status_usuario,
    count(DISTINCT db.cidadao_nome || db.dt_nascimento) AS total_usuarios_com_hipertensao,
    count(DISTINCT
        CASE
            WHEN db.status_em_dia = 'Em dia'::text THEN db.cidadao_nome || db.dt_nascimento
            ELSE NULL::text
        END) AS total_com_consulta_afericao_pa_em_dia,
    count(DISTINCT
        CASE
            WHEN db.id_tipo_de_diagnostico = 1 THEN db.cidadao_nome || db.dt_nascimento
            ELSE NULL::text
        END) AS total_com_diagnostico_autorreferido,
    count(DISTINCT
        CASE
            WHEN db.id_tipo_de_diagnostico = 2 THEN db.cidadao_nome || db.dt_nascimento
            ELSE NULL::text
        END) AS total_com_diagnostico_clinico,
    CURRENT_TIMESTAMP AS criacao_data,
    CURRENT_TIMESTAMP AS atualizacao_data
   FROM impulso_previne_dados_nominais.api_futuro_painel_hipertensos_lista_nominal db
  GROUP BY db.municipio_id_sus, db.equipe_ine, db.equipe_nome, db.id_faixa_etaria, db.id_tipo_de_diagnostico, db.acs_nome, db.id_status_usuario
WITH DATA;