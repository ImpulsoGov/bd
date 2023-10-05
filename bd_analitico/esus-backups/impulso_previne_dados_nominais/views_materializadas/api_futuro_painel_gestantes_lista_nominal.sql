
CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.api_futuro_painel_gestantes_lista_nominal
TABLESPACE pg_default
AS WITH acs_info AS (
         SELECT DISTINCT tb.chave_gestante,
            tb.acs_visita_domiciliar,
            tb.acs_cad_dom_familia,
            tb.acs_cad_individual
           FROM impulso_previne_dados_nominais.eventos_pre_natal tb
        )
 SELECT tb1.chave_gestacao AS chave_id_gestacao,
    tb1.chave_gestante AS chave_id_gestante,
    tb1.municipio_id_sus,
    tb1.equipe_ine,
    tb1.equipe_nome,
    p.id AS id_quadrimestre_atual,
    tb1.gestante_nome,
    COALESCE(tb1.gestante_documento_cpf::text, tb1.gestante_data_de_nascimento::text) AS gestante_cpf_dt_nascimento,
    tb1.gestacao_idade_gestacional_atual,
    tb1.gestacao_idade_gestacional_primeiro_atendimento,
    tb1.consulta_prenatal_ultima_data,
    tb1.consultas_pre_natal_validas,
        CASE
            WHEN tb1.atendimento_odontologico_realizado_valido THEN 1
            WHEN tb1.atendimento_odontologico_realizado_valido IS FALSE THEN 2
            ELSE 0
        END AS id_atendimeno_odontologico,
        CASE
            WHEN tb1.exame_hiv_realizado_valido AND tb1.exame_sifilis_realizado_valido IS FALSE THEN 1
            WHEN tb1.exame_sifilis_realizado_valido AND tb1.exame_hiv_realizado_valido IS FALSE THEN 2
            WHEN tb1.exame_sifilis_realizado_valido IS FALSE AND tb1.exame_hiv_realizado_valido IS FALSE THEN 3
            WHEN tb1.exame_sifilis_realizado_valido AND tb1.exame_hiv_realizado_valido THEN 4
            ELSE NULL::integer
        END AS id_exame_hiv_sifiis,
    tb1.acs_nome,
    tb1.gestacao_data_dpp AS gestacao_dpp,
        CASE
            WHEN tb1.gestacao_data_dpp < CURRENT_DATE AND (tb1.possui_registro_parto = 'Sim'::text OR tb1.possui_registro_aborto = 'Sim'::text) THEN 9
            WHEN tb1.gestacao_data_dpp > CURRENT_DATE AND tb1.possui_registro_parto = 'Não'::text AND tb1.possui_registro_aborto = 'Não'::text THEN 10
            WHEN tb1.gestacao_data_dpp > CURRENT_DATE AND (tb1.possui_registro_parto = 'Sim'::text OR tb1.possui_registro_aborto = 'Sim'::text) THEN 9
            WHEN tb1.gestacao_data_dpp < CURRENT_DATE AND tb1.possui_registro_parto = 'Não'::text AND tb1.possui_registro_aborto = 'Não'::text THEN 8
            WHEN tb1.gestacao_data_dpp IS NULL THEN 11
            ELSE NULL::integer
        END AS id_status_usuario,
        CASE
            WHEN tb1.sinalizacao_erro_registro IS NOT NULL THEN 1
            ELSE 2
        END AS id_erro_registro,
        CASE
            WHEN tb1.possui_registro_parto = 'Sim'::text THEN 1
            WHEN tb1.possui_registro_parto = 'Não'::text THEN 2
            ELSE 0
        END AS id_sinalizacao_parto,
        CASE
            WHEN tb1.possui_registro_aborto = 'Sim'::text THEN 1
            WHEN tb1.possui_registro_aborto = 'Não'::text THEN 2
            ELSE 0
        END AS id_sinalizacao_aborto,
    tb1.ordem_primeira_consulta_com_dum,
    acs.acs_visita_domiciliar,
    acs.acs_cad_dom_familia,
    acs.acs_cad_individual,
    tb1.atualizacao_data,
    tb1.criacao_data
   FROM impulso_previne_dados_nominais.lista_nominal_gestantes_unificada tb1
     LEFT JOIN listas_de_codigos.periodos p ON tb1.gestacao_quadrimestre = p.codigo::text
     LEFT JOIN acs_info acs ON acs.chave_gestante::text = tb1.chave_gestante::text
WITH DATA;