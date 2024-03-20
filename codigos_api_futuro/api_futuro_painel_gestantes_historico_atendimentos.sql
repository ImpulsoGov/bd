
-- impulso_previne_dados_nominais.api_futuro_painel_gestantes_historico_atendimentos source

CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.api_futuro_painel_gestantes_historico_atendimentos
TABLESPACE pg_default
AS WITH ordem_consulta_pre_natal AS (
         WITH base AS (
                 SELECT lng.chave_gestacao,
                    rg.chave_gestante,
                    rg.id_registro,
                    rg.data_registro,
                    rg.data_dum,
                    row_number() OVER (PARTITION BY rg.chave_gestante ORDER BY rg.data_registro, rg.id_registro) AS ordem_consulta_pre_natal_gestante,
                    count(
                        CASE
                            WHEN rg.data_dum = '3000-12-31'::date THEN NULL::date
                            ELSE rg.data_dum
                        END) OVER (PARTITION BY rg.chave_gestante ORDER BY rg.data_registro, rg.id_registro) AS cont_dum_preenchida
                   FROM impulso_previne_dados_nominais.eventos_pre_natal rg
                     JOIN impulso_previne_dados_nominais.lista_nominal_gestantes_unificada lng ON rg.chave_gestante::text = lng.chave_gestante::text
                  WHERE rg.tipo_registro::text = 'consulta_pre_natal'::text AND rg.data_registro >= lng.consulta_prenatal_primeira_data AND rg.data_registro <= lng.consulta_prenatal_ultima_data AND lng.ordem_gestacao = 'primeira_gestacao_identificada'::text
                UNION ALL
                 SELECT lng.chave_gestacao,
                    rg.chave_gestante,
                    rg.id_registro,
                    rg.data_registro,
                    rg.data_dum,
                    row_number() OVER (PARTITION BY rg.chave_gestante ORDER BY rg.data_registro, rg.id_registro) AS ordem_consulta_pre_natal_gestante,
                    count(
                        CASE
                            WHEN rg.data_dum = '3000-12-31'::date THEN NULL::date
                            ELSE rg.data_dum
                        END) OVER (PARTITION BY rg.chave_gestante ORDER BY rg.data_registro, rg.id_registro) AS cont_dum_preenchida
                   FROM impulso_previne_dados_nominais.eventos_pre_natal rg
                     JOIN impulso_previne_dados_nominais.lista_nominal_gestantes_unificada lng ON rg.chave_gestante::text = lng.chave_gestante::text
                  WHERE rg.tipo_registro::text = 'consulta_pre_natal'::text AND rg.data_registro >= lng.consulta_prenatal_primeira_data AND rg.data_registro <= lng.consulta_prenatal_ultima_data AND lng.ordem_gestacao = 'segunda_gestacao_identificada'::text
                ), dum_preenchida AS (
                 SELECT b_1.chave_gestacao,
                    b_1.chave_gestante,
                    b_1.id_registro,
                    b_1.data_registro,
                    b_1.data_dum,
                    b_1.ordem_consulta_pre_natal_gestante,
                    b_1.cont_dum_preenchida,
                    first_value(
                        CASE
                            WHEN b_1.data_dum = '3000-12-31'::date THEN NULL::date
                            ELSE b_1.data_dum
                        END) OVER (PARTITION BY b_1.chave_gestacao, b_1.cont_dum_preenchida ORDER BY b_1.data_registro, b_1.id_registro) AS dt_dum_preenchida
                   FROM base b_1
                )
         SELECT b.chave_gestacao,
            b.chave_gestante,
            b.id_registro,
            b.data_registro,
            b.data_dum,
            b.ordem_consulta_pre_natal_gestante,
            b.dt_dum_preenchida,
            lag(b.dt_dum_preenchida) OVER (PARTITION BY b.chave_gestacao ORDER BY b.data_registro, b.id_registro) AS ultima_dt_dum_preenchida
           FROM dum_preenchida b
        )
 SELECT rg.municipio_id_sus,
    rg.chave_gestante,
    lng.ordem_gestacao,
    rg.id_registro,
    oc.ordem_consulta_pre_natal_gestante || 'ª consulta realizada'::text AS tipo_registro,
    rg.data_registro,
    oc.dt_dum_preenchida AS data_dum,
        CASE
            WHEN lng.ordem_primeira_consulta_com_dum = oc.ordem_consulta_pre_natal_gestante THEN '1ª consulta considerada pelo Ministério (Previne Brasil)'::text
            ELSE NULL::text
        END AS obs_titulo_registro,
        CASE
            WHEN oc.dt_dum_preenchida <> oc.ultima_dt_dum_preenchida AND oc.ultima_dt_dum_preenchida IS NOT NULL THEN 'Nova DUM preenchida - '::text || oc.dt_dum_preenchida::text
            WHEN oc.dt_dum_preenchida IS NULL OR oc.ordem_consulta_pre_natal_gestante < lng.ordem_primeira_consulta_com_dum THEN 'DUM não foi preenchida'::text
            WHEN oc.dt_dum_preenchida IS NOT NULL AND lng.ordem_primeira_consulta_com_dum = oc.ordem_consulta_pre_natal_gestante THEN 'DUM preenchida - '::text || oc.dt_dum_preenchida::text
            ELSE NULL::text
        END AS sinalizacao_registro,
        CASE
            WHEN lng.ordem_primeira_consulta_com_dum = oc.ordem_consulta_pre_natal_gestante AND oc.ordem_consulta_pre_natal_gestante > 1 THEN 'Essa foi a primeira consulta com DUM preenchida, ou com uma DUM inserida corretamente. De acordo com a regra do Ministério da Saúde para o programa Previne Brasil, essa será a primeira consulta contabilizada para o indicador de consultas de pré-natal.'::text
            WHEN oc.ordem_consulta_pre_natal_gestante < lng.ordem_primeira_consulta_com_dum THEN 'O ministério só reconhecerá a gestante quando a DUM for preenchida e somente as consultas posteriores ao preenchimento da DUM serão contabilizadas no Previne Brasil. Lembre-se de preencher a DUM na próxima consulta.'::text
            WHEN oc.dt_dum_preenchida <> oc.ultima_dt_dum_preenchida AND oc.ultima_dt_dum_preenchida IS NOT NULL THEN 'Uma nova DUM foi preenchida dentro de um mesmo ciclo gestacional já aberto. Para os indicadores de pré-natal do Previne Brasil, apenas a primeira DUM preenchida e a primeira DPP projetada serão consideradas. Verifique o prontuário.'::text
            WHEN oc.dt_dum_preenchida IS NULL THEN 'O ministério só reconhecerá a gestante quando a DUM for preenchida. Somente as consultas posteriores ao preenchimento da DUM serão contabilizadas para o Previne Brasil. Lembre-se de preencher a DUM na próxima consulta. Atenção: uma DUM preenchida erroneamente pode prejudicar a identificação desta gestante.'::text
            WHEN lng.sinalizacao_erro_registro ~~ '%possivel_consulta_pos_parto_ou_parto_tardio_ou_erro_DUM%'::text THEN 'Consulta com CID/CIAP de gravidez identificada após uma finalização de gestação. Verifique o prontuário.'::text
            ELSE NULL::text
        END AS obs_aviso_registro
   FROM impulso_previne_dados_nominais.eventos_pre_natal rg
     JOIN ordem_consulta_pre_natal oc ON oc.id_registro::text = rg.id_registro::text AND oc.chave_gestante::text = rg.chave_gestante::text
     JOIN impulso_previne_dados_nominais.lista_nominal_gestantes_unificada lng ON oc.chave_gestacao = lng.chave_gestacao
  WHERE rg.tipo_registro::text = 'consulta_pre_natal'::text
UNION ALL
 SELECT DISTINCT rg.municipio_id_sus,
    rg.chave_gestante,
    NULL::text AS ordem_gestacao,
    rg.id_registro,
        CASE
            WHEN rg.tipo_registro::text = 'atendimento_odontologico'::text THEN 'Atendimento Odontológico'::text
            WHEN rg.tipo_registro::text = 'exame_sifilis_avaliado'::text THEN 'Coleta do exame de Sífilis'::text
            WHEN rg.tipo_registro::text = 'teste_rapido_exame_sifilis'::text THEN 'Coleta do exame de Sífilis - teste rápido'::text
            WHEN rg.tipo_registro::text = 'exame_hiv_avaliado'::text THEN 'Coleta do exame de HIV'::text
            WHEN rg.tipo_registro::text = 'teste_rapido_exame_hiv'::text THEN 'Coleta do exame de HIV - teste rápido'::text
            WHEN rg.tipo_registro::text = 'registro_de_parto'::text THEN 'Registro de parto identificado'::text
            WHEN rg.tipo_registro::text = 'registro_de_aborto'::text THEN 'Registro de aborto identificado'::text
            ELSE NULL::text
        END AS tipo_registro,
    rg.data_registro,
    NULL::date AS data_dum,
    NULL::text AS obs_titulo_registro,
    NULL::text AS sinalizacao_registro,
        CASE
            WHEN rg.tipo_registro::text = 'registro_de_aborto'::text THEN 'Quando há registro de aborto em prontuário, a gestante deixa de ser contabilizada para os indicadores de pré-natal.'::text
            ELSE NULL::text
        END AS obs_aviso_registro
   FROM impulso_previne_dados_nominais.eventos_pre_natal rg
     JOIN impulso_previne_dados_nominais.lista_nominal_gestantes_unificada lng ON rg.chave_gestante::text = lng.chave_gestante::text
  WHERE rg.tipo_registro::text <> 'consulta_pre_natal'::text
UNION ALL
 SELECT DISTINCT lng.municipio_id_sus,
    lng.chave_gestante,
    lng.ordem_gestacao,
    '0000000'::text AS id_registro,
    'DUM - Início da Gestação'::text AS tipo_registro,
    lng.gestacao_data_dum AS data_registro,
    NULL::date AS data_dum,
    NULL::text AS obs_titulo_registro,
    NULL::text AS sinalizacao_registro,
        CASE
            WHEN lng.gestacao_data_dum IS NULL THEN 'Essa gestante ainda não possui uma DUM preenchida. Qualquer produção (atendimento odontológico e coleta de exames)feita após a data DUM só será mostrada acima e contabilizada pelo ministério após o preenchimento da DUM. Atenção: a DUM preenchida erroneamente pode prejudicar a identificação desta gestante.'::text
            WHEN lng.gestacao_data_dpp < lng.consulta_prenatal_primeira_data THEN 'Atenção! A primeira DUM identificada é anterior a 9 meses da data do seu registro. Dessa maneira, esta gestação foi encerrada no mesmo momento pelo Ministério da Saúde considerando que a DPP projetada também foi anterior à data atual da consulta.'::text
            ELSE NULL::text
        END AS obs_aviso_registro
   FROM impulso_previne_dados_nominais.lista_nominal_gestantes_unificada lng
UNION ALL
 SELECT DISTINCT lng.municipio_id_sus,
    lng.chave_gestante,
    lng.ordem_gestacao,
    '9999999'::text AS id_registro,
    'DPP alcançada'::text AS tipo_registro,
    lng.gestacao_data_dpp AS data_registro,
    NULL::date AS data_dum,
    NULL::text AS obs_titulo_registro,
        CASE
            WHEN lng.possui_registro_parto = 'Não'::text THEN 'Registro de parto não identificado'::text
            ELSE NULL::text
        END AS sinalizacao_registro,
        CASE
            WHEN lng.possui_registro_parto = 'Não'::text THEN 'A gestação foi encerrada automaticamente na base do Ministério da Saúde a partir do 14º dia após a DPP projetada com a primeira inserção de DUM. Porém, não houve registro de parto identificado no prontuário da usuária. Verifique o prontuário.'::text
            WHEN lng.possui_registro_parto = 'Sim'::text THEN 'Esta gestação foi encerrada tanto no prontuário da usuária quanto automaticamente na base do Ministério da Saúde a partir do 14º dia após a DPP projetada com a primeira inserção de DUM.'::text
            ELSE NULL::text
        END AS obs_aviso_registro
   FROM impulso_previne_dados_nominais.lista_nominal_gestantes_unificada lng
  WHERE lng.gestacao_data_dpp <= CURRENT_DATE AND lng.possui_registro_aborto = 'Não'::text
WITH DATA;
