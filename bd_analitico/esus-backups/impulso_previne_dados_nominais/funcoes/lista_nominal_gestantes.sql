CREATE OR REPLACE FUNCTION impulso_previne_dados_nominais.lista_nominal_gestantes()
 RETURNS TABLE(municipio_id_sus character varying, chave_gestacao text, ordem_gestacao text, chave_gestante character varying, gestante_telefone character varying, gestante_nome character varying, gestante_data_de_nascimento date, estabelecimento_cnes text, estabelecimento_nome text, equipe_ine text, equipe_nome text, acs_nome text, acs_data_ultima_visita date, ordem_primeira_consulta_com_dum bigint, data_atendimento_com_primeira_dum_valida date, gestacao_data_dum date, gestacao_data_dpp date, gestacao_dpp_dias_para integer, gestacao_quadrimestre text, gestacao_idade_gestacional_primeiro_atendimento integer, consulta_prenatal_primeira_data date, consulta_prenatal_ultima_data date, consulta_prenatal_ultima_dias_desde integer, data_fim_primeira_gestacao date, tipo_encerramento_primeira_gestacao text, gestante_documento_cpf character varying, gestante_documento_cns character varying, gestacao_idade_gestacional_atual integer, sinalizacao_erro_registro text, gestacao_qtde_dums text, consultas_prenatal_total bigint, consultas_pre_natal_validas bigint, atendimento_odontologico_realizado boolean, exame_hiv_realizado boolean, exame_sifilis_realizado boolean, possui_registro_aborto text, possui_registro_parto text, exame_sifilis_hiv_realizado boolean, atualizacao_data timestamp with time zone, criacao_data timestamp with time zone)
 LANGUAGE plpgsql
AS $function$
DECLARE
    r RECORD;
BEGIN
     RETURN QUERY EXECUTE FORMAT('WITH base_atendimentos_pre_natal AS (
         WITH base AS (
                 SELECT DISTINCT b_1.municipio_id_sus,
                    b_1.id_registro,
                    b_1.data_registro AS data_atendimento,
                    b_1.chave_gestante,
                    b_1.profissional_nome_atendimento,
                        CASE
                            WHEN b_1.data_dum <> ''3000-12-31''::date THEN b_1.data_dum
                            WHEN b_1.idade_gestacional_atendimento IS NOT NULL THEN (b_1.data_registro - ''7 days''::interval * b_1.idade_gestacional_atendimento::double precision)::date
                            ELSE NULL::date
                        END AS data_dum_atendimento,
                        CASE
                            WHEN b_1.data_dum <> ''3000-12-31''::date THEN (b_1.data_dum + ''294 days''::interval)::date
                            WHEN b_1.idade_gestacional_atendimento IS NOT NULL THEN (b_1.data_registro - ''7 days''::interval * b_1.idade_gestacional_atendimento::double precision + ''294 days''::interval)::date
                            ELSE NULL::date
                        END AS data_dpp_atendimento,
                        CASE
                            WHEN b_1.data_dum <> ''3000-12-31''::date THEN (CURRENT_DATE - b_1.data_dum) / 7
                            WHEN b_1.idade_gestacional_atendimento IS NOT NULL THEN (CURRENT_DATE - (b_1.data_registro - ''7 days''::interval * b_1.idade_gestacional_atendimento::double precision)::date) / 7
                            ELSE NULL::integer
                        END AS gestante_idade_gestacional,
                        CASE
                            WHEN b_1.idade_gestacional_atendimento IS NOT NULL THEN b_1.idade_gestacional_atendimento
                            WHEN b_1.data_dum <> ''3000-12-31''::date THEN (b_1.data_registro - b_1.data_dum) / 7
                            ELSE NULL::integer
                        END AS gestante_idade_gestacional_atendimento
                  FROM impulso_previne_dados_nominais.unir_tabelas_lista_nominal_gestantes() b_1(municipio_id_sus, id_registro, tipo_registro, data_registro, chave_gestante, gestante_nome, gestante_data_de_nascimento, gestante_documento_cpf, gestante_documento_cns, gestante_telefone, data_dum, idade_gestacional_atendimento, profissional_cns_atendimento, profissional_nome_atendimento, estabelecimento_cnes_atendimento, estabelecimento_nome_atendimento, equipe_ine_atendimento, equipe_nome_atendimento, data_ultimo_cadastro_individual, estabelecimento_cnes_cad_indivual, estabelecimento_nome_cad_individual, equipe_ine_cad_individual, equipe_nome_cad_individual, data_ultima_visita_acs, acs_visita_domiciliar, acs_cad_dom_familia, acs_cad_individual, criacao_data, atualizacao_data)
                  WHERE b_1.tipo_registro::text = ''consulta_pre_natal''::text
                )
         SELECT b.municipio_id_sus,
            b.id_registro,
            b.data_atendimento,
            b.chave_gestante,
            b.profissional_nome_atendimento,
            b.data_dum_atendimento,
            b.data_dpp_atendimento,
            b.gestante_idade_gestacional,
            b.gestante_idade_gestacional_atendimento,
            (array_agg(b.data_atendimento) FILTER (WHERE b.data_dum_atendimento IS NOT NULL) OVER (PARTITION BY b.chave_gestante ORDER BY b.id_registro ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS primeira_data_consulta_pre_natal_com_dum,
            (array_agg(b.data_dum_atendimento) FILTER (WHERE b.data_dum_atendimento IS NOT NULL) OVER (PARTITION BY b.chave_gestante ORDER BY b.id_registro ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS primeira_data_dum_valida
           FROM base b
        ), validacao_dum AS (
         SELECT apn.municipio_id_sus,
            apn.chave_gestante,
            count(DISTINCT apn.id_registro) AS consultas_pre_natal,
            count(DISTINCT
                CASE
                    WHEN apn.data_dum_atendimento IS NOT NULL THEN apn.data_dum_atendimento
                    ELSE NULL::date
                END) AS cont_data_dum_validas,
            count(DISTINCT
                CASE
                    WHEN apn.data_dum_atendimento IS NULL THEN apn.id_registro
                    ELSE NULL::character varying
                END) AS atend_dum_invalida,
            max(apn.primeira_data_dum_valida) AS primeira_data_dum_valida,
            max(
                CASE
                    WHEN apn.data_dum_atendimento IS NOT NULL THEN apn.data_dum_atendimento
                    ELSE NULL::date
                END) AS maior_data_dum,
            min(
                CASE
                    WHEN apn.data_dum_atendimento IS NOT NULL THEN apn.data_dum_atendimento
                    ELSE NULL::date
                END) AS menor_data_dum,
            max(
                CASE
                    WHEN apn.data_dum_atendimento IS NOT NULL THEN apn.data_dum_atendimento
                    ELSE NULL::date
                END) - min(
                CASE
                    WHEN apn.data_dum_atendimento IS NOT NULL THEN apn.data_dum_atendimento
                    ELSE NULL::date
                END) AS diff_maior_menor_data_dum,
            max(apn.primeira_data_consulta_pre_natal_com_dum) AS primeira_data_consulta_pre_natal_com_dum,
            max(apn.data_atendimento) AS maior_data_consulta_pre_natal,
            min(apn.data_atendimento) AS menor_data_consulta_pre_natal,
            max(apn.data_atendimento) - min(apn.data_atendimento) AS diff_maior_menor_data_consulta_pre_natal,
            max(apn.primeira_data_dum_valida) + ''294 days''::interval AS primeira_data_dpp,
            max(apn.data_dpp_atendimento) AS maior_data_dpp,
            min(apn.data_dpp_atendimento) AS menor_data_dpp,
            max(apn.data_dpp_atendimento) - min(apn.data_dpp_atendimento) AS diff_maior_menor_data_dpp
           FROM base_atendimentos_pre_natal apn
          GROUP BY apn.municipio_id_sus, apn.chave_gestante
        ), validacao_registros_parto AS (
         SELECT b.municipio_id_sus,
            b.chave_gestante,
            count(DISTINCT b.id_registro) AS cont_partos,
            max(b.data_registro) AS maior_data_registro_parto,
            min(b.data_registro) AS menor_data_registro_parto,
            max(b.data_registro) - min(b.data_registro) AS diff_dias_primeio_ultimo_parto
           FROM impulso_previne_dados_nominais.unir_tabelas_lista_nominal_gestantes() b(municipio_id_sus, id_registro, tipo_registro, data_registro, chave_gestante, gestante_nome, gestante_data_de_nascimento, gestante_documento_cpf, gestante_documento_cns, gestante_telefone, data_dum, idade_gestacional_atendimento, profissional_cns_atendimento, profissional_nome_atendimento, estabelecimento_cnes_atendimento, estabelecimento_nome_atendimento, equipe_ine_atendimento, equipe_nome_atendimento, data_ultimo_cadastro_individual, estabelecimento_cnes_cad_indivual, estabelecimento_nome_cad_individual, equipe_ine_cad_individual, equipe_nome_cad_individual, data_ultima_visita_acs, acs_visita_domiciliar, acs_cad_dom_familia, acs_cad_individual, criacao_data, atualizacao_data)
          WHERE b.tipo_registro::text = ''registro_de_parto''::text
          GROUP BY b.municipio_id_sus, b.chave_gestante
        ), validacao_registros_aborto AS (
         SELECT b.municipio_id_sus,
            b.chave_gestante,
            count(DISTINCT b.id_registro) AS cont_abortos,
            max(b.data_registro) AS maior_data_registro_aborto,
            min(b.data_registro) AS menor_data_registro_aborto,
            max(b.data_registro) - min(b.data_registro) AS diff_dias_primeio_ultimo_aborto
           FROM impulso_previne_dados_nominais.unir_tabelas_lista_nominal_gestantes() b(municipio_id_sus, id_registro, tipo_registro, data_registro, chave_gestante, gestante_nome, gestante_data_de_nascimento, gestante_documento_cpf, gestante_documento_cns, gestante_telefone, data_dum, idade_gestacional_atendimento, profissional_cns_atendimento, profissional_nome_atendimento, estabelecimento_cnes_atendimento, estabelecimento_nome_atendimento, equipe_ine_atendimento, equipe_nome_atendimento, data_ultimo_cadastro_individual, estabelecimento_cnes_cad_indivual, estabelecimento_nome_cad_individual, equipe_ine_cad_individual, equipe_nome_cad_individual, data_ultima_visita_acs, acs_visita_domiciliar, acs_cad_dom_familia, acs_cad_individual, criacao_data, atualizacao_data)
          WHERE b.tipo_registro::text = ''registro_de_aborto''::text
          GROUP BY b.municipio_id_sus, b.chave_gestante
        ), analise_gestante AS (
         SELECT vd.municipio_id_sus,
            vd.chave_gestante,
            vd.cont_data_dum_validas,
            vd.primeira_data_dum_valida,
            vd.primeira_data_dpp,
            LEAST(ra.menor_data_registro_aborto::timestamp without time zone, vd.primeira_data_dpp)::date AS data_fim_primeira_gestacao,
                CASE
                    WHEN LEAST(ra.menor_data_registro_aborto::timestamp without time zone, vd.primeira_data_dpp) = ra.menor_data_registro_aborto THEN ''primeira_gestacao_encerrada_registro_aborto''::text
                    WHEN LEAST(ra.menor_data_registro_aborto::timestamp without time zone, vd.primeira_data_dpp, ''2300-01-01''::date::timestamp without time zone) > CURRENT_DATE THEN ''primeira_gestacao_nao_encerrada''::text
                    WHEN LEAST(ra.menor_data_registro_aborto::timestamp without time zone, vd.primeira_data_dpp) = vd.primeira_data_dpp THEN ''primeira_gestacao_encerrada_DPP''::text
                    ELSE NULL::text
                END AS tipo_encerramento_primeira_gestacao,
                CASE
                    WHEN vd.maior_data_consulta_pre_natal >= LEAST(ra.menor_data_registro_aborto::timestamp without time zone, vd.primeira_data_dpp, ''2300-01-01''::date::timestamp without time zone) THEN NULL::text
                    WHEN LEAST(ra.menor_data_registro_aborto::timestamp without time zone, vd.primeira_data_dpp)::date IS NOT NULL THEN NULL::text
                    WHEN vd.diff_maior_menor_data_dum > 90 OR vd.diff_maior_menor_data_consulta_pre_natal > 294 THEN ''possivel_gestante_com_duas_gestacoes_ou_erro_registro_DUM''::text
                    WHEN (CURRENT_DATE - vd.maior_data_consulta_pre_natal) > 294 THEN ''possivel_gestante_com_gestacao_encerrada''::text
                    ELSE NULL::text
                END AS quant_gestacoes,
                CASE
                    WHEN rp.diff_dias_primeio_ultimo_parto > 180 THEN ''possibilidade_dois_partos_ou_erro_registro''::text
                    WHEN rp.diff_dias_primeio_ultimo_parto = 0 THEN NULL
                    ELSE ''possibilidade_apenas_um_parto_ou_erro_registro''::text
                END AS tipo_registro_parto,
                CASE
                    WHEN ra.diff_dias_primeio_ultimo_aborto > 60 THEN ''possibilidade_dois_abortos_ou_erro_registro''::text
                    WHEN ra.diff_dias_primeio_ultimo_aborto = 0 THEN NULL
                    ELSE ''possibilidade_apenas_um_aborto_ou_erro_registro''::text
                END AS tipo_registro_aborto
           FROM validacao_dum vd
             LEFT JOIN validacao_registros_parto rp ON rp.chave_gestante::text = vd.chave_gestante::text
             LEFT JOIN validacao_registros_aborto ra ON ra.chave_gestante::text = vd.chave_gestante::text
        ), base_atendimentos_por_gestacao AS (
         SELECT apn.municipio_id_sus,
            apn.chave_gestante::text || ''_1''::text AS chave_gestacao,
            ''primeira_gestacao_identificada''::text AS ordem_gestacao,
            apn.id_registro,
            apn.chave_gestante,
            apn.data_atendimento,
            apn.profissional_nome_atendimento,
            apn.data_dum_atendimento,
            apn.data_dpp_atendimento,
            apn.gestante_idade_gestacional,
            apn.gestante_idade_gestacional_atendimento,
            row_number() OVER (PARTITION BY apn.chave_gestante ORDER BY apn.id_registro)  AS ordem_consulta_pre_natal_gestacao,
            first_value(apn.data_atendimento) OVER (PARTITION BY apn.chave_gestante, apn.municipio_id_sus ORDER BY apn.data_atendimento) AS data_primeiro_atendimento,
            first_value(apn.data_atendimento) OVER (PARTITION BY apn.chave_gestante, apn.municipio_id_sus ORDER BY apn.data_atendimento DESC) AS data_ultimo_atendimento,
            (array_agg(apn.data_dum_atendimento) FILTER (WHERE apn.data_dum_atendimento IS NOT NULL) OVER (PARTITION BY apn.chave_gestante, apn.municipio_id_sus ORDER BY apn.data_atendimento ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS data_primeira_dum_valida,
            (array_agg(apn.data_atendimento) FILTER (WHERE apn.data_dum_atendimento IS NOT NULL) OVER (PARTITION BY apn.chave_gestante, apn.municipio_id_sus ORDER BY apn.data_atendimento ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS data_atendimento_com_primeira_dum_valida,
            (array_agg(apn.gestante_idade_gestacional_atendimento) FILTER (WHERE apn.data_dum_atendimento IS NOT NULL) OVER (PARTITION BY apn.chave_gestante, apn.municipio_id_sus ORDER BY apn.data_atendimento ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS idade_gestacional_atendimento_com_primeira_dum_valida,
            cg.data_fim_primeira_gestacao,
            cg.tipo_encerramento_primeira_gestacao,
            cg.quant_gestacoes,
            cg.tipo_registro_parto,
            cg.tipo_registro_aborto,
                CASE
                    WHEN apn.data_dpp_atendimento IS NOT NULL AND apn.data_dpp_atendimento < apn.data_atendimento THEN ''registro_de_pre_natal_com_dpp_no_passado''::text
                    ELSE NULL::text
                END AS registro_com_dpp_passado,
                CASE
                    WHEN (apn.data_atendimento - cg.data_fim_primeira_gestacao) >= 0 AND (apn.data_atendimento - cg.data_fim_primeira_gestacao) <= 30 THEN ''possivel_consulta_pos_parto_ou_parto_tardio_ou_erro_DUM''::text
                    ELSE NULL::text
                END AS consulta_proxima_fim_gestacao
           FROM base_atendimentos_pre_natal apn
             JOIN analise_gestante cg ON cg.chave_gestante::text = apn.chave_gestante::text
          WHERE apn.data_atendimento < cg.data_fim_primeira_gestacao OR cg.data_fim_primeira_gestacao IS NULL
        UNION ALL
         SELECT apn.municipio_id_sus,
            apn.chave_gestante::text || ''_2''::text AS chave_gestacao,
            ''segunda_gestacao_identificada''::text AS ordem_gestacao,
            apn.id_registro,
            apn.chave_gestante,
            apn.data_atendimento,
            apn.profissional_nome_atendimento,
            apn.data_dum_atendimento,
            apn.data_dpp_atendimento,
            apn.gestante_idade_gestacional,
            apn.gestante_idade_gestacional_atendimento,
            row_number() OVER (PARTITION BY apn.chave_gestante ORDER BY apn.id_registro) AS ordem_consulta_pre_natal_gestacao,
            --CASE WHEN bag.data AS ordem_consulta_pre_natal_gestacao,
            first_value(apn.data_atendimento) OVER (PARTITION BY apn.chave_gestante, apn.municipio_id_sus ORDER BY apn.data_atendimento) AS data_primeiro_atendimento,
            first_value(apn.data_atendimento) OVER (PARTITION BY apn.chave_gestante, apn.municipio_id_sus ORDER BY apn.data_atendimento DESC) AS data_ultimo_atendimento,
            (array_agg(apn.data_dum_atendimento) FILTER (WHERE apn.data_dum_atendimento IS NOT NULL) OVER (PARTITION BY apn.chave_gestante, apn.municipio_id_sus ORDER BY apn.data_atendimento ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS data_primeira_dum_valida,
            (array_agg(apn.data_atendimento) FILTER (WHERE apn.data_dum_atendimento IS NOT NULL) OVER (PARTITION BY apn.chave_gestante, apn.municipio_id_sus ORDER BY apn.data_atendimento ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS data_atendimento_com_primeira_dum_valida,
            (array_agg(apn.gestante_idade_gestacional_atendimento) FILTER (WHERE apn.data_dum_atendimento IS NOT NULL) OVER (PARTITION BY apn.chave_gestante, apn.municipio_id_sus ORDER BY apn.data_atendimento ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS idade_gestacional_atendimento_com_primeira_dum_valida,
            cg.data_fim_primeira_gestacao,
            cg.tipo_encerramento_primeira_gestacao,
            cg.quant_gestacoes,
            cg.tipo_registro_parto,
            cg.tipo_registro_aborto,
                CASE
                    WHEN apn.data_dpp_atendimento IS NOT NULL AND apn.data_dpp_atendimento < apn.data_atendimento THEN ''registro_de_pre_natal_com_dpp_no_passado''::text
                    ELSE NULL::text
                END AS registro_com_dpp_passado,
                CASE
                    WHEN (apn.data_atendimento - cg.data_fim_primeira_gestacao) >= 0 AND (apn.data_atendimento - cg.data_fim_primeira_gestacao) <= 30 THEN ''possivel_consulta_pos_parto_ou_parto_tardio_ou_erro_DUM''::text
                    ELSE NULL::text
                END AS consulta_proxima_fim_gestacao
           FROM base_atendimentos_pre_natal apn
             JOIN analise_gestante cg ON cg.chave_gestante::text = apn.chave_gestante::text
          WHERE apn.data_atendimento >= cg.data_fim_primeira_gestacao
        )
        , infos_gestante_atendimento_individual_recente AS (
         WITH base AS (
                 SELECT b.municipio_id_sus,
                    b.chave_gestante,
                    b.gestante_nome,
                    b.gestante_data_de_nascimento,
                    (array_agg(b.gestante_documento_cpf) FILTER (WHERE b.gestante_documento_cpf IS NOT NULL) OVER (PARTITION BY b.chave_gestante, b.municipio_id_sus ORDER BY b.data_registro DESC))[1] AS gestante_documento_cpf,
                    (array_agg(b.gestante_documento_cns) FILTER (WHERE b.gestante_documento_cns IS NOT NULL) OVER (PARTITION BY b.chave_gestante, b.municipio_id_sus ORDER BY b.data_registro DESC))[1] AS gestante_documento_cns,
                    b.gestante_telefone,
                    b.estabelecimento_cnes_atendimento,
                    b.estabelecimento_nome_atendimento,
                    b.equipe_ine_atendimento,
                    b.equipe_nome_atendimento,
                    b.data_ultimo_cadastro_individual,
                    b.estabelecimento_cnes_cad_indivual,
                    b.estabelecimento_nome_cad_individual,
                    b.equipe_ine_cad_individual,
                    b.equipe_nome_cad_individual,
                    b.acs_cad_individual,
                    b.data_ultima_visita_acs,
                    b.acs_visita_domiciliar,
                    b.acs_cad_dom_familia,
                    row_number() OVER (PARTITION BY b.chave_gestante, b.municipio_id_sus ORDER BY b.id_registro DESC) = 1 AS ultimo_atendimento_individual
                   FROM impulso_previne_dados_nominais.unir_tabelas_lista_nominal_gestantes() b(municipio_id_sus, id_registro, tipo_registro, data_registro, chave_gestante, gestante_nome, gestante_data_de_nascimento, gestante_documento_cpf, gestante_documento_cns, gestante_telefone, data_dum, idade_gestacional_atendimento, profissional_cns_atendimento, profissional_nome_atendimento, estabelecimento_cnes_atendimento, estabelecimento_nome_atendimento, equipe_ine_atendimento, equipe_nome_atendimento, data_ultimo_cadastro_individual, estabelecimento_cnes_cad_indivual, estabelecimento_nome_cad_individual, equipe_ine_cad_individual, equipe_nome_cad_individual, data_ultima_visita_acs, acs_visita_domiciliar, acs_cad_dom_familia, acs_cad_individual, criacao_data, atualizacao_data)
                  WHERE b.tipo_registro::text = ''consulta_pre_natal''::text
                )
         SELECT base.municipio_id_sus,
            base.chave_gestante,
            base.gestante_nome,
            base.gestante_data_de_nascimento,
            base.gestante_documento_cpf,
            base.gestante_documento_cns,
            base.gestante_telefone,
            base.estabelecimento_cnes_atendimento,
            base.estabelecimento_nome_atendimento,
            base.equipe_ine_atendimento,
            base.equipe_nome_atendimento,
            base.data_ultimo_cadastro_individual,
            base.estabelecimento_cnes_cad_indivual,
            base.estabelecimento_nome_cad_individual,
            base.equipe_ine_cad_individual,
            base.equipe_nome_cad_individual,
            base.acs_cad_individual,
            base.data_ultima_visita_acs,
            base.acs_visita_domiciliar,
            base.acs_cad_dom_familia,
            base.ultimo_atendimento_individual
           FROM base
          WHERE base.ultimo_atendimento_individual IS TRUE
        ), base_final_gestacoes AS (
         SELECT bag.municipio_id_sus,
            bag.chave_gestacao,
            bag.ordem_gestacao,
            bag.chave_gestante,
            ig.gestante_telefone,
            ig.gestante_nome,
            ig.gestante_data_de_nascimento,
            COALESCE(NULLIF(ig.estabelecimento_cnes_cad_indivual::text, ''-''::text), ig.estabelecimento_cnes_atendimento::text) AS estabelecimento_cnes,
            upper(COALESCE(NULLIF(ig.estabelecimento_nome_cad_individual::text, ''Não informado''::text), ig.estabelecimento_nome_atendimento::text)) AS estabelecimento_nome,
            COALESCE(NULLIF(ig.equipe_ine_cad_individual::text, ''-''::text), ig.equipe_ine_atendimento::text) AS equipe_ine,
            upper(COALESCE(NULLIF(ig.equipe_nome_cad_individual::text, ''SEM EQUIPE''::text), ig.equipe_nome_atendimento::text)) AS equipe_nome,
            upper(COALESCE(ig.acs_visita_domiciliar, ig.acs_cad_individual, ''SEM ACS''::character varying)::text) AS acs_nome,
            ig.data_ultima_visita_acs AS acs_data_ultima_visita,
            bag.data_atendimento_com_primeira_dum_valida,
            bag.data_primeira_dum_valida AS gestacao_data_dum,
            (bag.data_primeira_dum_valida + ''294 days''::interval)::date AS gestacao_data_dpp,
            (bag.data_primeira_dum_valida + ''294 days''::interval)::date - CURRENT_DATE AS gestacao_dpp_dias_para,
                CASE
                    WHEN (bag.data_primeira_dum_valida + ''294 days''::interval)::date >= ''2022-01-01''::date AND (bag.data_primeira_dum_valida + ''294 days''::interval)::date <= ''2022-04-30''::date THEN ''2022.Q1''::text
                    WHEN (bag.data_primeira_dum_valida + ''294 days''::interval)::date >= ''2022-05-01''::date AND (bag.data_primeira_dum_valida + ''294 days''::interval)::date <= ''2022-08-31''::date THEN ''2022.Q2''::text
                    WHEN (bag.data_primeira_dum_valida + ''294 days''::interval)::date >= ''2022-09-01''::date AND (bag.data_primeira_dum_valida + ''294 days''::interval)::date <= ''2022-12-31''::date THEN ''2022.Q3''::text
                    WHEN (bag.data_primeira_dum_valida + ''294 days''::interval)::date >= ''2023-01-01''::date AND (bag.data_primeira_dum_valida + ''294 days''::interval)::date <= ''2023-04-30''::date THEN ''2023.Q1''::text
                    WHEN (bag.data_primeira_dum_valida + ''294 days''::interval)::date >= ''2023-05-01''::date AND (bag.data_primeira_dum_valida + ''294 days''::interval)::date <= ''2023-08-31''::date THEN ''2023.Q2''::text
                    WHEN (bag.data_primeira_dum_valida + ''294 days''::interval)::date >= ''2023-09-01''::date AND (bag.data_primeira_dum_valida + ''294 days''::interval)::date <= ''2023-12-31''::date THEN ''2023.Q3''::text
                    WHEN (bag.data_primeira_dum_valida + ''294 days''::interval)::date >= ''2024-01-01''::date AND (bag.data_primeira_dum_valida + ''294 days''::interval)::date <= ''2024-04-30''::date THEN ''2024.Q1''::text
                    WHEN (bag.data_primeira_dum_valida + ''294 days''::interval)::date >= ''2024-05-01''::date AND (bag.data_primeira_dum_valida + ''294 days''::interval)::date <= ''2024-08-31''::date THEN ''2024.Q2''::text
                    WHEN (bag.data_primeira_dum_valida + ''294 days''::interval)::date >= ''2024-09-01''::date AND (bag.data_primeira_dum_valida + ''294 days''::interval)::date <= ''2024-12-31''::date THEN ''2024.Q3''::text
                    WHEN (bag.data_primeira_dum_valida + ''294 days''::interval)::date >= ''2025-01-01''::date AND (bag.data_primeira_dum_valida + ''294 days''::interval)::date <= ''2025-04-30''::date THEN ''2025.Q1''::text
                    WHEN (bag.data_primeira_dum_valida + ''294 days''::interval)::date >= ''2025-05-01''::date AND (bag.data_primeira_dum_valida + ''294 days''::interval)::date <= ''2025-08-31''::date THEN ''2025.Q2''::text
                    WHEN (bag.data_primeira_dum_valida + ''294 days''::interval)::date >= ''2025-09-01''::date AND (bag.data_primeira_dum_valida + ''294 days''::interval)::date <= ''2025-08-31''::date THEN ''2025.Q3''::text
                    ELSE ''SEM QUADRI''::text
                END AS gestacao_quadrimestre,
            bag.idade_gestacional_atendimento_com_primeira_dum_valida AS gestacao_idade_gestacional_primeiro_atendimento,
            bag.data_primeiro_atendimento AS consulta_prenatal_primeira_data,
            bag.data_ultimo_atendimento AS consulta_prenatal_ultima_data,
            CURRENT_DATE - bag.data_ultimo_atendimento AS consulta_prenatal_ultima_dias_desde,
            bag.data_fim_primeira_gestacao,
            bag.tipo_encerramento_primeira_gestacao,
            ig.gestante_documento_cpf,
            ig.gestante_documento_cns,
            MIN(CASE WHEN data_dum_atendimento IS NOT NULL THEN bag.ordem_consulta_pre_natal_gestacao END) AS ordem_primeira_consulta_com_dum,
            max(bag.gestante_idade_gestacional) AS gestacao_idade_gestacional_atual,
            concat(max(bag.quant_gestacoes), '', '', max(bag.tipo_registro_parto), '', '', max(bag.tipo_registro_aborto), '', '', max(bag.registro_com_dpp_passado), '', '', max(bag.consulta_proxima_fim_gestacao)) AS sinalizacao_erro_registro,
                CASE
                    WHEN count(DISTINCT bag.data_dum_atendimento) = 0 THEN ''somente_DUMs_invalidas''::text
                    WHEN count(DISTINCT bag.data_dum_atendimento) > 1 THEN ''mais_de_uma_DUM_valida''::text
                    WHEN count(DISTINCT bag.data_dum_atendimento) = 1 THEN ''uma_DUM_valida''::text
                    ELSE NULL::text
                END AS gestacao_qtde_dums,
            count(DISTINCT bag.id_registro) AS consultas_prenatal_total,
            count(DISTINCT
                CASE
                    WHEN bag.data_atendimento >= bag.data_atendimento_com_primeira_dum_valida AND (bag.profissional_nome_atendimento::text <> ALL (ARRAY[''Não informado''::character varying, ''PROFISSIONAL NÃO CADASTRADO''::character varying]::text[])) THEN bag.id_registro
                    ELSE NULL::character varying
                END) AS consultas_pre_natal_validas,
            count(
                CASE
                    WHEN bag.ordem_gestacao = ''primeira_gestacao_identificada''::text AND odonto.data_registro >= bag.data_primeira_dum_valida AND odonto.data_registro <= bag.data_fim_primeira_gestacao THEN odonto.data_registro
                    WHEN bag.ordem_gestacao = ''segunda_gestacao_identificada''::text AND odonto.data_registro >= bag.data_primeira_dum_valida AND odonto.data_registro <= (bag.data_primeira_dum_valida + ''294 days''::interval)::date THEN odonto.data_registro
                    WHEN bag.data_fim_primeira_gestacao IS NULL AND odonto.data_registro >= bag.data_primeiro_atendimento THEN odonto.data_registro
                    ELSE NULL::date
                END) > 0 AS atendimento_odontologico_realizado,
            count(
                CASE
                    WHEN bag.ordem_gestacao = ''primeira_gestacao_identificada''::text AND hiv.data_registro >= bag.data_primeira_dum_valida AND hiv.data_registro <= bag.data_fim_primeira_gestacao THEN hiv.data_registro
                    WHEN bag.ordem_gestacao = ''segunda_gestacao_identificada''::text AND hiv.data_registro >= bag.data_primeira_dum_valida AND hiv.data_registro <= (bag.data_primeira_dum_valida + ''294 days''::interval)::date THEN hiv.data_registro
                    WHEN bag.data_fim_primeira_gestacao IS NULL AND hiv.data_registro >= bag.data_primeiro_atendimento THEN hiv.data_registro
                    ELSE NULL::date
                END) > 0 AS exame_hiv_realizado,
            count(
                CASE
                    WHEN bag.ordem_gestacao = ''primeira_gestacao_identificada''::text AND sifilis.data_registro >= bag.data_primeira_dum_valida AND sifilis.data_registro <= bag.data_fim_primeira_gestacao THEN sifilis.data_registro
                    WHEN bag.ordem_gestacao = ''segunda_gestacao_identificada''::text AND sifilis.data_registro >= bag.data_primeira_dum_valida AND sifilis.data_registro <= (bag.data_primeira_dum_valida + ''294 days''::interval)::date THEN sifilis.data_registro
                    WHEN bag.data_fim_primeira_gestacao IS NULL AND sifilis.data_registro >= bag.data_primeiro_atendimento THEN sifilis.data_registro
                    ELSE NULL::date
                END) > 0 AS exame_sifilis_realizado,
                CASE
                    WHEN count(
                    CASE
                        WHEN bag.ordem_gestacao = ''primeira_gestacao_identificada''::text AND aborto.data_registro <= bag.data_fim_primeira_gestacao THEN aborto.data_registro
                        WHEN bag.ordem_gestacao = ''segunda_gestacao_identificada''::text AND aborto.data_registro > bag.data_fim_primeira_gestacao THEN aborto.data_registro
                        ELSE NULL::date
                    END) > 0 THEN ''Sim''::text
                    ELSE ''Não''::text
                END AS possui_registro_aborto,
                CASE
                    WHEN count(
                    CASE
                        WHEN bag.ordem_gestacao = ''primeira_gestacao_identificada''::text AND parto.data_registro <= (bag.data_fim_primeira_gestacao + ''180 days''::interval) THEN parto.data_registro
                        WHEN bag.ordem_gestacao = ''segunda_gestacao_identificada''::text AND parto.data_registro > (bag.data_fim_primeira_gestacao + ''180 days''::interval) THEN parto.data_registro
                        ELSE NULL::date
                    END) > 0 THEN ''Sim''::text
                    ELSE ''Não''::text
                END AS possui_registro_parto
           FROM base_atendimentos_por_gestacao bag
             LEFT JOIN infos_gestante_atendimento_individual_recente ig ON bag.chave_gestante::text = ig.chave_gestante::text
             LEFT JOIN impulso_previne_dados_nominais.unir_tabelas_lista_nominal_gestantes() odonto(municipio_id_sus, id_registro, tipo_registro, data_registro, chave_gestante, gestante_nome, gestante_data_de_nascimento, gestante_documento_cpf, gestante_documento_cns, gestante_telefone, data_dum, idade_gestacional_atendimento, profissional_cns_atendimento, profissional_nome_atendimento, estabelecimento_cnes_atendimento, estabelecimento_nome_atendimento, equipe_ine_atendimento, equipe_nome_atendimento, data_ultimo_cadastro_individual, estabelecimento_cnes_cad_indivual, estabelecimento_nome_cad_individual, equipe_ine_cad_individual, equipe_nome_cad_individual, data_ultima_visita_acs, acs_visita_domiciliar, acs_cad_dom_familia, acs_cad_individual, criacao_data, atualizacao_data) ON bag.chave_gestante::text = odonto.chave_gestante::text AND odonto.tipo_registro::text = ''atendimento_odontologico''::text
             LEFT JOIN impulso_previne_dados_nominais.unir_tabelas_lista_nominal_gestantes() sifilis(municipio_id_sus, id_registro, tipo_registro, data_registro, chave_gestante, gestante_nome, gestante_data_de_nascimento, gestante_documento_cpf, gestante_documento_cns, gestante_telefone, data_dum, idade_gestacional_atendimento, profissional_cns_atendimento, profissional_nome_atendimento, estabelecimento_cnes_atendimento, estabelecimento_nome_atendimento, equipe_ine_atendimento, equipe_nome_atendimento, data_ultimo_cadastro_individual, estabelecimento_cnes_cad_indivual, estabelecimento_nome_cad_individual, equipe_ine_cad_individual, equipe_nome_cad_individual, data_ultima_visita_acs, acs_visita_domiciliar, acs_cad_dom_familia, acs_cad_individual, criacao_data, atualizacao_data) ON bag.chave_gestante::text = sifilis.chave_gestante::text AND (sifilis.tipo_registro::text = ANY (ARRAY[''teste_rapido_exame_sifilis''::character varying, ''exame_sifilis_avaliado''::character varying]::text[]))
             LEFT JOIN impulso_previne_dados_nominais.unir_tabelas_lista_nominal_gestantes() hiv(municipio_id_sus, id_registro, tipo_registro, data_registro, chave_gestante, gestante_nome, gestante_data_de_nascimento, gestante_documento_cpf, gestante_documento_cns, gestante_telefone, data_dum, idade_gestacional_atendimento, profissional_cns_atendimento, profissional_nome_atendimento, estabelecimento_cnes_atendimento, estabelecimento_nome_atendimento, equipe_ine_atendimento, equipe_nome_atendimento, data_ultimo_cadastro_individual, estabelecimento_cnes_cad_indivual, estabelecimento_nome_cad_individual, equipe_ine_cad_individual, equipe_nome_cad_individual, data_ultima_visita_acs, acs_visita_domiciliar, acs_cad_dom_familia, acs_cad_individual, criacao_data, atualizacao_data) ON bag.chave_gestante::text = hiv.chave_gestante::text AND (hiv.tipo_registro::text = ANY (ARRAY[''teste_rapido_exame_hiv''::character varying, ''exame_hiv_avaliado''::character varying]::text[]))
             LEFT JOIN impulso_previne_dados_nominais.unir_tabelas_lista_nominal_gestantes() parto(municipio_id_sus, id_registro, tipo_registro, data_registro, chave_gestante, gestante_nome, gestante_data_de_nascimento, gestante_documento_cpf, gestante_documento_cns, gestante_telefone, data_dum, idade_gestacional_atendimento, profissional_cns_atendimento, profissional_nome_atendimento, estabelecimento_cnes_atendimento, estabelecimento_nome_atendimento, equipe_ine_atendimento, equipe_nome_atendimento, data_ultimo_cadastro_individual, estabelecimento_cnes_cad_indivual, estabelecimento_nome_cad_individual, equipe_ine_cad_individual, equipe_nome_cad_individual, data_ultima_visita_acs, acs_visita_domiciliar, acs_cad_dom_familia, acs_cad_individual, criacao_data, atualizacao_data) ON bag.chave_gestante::text = parto.chave_gestante::text AND parto.tipo_registro::text = ''registro_de_parto''::text
             LEFT JOIN impulso_previne_dados_nominais.unir_tabelas_lista_nominal_gestantes() aborto(municipio_id_sus, id_registro, tipo_registro, data_registro, chave_gestante, gestante_nome, gestante_data_de_nascimento, gestante_documento_cpf, gestante_documento_cns, gestante_telefone, data_dum, idade_gestacional_atendimento, profissional_cns_atendimento, profissional_nome_atendimento, estabelecimento_cnes_atendimento, estabelecimento_nome_atendimento, equipe_ine_atendimento, equipe_nome_atendimento, data_ultimo_cadastro_individual, estabelecimento_cnes_cad_indivual, estabelecimento_nome_cad_individual, equipe_ine_cad_individual, equipe_nome_cad_individual, data_ultima_visita_acs, acs_visita_domiciliar, acs_cad_dom_familia, acs_cad_individual, criacao_data, atualizacao_data) ON bag.chave_gestante::text = aborto.chave_gestante::text AND aborto.tipo_registro::text = ''registro_de_aborto''::text
          GROUP BY bag.municipio_id_sus, bag.chave_gestacao, bag.ordem_gestacao, bag.chave_gestante, ig.gestante_telefone, ig.gestante_nome, ig.gestante_data_de_nascimento, (COALESCE(NULLIF(ig.estabelecimento_cnes_cad_indivual::text, ''-''::text), ig.estabelecimento_cnes_atendimento::text)), (upper(COALESCE(NULLIF(ig.estabelecimento_nome_cad_individual::text, ''Não informado''::text), ig.estabelecimento_nome_atendimento::text))), (COALESCE(NULLIF(ig.equipe_ine_cad_individual::text, ''-''::text), ig.equipe_ine_atendimento::text)), (upper(COALESCE(NULLIF(ig.equipe_nome_cad_individual::text, ''SEM EQUIPE''::text), ig.equipe_nome_atendimento::text))), (upper(COALESCE(ig.acs_visita_domiciliar, ig.acs_cad_individual, ''SEM ACS''::character varying)::text)), ig.data_ultima_visita_acs, bag.data_primeira_dum_valida, ((bag.data_primeira_dum_valida + ''294 days''::interval)::date), ((bag.data_primeira_dum_valida + ''294 days''::interval)::date - CURRENT_DATE), (
                CASE
                    WHEN (bag.data_primeira_dum_valida + ''294 days''::interval)::date >= ''2022-01-01''::date AND (bag.data_primeira_dum_valida + ''294 days''::interval)::date <= ''2022-04-30''::date THEN ''2022.Q1''::text
                    WHEN (bag.data_primeira_dum_valida + ''294 days''::interval)::date >= ''2022-05-01''::date AND (bag.data_primeira_dum_valida + ''294 days''::interval)::date <= ''2022-08-31''::date THEN ''2022.Q2''::text
                    WHEN (bag.data_primeira_dum_valida + ''294 days''::interval)::date >= ''2022-09-01''::date AND (bag.data_primeira_dum_valida + ''294 days''::interval)::date <= ''2022-12-31''::date THEN ''2022.Q3''::text
                    WHEN (bag.data_primeira_dum_valida + ''294 days''::interval)::date >= ''2023-01-01''::date AND (bag.data_primeira_dum_valida + ''294 days''::interval)::date <= ''2023-04-30''::date THEN ''2023.Q1''::text
                    WHEN (bag.data_primeira_dum_valida + ''294 days''::interval)::date >= ''2023-05-01''::date AND (bag.data_primeira_dum_valida + ''294 days''::interval)::date <= ''2023-08-31''::date THEN ''2023.Q2''::text
                    WHEN (bag.data_primeira_dum_valida + ''294 days''::interval)::date >= ''2023-09-01''::date AND (bag.data_primeira_dum_valida + ''294 days''::interval)::date <= ''2023-12-31''::date THEN ''2023.Q3''::text
                    WHEN (bag.data_primeira_dum_valida + ''294 days''::interval)::date >= ''2024-01-01''::date AND (bag.data_primeira_dum_valida + ''294 days''::interval)::date <= ''2024-04-30''::date THEN ''2024.Q1''::text
                    WHEN (bag.data_primeira_dum_valida + ''294 days''::interval)::date >= ''2024-05-01''::date AND (bag.data_primeira_dum_valida + ''294 days''::interval)::date <= ''2024-08-31''::date THEN ''2024.Q2''::text
                    WHEN (bag.data_primeira_dum_valida + ''294 days''::interval)::date >= ''2024-09-01''::date AND (bag.data_primeira_dum_valida + ''294 days''::interval)::date <= ''2024-12-31''::date THEN ''2024.Q3''::text
                    WHEN (bag.data_primeira_dum_valida + ''294 days''::interval)::date >= ''2025-01-01''::date AND (bag.data_primeira_dum_valida + ''294 days''::interval)::date <= ''2025-04-30''::date THEN ''2025.Q1''::text
                    WHEN (bag.data_primeira_dum_valida + ''294 days''::interval)::date >= ''2025-05-01''::date AND (bag.data_primeira_dum_valida + ''294 days''::interval)::date <= ''2025-08-31''::date THEN ''2025.Q2''::text
                    WHEN (bag.data_primeira_dum_valida + ''294 days''::interval)::date >= ''2025-09-01''::date AND (bag.data_primeira_dum_valida + ''294 days''::interval)::date <= ''2025-08-31''::date THEN ''2025.Q3''::text
                    ELSE ''SEM QUADRI''::text
                END), bag.idade_gestacional_atendimento_com_primeira_dum_valida, bag.data_primeiro_atendimento, bag.data_ultimo_atendimento, (CURRENT_DATE - bag.data_ultimo_atendimento), bag.data_fim_primeira_gestacao, bag.tipo_encerramento_primeira_gestacao, ig.gestante_documento_cpf, ig.gestante_documento_cns, bag.data_atendimento_com_primeira_dum_valida
        )
 SELECT base_final_gestacoes.municipio_id_sus,
    base_final_gestacoes.chave_gestacao,
    base_final_gestacoes.ordem_gestacao,
    base_final_gestacoes.chave_gestante,
    base_final_gestacoes.gestante_telefone,
    base_final_gestacoes.gestante_nome,
    base_final_gestacoes.gestante_data_de_nascimento,
    base_final_gestacoes.estabelecimento_cnes,
    base_final_gestacoes.estabelecimento_nome,
    base_final_gestacoes.equipe_ine,
    base_final_gestacoes.equipe_nome,
    base_final_gestacoes.acs_nome,
    base_final_gestacoes.acs_data_ultima_visita,
    base_final_gestacoes.ordem_primeira_consulta_com_dum,
    base_final_gestacoes.data_atendimento_com_primeira_dum_valida,
    base_final_gestacoes.gestacao_data_dum,
    base_final_gestacoes.gestacao_data_dpp,
    base_final_gestacoes.gestacao_dpp_dias_para,
    base_final_gestacoes.gestacao_quadrimestre,
    base_final_gestacoes.gestacao_idade_gestacional_primeiro_atendimento,
    base_final_gestacoes.consulta_prenatal_primeira_data,
    base_final_gestacoes.consulta_prenatal_ultima_data,
    base_final_gestacoes.consulta_prenatal_ultima_dias_desde,
    base_final_gestacoes.data_fim_primeira_gestacao,
    base_final_gestacoes.tipo_encerramento_primeira_gestacao,
    base_final_gestacoes.gestante_documento_cpf,
    base_final_gestacoes.gestante_documento_cns,
    base_final_gestacoes.gestacao_idade_gestacional_atual,
    base_final_gestacoes.sinalizacao_erro_registro,
    base_final_gestacoes.gestacao_qtde_dums,
    base_final_gestacoes.consultas_prenatal_total,
    base_final_gestacoes.consultas_pre_natal_validas,
    base_final_gestacoes.atendimento_odontologico_realizado,
    base_final_gestacoes.exame_hiv_realizado,
    base_final_gestacoes.exame_sifilis_realizado,
    base_final_gestacoes.possui_registro_aborto,
    base_final_gestacoes.possui_registro_parto,
        CASE
            WHEN base_final_gestacoes.exame_sifilis_realizado IS TRUE AND base_final_gestacoes.exame_hiv_realizado IS TRUE THEN true
            ELSE false
        END AS exame_sifilis_hiv_realizado,
    now() AS atualizacao_data,
    now() AS criacao_data
   FROM base_final_gestacoes
  WHERE base_final_gestacoes.gestacao_data_dpp >=
        CASE
            WHEN date_part(''month''::text, CURRENT_DATE) >= 1::double precision AND date_part(''month''::text, CURRENT_DATE) <= 4::double precision THEN concat(date_part(''year''::text, (CURRENT_DATE - ''365 days''::interval)::date), ''-09-01'')
            WHEN date_part(''month''::text, CURRENT_DATE) >= 5::double precision AND date_part(''month''::text, CURRENT_DATE) <= 8::double precision THEN concat(date_part(''year''::text, CURRENT_DATE), ''-01-01'')
            WHEN date_part(''month''::text, CURRENT_DATE) >= 9::double precision AND date_part(''month''::text, CURRENT_DATE) <= 12::double precision THEN concat(date_part(''year''::text, CURRENT_DATE), ''-05-01'')
            ELSE NULL::text
        END::date OR base_final_gestacoes.consulta_prenatal_ultima_data >=
        CASE
            WHEN date_part(''month''::text, CURRENT_DATE) >= 1::double precision AND date_part(''month''::text, CURRENT_DATE) <= 4::double precision THEN concat(date_part(''year''::text, (CURRENT_DATE - ''365 days''::interval)::date), ''-09-01'')
            WHEN date_part(''month''::text, CURRENT_DATE) >= 5::double precision AND date_part(''month''::text, CURRENT_DATE) <= 8::double precision THEN concat(date_part(''year''::text, CURRENT_DATE), ''-01-01'')
            WHEN date_part(''month''::text, CURRENT_DATE) >= 9::double precision AND date_part(''month''::text, CURRENT_DATE) <= 12::double precision THEN concat(date_part(''year''::text, CURRENT_DATE), ''-05-01'')
            ELSE NULL::text
        END::date;')
    RETURN;
END;
$function$
;
