-- impulso_previne_dados_nominais.analise_comparacao_sisab_2023q1 source

CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.analise_comparacao_sisab_2023q1
TABLESPACE pg_default
AS WITH gestantes_totais AS (
         SELECT l.municipio_id_sus,
            concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
            l.gestacao_quadrimestre,
            count(DISTINCT l.chave_gestacao) AS gestantes_identificadas_unicas,
            count(l.chave_gestacao) AS gestantes_identificadas,
            count(DISTINCT
                CASE
                    WHEN l.possui_registro_aborto = 'Sim'::text THEN l.chave_gestacao
                    ELSE NULL::text
                END) AS gestantes_identificadas_com_aborto,
            count(DISTINCT
                CASE
                    WHEN l.possui_registro_parto = 'Sim'::text THEN l.chave_gestacao
                    ELSE NULL::text
                END) AS gestantes_identificadas_com_parto
           FROM impulso_previne_dados_nominais.lista_nominal_gestantes_unificada l
             LEFT JOIN listas_de_codigos.municipios m ON l.municipio_id_sus::bpchar = m.id_sus
          WHERE l.gestacao_quadrimestre = '2023.Q1'::text
          GROUP BY l.municipio_id_sus, (concat(m.nome, ' - ', m.uf_sigla)), l.gestacao_quadrimestre
        ), gestantes_validas AS (
         SELECT l.municipio_id_sus,
            concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
            l.gestacao_quadrimestre,
            count(DISTINCT l.chave_gestacao) AS gestantes_denominador,
            count(DISTINCT
                CASE
                    WHEN l.possui_registro_parto = 'Sim'::text OR l.gestacao_data_dpp < CURRENT_DATE THEN l.chave_gestacao::character varying
                    ELSE NULL::character varying
                END) AS gestantes_encerradas,
            count(DISTINCT
                CASE
                    WHEN l.gestacao_idade_gestacional_primeiro_atendimento >= 0 AND l.gestacao_idade_gestacional_primeiro_atendimento <= 12 AND l.consultas_prenatal_total < 6 THEN l.chave_gestacao::character varying
                    ELSE NULL::character varying
                END) AS gestantes_1consulta_em_12semanas,
            count(DISTINCT
                CASE
                    WHEN l.gestacao_idade_gestacional_primeiro_atendimento > 12 THEN l.chave_gestacao::character varying
                    ELSE NULL::character varying
                END) AS gestantes_1consulta_apos_12semanas,
            count(DISTINCT
                CASE
                    WHEN l.consultas_prenatal_total > 5 THEN l.chave_gestacao::character varying
                    ELSE NULL::character varying
                END) AS gestantes_6consultas,
            count(DISTINCT
                CASE
                    WHEN l.gestacao_idade_gestacional_primeiro_atendimento >= 0 AND l.gestacao_idade_gestacional_primeiro_atendimento <= 12 AND l.consultas_prenatal_total > 5 THEN l.chave_gestacao::character varying
                    ELSE NULL::character varying
                END) AS gestantes_6consultas_1consulta_em_12semanas,
            count(DISTINCT
                CASE
                    WHEN l.consultas_prenatal_total <= 5 AND l.possui_registro_parto = 'Não'::text AND l.gestacao_data_dpp >= CURRENT_DATE THEN l.chave_gestacao::character varying
                    ELSE NULL::character varying
                END) AS gestantes_ativas_abaixo6consultas,
            count(DISTINCT
                CASE
                    WHEN l.gestacao_idade_gestacional_primeiro_atendimento >= 0 AND l.gestacao_idade_gestacional_primeiro_atendimento <= 12 AND l.consultas_prenatal_total <= 5 AND l.possui_registro_parto = 'Não'::text AND l.gestacao_data_dpp >= CURRENT_DATE THEN l.chave_gestacao::character varying
                    ELSE NULL::character varying
                END) AS gestantes_ativas_abaixo6consultas_1consulta_em_12semanas,
            count(DISTINCT
                CASE
                    WHEN l.exame_sifilis_hiv_realizado IS TRUE THEN l.chave_gestacao::character varying
                    ELSE NULL::character varying
                END) AS gestantes_com_sifilis_hiv_realizado,
            count(DISTINCT
                CASE
                    WHEN l.exame_sifilis_hiv_realizado IS FALSE AND l.possui_registro_parto = 'Não'::text AND l.gestacao_data_dpp >= CURRENT_DATE THEN l.chave_gestacao::character varying
                    ELSE NULL::character varying
                END) AS gestantes_ativas_sem_sifilis_hiv_realizado,
            count(DISTINCT
                CASE
                    WHEN l.atendimento_odontologico_realizado IS TRUE THEN l.chave_gestacao::character varying
                    ELSE NULL::character varying
                END) AS gestantes_odonto_realizado,
            count(DISTINCT
                CASE
                    WHEN l.atendimento_odontologico_realizado IS FALSE AND l.possui_registro_parto = 'Não'::text AND l.gestacao_data_dpp >= CURRENT_DATE THEN l.chave_gestacao::character varying
                    ELSE NULL::character varying
                END) AS gestantes_ativas_sem_odonto_realizado
           FROM impulso_previne_dados_nominais.lista_nominal_gestantes_unificada l
             LEFT JOIN listas_de_codigos.municipios m ON l.municipio_id_sus::bpchar = m.id_sus
          WHERE l.possui_registro_aborto = 'Não'::text AND l.gestacao_quadrimestre = '2023.Q1'::text
          GROUP BY l.municipio_id_sus, (concat(m.nome, ' - ', m.uf_sigla)), l.gestacao_quadrimestre
        ), ind_sisab AS (
         SELECT sisab.municipio_id_ibge,
            sisab.municipio_id_sus,
            sisab.municipio_nome,
            sisab.municipio_uf,
            sisab.periodo_codigo,
            sisab.periodo_id,
            sisab.periodo_data_inicio,
            sisab.periodo_data_fim,
            sisab.indicador_id,
            sisab.indicador_ordem,
            sisab.indicador_prioridade,
            sisab.indicador_nome,
            sisab.indicador_peso,
            sisab.indicador_validade_resultado,
            sisab.indicador_acoes_por_usuario,
            sisab.indicador_numerador,
            sisab.indicador_denominador_estimado,
            sisab.indicador_denominador_utilizado_informado,
            sisab.indicador_denominador_utilizado,
            sisab.indicador_denominador_utilizado_tipo,
            sisab.indicador_denominador_informado_diferenca_utilizado,
            sisab.indicador_denominador_informado_diferenca_utilizado_formatado,
            sisab.indicador_nota,
            sisab.indicador_nota_porcentagem,
            sisab.indicador_meta,
            sisab.indicador_diferenca_meta,
            sisab.indicador_recomendacao,
            sisab.delta,
            sisab.delta_formatado,
            sisab.indicador_usuarios_100_porcento_meta,
            sisab.indicador_usuarios_cadastrados_sem_atendimento,
            sisab.indicador_usuarios_cadastrar_para_meta,
            sisab.indicador_score,
            sisab.criacao_data,
            sisab.atualizacao_data,
            sisab.indicador_denominador_informado
           FROM _impulso_previne_dados_abertos.indicadores_desempenho_score_equipes_validas2 sisab
          WHERE sisab.periodo_codigo::text = '2023.Q1'::text
        ), base AS (
         SELECT gv.municipio_id_sus,
            gv.municipio_uf,
            gv.gestacao_quadrimestre,
            gt.gestantes_identificadas_unicas,
            gt.gestantes_identificadas,
            gt.gestantes_identificadas_com_aborto,
            gt.gestantes_identificadas_com_parto,
            gv.gestantes_denominador,
            gv.gestantes_encerradas,
            gv.gestantes_1consulta_em_12semanas,
            gv.gestantes_1consulta_apos_12semanas,
            gv.gestantes_6consultas,
            gv.gestantes_6consultas_1consulta_em_12semanas,
            gv.gestantes_ativas_abaixo6consultas,
            gv.gestantes_ativas_abaixo6consultas_1consulta_em_12semanas,
            gv.gestantes_com_sifilis_hiv_realizado,
            gv.gestantes_ativas_sem_sifilis_hiv_realizado,
            gv.gestantes_odonto_realizado,
            gv.gestantes_ativas_sem_odonto_realizado,
            s_ind1.indicador_denominador_informado AS denominador_informado_sisab,
            s_ind1.indicador_denominador_estimado AS denominador_estimado_sisab,
            s_ind1.indicador_numerador AS ind1_numerador_sisab,
            s_ind1.indicador_nota_porcentagem AS ind1_resultado_sisab,
            s_ind1.indicador_meta AS ind1_meta_sisab,
            s_ind2.indicador_numerador AS ind2_numerador_sisab,
            s_ind2.indicador_nota_porcentagem AS ind2_resultado_sisab,
            s_ind2.indicador_meta AS ind2_meta_sisab,
            s_ind3.indicador_numerador AS ind3_numerador_sisab,
            s_ind3.indicador_nota_porcentagem AS ind3_resultado_sisab,
            s_ind3.indicador_meta AS ind3_meta_sisab
           FROM gestantes_validas gv
             LEFT JOIN ind_sisab s_ind1 ON s_ind1.municipio_uf = gv.municipio_uf AND s_ind1.indicador_ordem = 1
             LEFT JOIN ind_sisab s_ind2 ON s_ind2.municipio_uf = gv.municipio_uf AND s_ind2.indicador_ordem = 2
             LEFT JOIN ind_sisab s_ind3 ON s_ind3.municipio_uf = gv.municipio_uf AND s_ind3.indicador_ordem = 3
             LEFT JOIN gestantes_totais gt ON gt.municipio_uf = gv.municipio_uf AND gt.gestacao_quadrimestre = gv.gestacao_quadrimestre
        ), base_com_denominador_utilizado AS (
         SELECT b_1.municipio_id_sus,
            b_1.municipio_uf,
            b_1.gestacao_quadrimestre,
            b_1.gestantes_identificadas_unicas,
            b_1.gestantes_identificadas,
            b_1.gestantes_identificadas_com_aborto,
            b_1.gestantes_identificadas_com_parto,
            b_1.gestantes_denominador,
            b_1.denominador_estimado_sisab,
            b_1.denominador_informado_sisab,
            b_1.ind1_numerador_sisab,
            b_1.ind2_numerador_sisab,
            b_1.ind3_numerador_sisab,
            b_1.ind1_resultado_sisab,
            b_1.ind2_resultado_sisab,
            b_1.ind3_resultado_sisab,
            b_1.gestantes_encerradas,
            b_1.gestantes_1consulta_em_12semanas,
            b_1.gestantes_1consulta_apos_12semanas,
            b_1.gestantes_6consultas,
            b_1.gestantes_6consultas_1consulta_em_12semanas,
            b_1.gestantes_ativas_abaixo6consultas,
            b_1.gestantes_ativas_abaixo6consultas_1consulta_em_12semanas,
            b_1.gestantes_com_sifilis_hiv_realizado,
            b_1.gestantes_ativas_sem_sifilis_hiv_realizado,
            b_1.gestantes_odonto_realizado,
            b_1.gestantes_ativas_sem_odonto_realizado,
                CASE
                    WHEN b_1.gestantes_denominador::numeric >= (0.85 * b_1.denominador_estimado_sisab::numeric) THEN b_1.gestantes_denominador
                    WHEN b_1.gestantes_denominador::numeric < (0.85 * b_1.denominador_estimado_sisab::numeric) THEN b_1.denominador_estimado_sisab::bigint
                    ELSE NULL::bigint
                END AS gestantes_denominador_utilizado,
                CASE
                    WHEN b_1.gestantes_denominador::numeric >= (0.85 * b_1.denominador_estimado_sisab::numeric) THEN 'denominador informado'::text
                    WHEN b_1.gestantes_denominador::numeric < (0.85 * b_1.denominador_estimado_sisab::numeric) THEN 'denominador estimado'::text
                    ELSE NULL::text
                END AS tipo_denominador_utilizado
           FROM base b_1
        ), base_com_indicadores AS (
         SELECT b_1.municipio_id_sus,
            b_1.municipio_uf,
            b_1.gestacao_quadrimestre,
            b_1.ind1_numerador_sisab,
            b_1.gestantes_6consultas_1consulta_em_12semanas,
            b_1.gestantes_6consultas_1consulta_em_12semanas - b_1.ind1_numerador_sisab AS ind1_dif_numerador,
            round(b_1.gestantes_6consultas_1consulta_em_12semanas::numeric * 1.00 / b_1.ind1_numerador_sisab::numeric - 1::numeric, 2) AS ind1_dif_numerador_perc,
            b_1.ind2_numerador_sisab,
            b_1.gestantes_com_sifilis_hiv_realizado,
            b_1.gestantes_com_sifilis_hiv_realizado - b_1.ind2_numerador_sisab AS ind2_dif_numerador,
            round(b_1.gestantes_com_sifilis_hiv_realizado::numeric * 1.00 / b_1.ind2_numerador_sisab::numeric - 1::numeric, 2) AS ind2_dif_numerador_perc,
            b_1.ind3_numerador_sisab,
            b_1.gestantes_odonto_realizado,
            b_1.gestantes_odonto_realizado - b_1.ind3_numerador_sisab AS ind3_dif_numerador,
            round(b_1.gestantes_odonto_realizado::numeric * 1.00 / b_1.ind3_numerador_sisab::numeric - 1::numeric, 2) AS ind3_dif_numerador_perc,
            b_1.denominador_estimado_sisab,
            b_1.denominador_informado_sisab,
            b_1.gestantes_denominador,
            b_1.gestantes_denominador - b_1.denominador_informado_sisab AS dif_denominador,
            round(b_1.gestantes_denominador::numeric * 1.00 / b_1.denominador_informado_sisab::numeric - 1::numeric, 2) AS dif_denominador_perc,
            b_1.gestantes_denominador_utilizado,
            b_1.tipo_denominador_utilizado,
            b_1.ind1_resultado_sisab,
            round(b_1.gestantes_6consultas_1consulta_em_12semanas::numeric * 1.00 / b_1.gestantes_denominador_utilizado::numeric * 100::numeric, 2) AS ind1_6consultas_prenatal,
            b_1.ind2_resultado_sisab,
            round(b_1.gestantes_com_sifilis_hiv_realizado::numeric * 1.00 / b_1.gestantes_denominador_utilizado::numeric * 100::numeric, 2) AS ind2_exame_sifilis_hiv,
            b_1.ind3_resultado_sisab,
            round(b_1.gestantes_odonto_realizado::numeric * 1.00 / b_1.gestantes_denominador_utilizado::numeric * 100::numeric, 2) AS ind3_atend_odonto
           FROM base_com_denominador_utilizado b_1
        ), base_gestantes AS (
         SELECT b.municipio_id_sus,
            b.municipio_uf,
            b.gestacao_quadrimestre,
            b.ind1_numerador_sisab,
            b.gestantes_6consultas_1consulta_em_12semanas,
            b.ind1_dif_numerador,
            b.ind1_dif_numerador_perc,
            b.ind2_numerador_sisab,
            b.gestantes_com_sifilis_hiv_realizado,
            b.ind2_dif_numerador,
            b.ind2_dif_numerador_perc,
            b.ind3_numerador_sisab,
            b.gestantes_odonto_realizado,
            b.ind3_dif_numerador,
            b.ind3_dif_numerador_perc,
            b.denominador_estimado_sisab,
            b.denominador_informado_sisab,
            b.gestantes_denominador,
            b.dif_denominador,
            b.dif_denominador_perc,
            b.gestantes_denominador_utilizado,
            b.tipo_denominador_utilizado,
            b.ind1_resultado_sisab,
            b.ind1_6consultas_prenatal,
            round(b.ind1_6consultas_prenatal - b.ind1_resultado_sisab::numeric, 2) AS ind1_dif_perc,
            b.ind2_resultado_sisab,
            b.ind2_exame_sifilis_hiv,
            round(b.ind2_exame_sifilis_hiv - b.ind2_resultado_sisab::numeric, 2) AS ind2_dif_perc,
            b.ind3_resultado_sisab,
            b.ind3_atend_odonto,
            round(b.ind3_atend_odonto - b.ind3_resultado_sisab::numeric, 2) AS ind3_dif_perc
           FROM base_com_indicadores b
          ORDER BY b.municipio_uf, b.gestacao_quadrimestre DESC
        ), transmissao_recente_por_municipio_datas AS (
         SELECT tb1_1.municipio_id_sus,
            max("substring"(tb1_1.periodo_data_transmissao::text, 1, 10))::date AS ultima_transmissao
           FROM impulso_previne_dados_nominais.lista_nominal_hipertensos_historico tb1_1
          WHERE tb1_1.periodo_data_transmissao > '2023-04-24'::date AND tb1_1.periodo_data_transmissao <= '2023-04-28'::date
          GROUP BY tb1_1.municipio_id_sus
        ), dados_transmissoes_recentes AS (
         SELECT tb1_1.municipio_id_sus,
            tb1_1.quadrimestre_atual,
            tb1_1.realizou_afericao_ultimos_6_meses,
            tb1_1.dt_afericao_pressao_mais_recente,
            tb1_1.realizou_consulta_ultimos_6_meses,
            tb1_1.dt_consulta_mais_recente,
            tb1_1.co_seq_fat_cidadao_pec,
            tb1_1.cidadao_cpf,
            tb1_1.cidadao_cns,
            tb1_1.cidadao_nome,
            tb1_1.cidadao_nome_social,
            tb1_1.cidadao_sexo,
            tb1_1.dt_nascimento,
            tb1_1.estabelecimento_cnes_atendimento,
            tb1_1.estabelecimento_cnes_cadastro,
            tb1_1.estabelecimento_nome_atendimento,
            tb1_1.estabelecimento_nome_cadastro,
            tb1_1.equipe_ine_atendimento,
            tb1_1.equipe_ine_cadastro,
            tb1_1.equipe_nome_atendimento,
            tb1_1.equipe_nome_cadastro,
            tb1_1.acs_nome_cadastro,
            tb1_1.acs_nome_visita,
            tb1_1.possui_hipertensao_autorreferida,
            tb1_1.possui_hipertensao_diagnosticada,
            tb1_1.data_ultimo_cadastro,
            tb1_1.dt_ultima_consulta,
            tb1_1.se_faleceu,
            tb1_1.se_mudou,
            tb1_1.periodo_data_transmissao AS criacao_data
           FROM impulso_previne_dados_nominais.lista_nominal_hipertensos_historico tb1_1
             JOIN transmissao_recente_por_municipio_datas tb2_1 ON tb1_1.municipio_id_sus::text = tb2_1.municipio_id_sus::text AND tb1_1.periodo_data_transmissao = tb2_1.ultima_transmissao
        ), lista_hipertensao AS (
         SELECT tb1.municipio_id_sus,
            tb1.cidadao_nome,
            tb1.dt_nascimento,
                CASE
                    WHEN tb1.realizou_consulta_ultimos_6_meses IS FALSE OR tb1.realizou_afericao_ultimos_6_meses IS FALSE THEN 'Não está em dia'::text
                    WHEN tb1.realizou_consulta_ultimos_6_meses AND tb1.realizou_afericao_ultimos_6_meses THEN 'Em dia'::text
                    ELSE NULL::text
                END AS status_em_dia,
                CASE
                    WHEN tb1.cidadao_cpf IS NULL THEN tb1.dt_nascimento::text::character varying::text::character varying
                    ELSE tb1.cidadao_cpf
                END AS cidadao_cpf_dt_nascimento,
            tb1.dt_consulta_mais_recente,
                CASE
                    WHEN tb1.realizou_consulta_ultimos_6_meses THEN 'Em dia'::text
                    ELSE impulso_previne_dados_nominais.prazo_proximo_dia()
                END AS prazo_proxima_consulta,
            tb1.dt_afericao_pressao_mais_recente::date AS dt_afericao_pressao_mais_recente,
                CASE
                    WHEN tb1.realizou_afericao_ultimos_6_meses THEN 'Em dia'::text
                    ELSE impulso_previne_dados_nominais.prazo_proximo_dia()
                END AS prazo_proxima_afericao_pa,
            COALESCE(tb1.acs_nome_visita, tb1.acs_nome_cadastro) AS acs_nome,
            COALESCE(tb1.estabelecimento_cnes_cadastro, tb1.estabelecimento_cnes_atendimento) AS estabelecimento_cnes,
            COALESCE(tb1.estabelecimento_nome_cadastro, tb1.estabelecimento_nome_atendimento) AS estabelecimento_nome,
            impulso_previne_dados_nominais.equipe_ine(tb1.municipio_id_sus::text, COALESCE(tb1.equipe_ine_cadastro, tb1.equipe_ine_atendimento)) AS equipe_ine,
            impulso_previne_dados_nominais.equipe_ine(tb1.municipio_id_sus::text, COALESCE(tb1.equipe_nome_cadastro, tb1.equipe_nome_atendimento)) AS equipe_nome,
                CASE
                    WHEN tb1.possui_hipertensao_diagnosticada THEN 2
                    WHEN tb1.possui_hipertensao_autorreferida AND tb1.possui_hipertensao_diagnosticada IS FALSE THEN 1
                    WHEN tb1.possui_hipertensao_autorreferida AND tb1.possui_hipertensao_diagnosticada IS NULL THEN 1
                    ELSE 0
                END AS id_tipo_de_diagnostico,
                CASE
                    WHEN date_part('year'::text, age(CURRENT_DATE::timestamp with time zone, tb1.dt_nascimento::timestamp with time zone)) <= 40::double precision THEN 1
                    WHEN date_part('year'::text, age(CURRENT_DATE::timestamp with time zone, tb1.dt_nascimento::timestamp with time zone)) > 40::double precision AND date_part('year'::text, age(CURRENT_DATE::timestamp with time zone, tb1.dt_nascimento::timestamp with time zone)) <= 49::double precision THEN 2
                    WHEN date_part('year'::text, age(CURRENT_DATE::timestamp with time zone, tb1.dt_nascimento::timestamp with time zone)) > 49::double precision AND date_part('year'::text, age(CURRENT_DATE::timestamp with time zone, tb1.dt_nascimento::timestamp with time zone)) <= 59::double precision THEN 3
                    WHEN date_part('year'::text, age(CURRENT_DATE::timestamp with time zone, tb1.dt_nascimento::timestamp with time zone)) > 59::double precision AND date_part('year'::text, age(CURRENT_DATE::timestamp with time zone, tb1.dt_nascimento::timestamp with time zone)) <= 70::double precision THEN 4
                    WHEN tb1.dt_nascimento IS NULL THEN 0
                    ELSE 5
                END AS id_faixa_etaria,
                CASE
                    WHEN tb1.realizou_afericao_ultimos_6_meses AND tb1.realizou_consulta_ultimos_6_meses THEN 1
                    WHEN tb1.realizou_afericao_ultimos_6_meses IS FALSE AND tb1.realizou_consulta_ultimos_6_meses IS FALSE THEN 2
                    WHEN tb1.realizou_afericao_ultimos_6_meses AND tb1.realizou_consulta_ultimos_6_meses IS FALSE THEN 3
                    WHEN tb1.realizou_afericao_ultimos_6_meses IS FALSE AND tb1.realizou_consulta_ultimos_6_meses THEN 4
                    ELSE 0
                END AS id_status_usuario,
            tb1.criacao_data,
            CURRENT_TIMESTAMP AS atualizacao_data
           FROM dados_transmissoes_recentes tb1
          WHERE tb1.se_faleceu = 0
        ), aux AS (
         SELECT concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
            lnh.municipio_id_sus,
            lnh.cidadao_nome::text || lnh.dt_nascimento AS id_paciente,
            lnh.status_em_dia
           FROM lista_hipertensao lnh
             JOIN listas_de_codigos.municipios m ON lnh.municipio_id_sus::bpchar = m.id_sus
        ), base_hiper AS (
         SELECT b.municipio_id_sus,
            b.municipio_uf,
            count(
                CASE
                    WHEN b.status_em_dia = 'Em dia'::text THEN b.id_paciente
                    ELSE NULL::text
                END) AS ind6_numerador,
            s_ind6.indicador_numerador AS ind6_numerador_sisab,
            count(DISTINCT b.id_paciente) AS ind6_denominador_identificado_atual,
            s_ind6.indicador_denominador_informado AS ind6_denominador_informado_sisab,
            s_ind6.indicador_denominador_estimado AS ind6_denominador_estimado_sisab,
                CASE
                    WHEN count(DISTINCT b.id_paciente)::numeric >= (0.85 * s_ind6.indicador_denominador_estimado::numeric) THEN count(DISTINCT b.id_paciente)
                    WHEN count(DISTINCT b.id_paciente)::numeric < (0.85 * s_ind6.indicador_denominador_estimado::numeric) THEN s_ind6.indicador_denominador_estimado::bigint
                    ELSE NULL::bigint
                END AS ind6_denominador_utilizado,
            s_ind6.indicador_nota_porcentagem AS ind6_resultado_sisab,
            s_ind6.indicador_meta AS ind6_meta_sisab
           FROM aux b
             LEFT JOIN ind_sisab s_ind6 ON s_ind6.municipio_uf = b.municipio_uf AND s_ind6.indicador_ordem = 6
          WHERE b.municipio_id_sus::text <> ALL (ARRAY['100111'::text, '111111'::text])
          GROUP BY b.municipio_id_sus, b.municipio_uf, s_ind6.indicador_numerador, s_ind6.indicador_denominador_informado, s_ind6.indicador_denominador_estimado, s_ind6.indicador_nota_porcentagem, s_ind6.indicador_meta
        ), base_hipertensao AS (
         SELECT b.municipio_id_sus,
            b.municipio_uf,
            b.ind6_numerador,
            b.ind6_numerador_sisab,
            b.ind6_numerador_sisab - b.ind6_numerador AS ind6_dif_numerador,
            round(b.ind6_numerador::numeric * 1.00 / b.ind6_numerador_sisab::numeric - 1::numeric, 2) AS ind6_dif_numerador_perc,
            b.ind6_denominador_identificado_atual,
            b.ind6_denominador_utilizado,
            b.ind6_denominador_informado_sisab,
            b.ind6_denominador_identificado_atual - b.ind6_denominador_informado_sisab AS ind6_dif_denominador,
            b.ind6_denominador_estimado_sisab,
            b.ind6_resultado_sisab,
            round(b.ind6_numerador::numeric * 1.00 / b.ind6_denominador_utilizado::numeric * 100::numeric, 2) AS ind6_hipertensao,
            round(b.ind6_numerador::numeric * 1.00 / b.ind6_denominador_utilizado::numeric * 100::numeric - b.ind6_resultado_sisab::numeric, 2) AS ind6_dif_perc,
            b.ind6_meta_sisab
           FROM base_hiper b
          ORDER BY b.municipio_uf
        ), transmissao_recente_por_municipio_dia AS (
         SELECT tb1_1.municipio_id_sus,
            max(tb1_1.periodo_data_transmissao) AS ultima_transmissao
           FROM impulso_previne_dados_nominais.lista_nominal_diabeticos_historico tb1_1
          WHERE tb1_1.periodo_data_transmissao <= '2023-04-28'::date AND tb1_1.periodo_data_transmissao >= '2023-04-24'::date
          GROUP BY tb1_1.municipio_id_sus
        ), dados_transmissoes_recentes_dia AS (
         SELECT tb1_1.municipio_id_sus,
            tb1_1.quadrimestre_atual,
            tb1_1.realizou_solicitacao_hemoglobina_ultimos_6_meses,
            tb1_1.dt_solicitacao_hemoglobina_glicada_mais_recente,
            tb1_1.realizou_consulta_ultimos_6_meses,
            tb1_1.dt_consulta_mais_recente,
            tb1_1.co_seq_fat_cidadao_pec,
            tb1_1.cidadao_cpf,
            tb1_1.cidadao_cns,
            tb1_1.cidadao_nome,
            tb1_1.cidadao_nome_social,
            tb1_1.cidadao_sexo,
            tb1_1.dt_nascimento,
            tb1_1.estabelecimento_cnes_atendimento,
            tb1_1.estabelecimento_cnes_cadastro,
            tb1_1.estabelecimento_nome_atendimento,
            tb1_1.estabelecimento_nome_cadastro,
            tb1_1.equipe_ine_atendimento,
            tb1_1.equipe_ine_cadastro,
            tb1_1.equipe_nome_atendimento,
            tb1_1.equipe_nome_cadastro,
            tb1_1.acs_nome_cadastro,
            tb1_1.acs_nome_visita,
            tb1_1.possui_diabetes_autoreferida,
            tb1_1.possui_diabetes_diagnosticada,
            tb1_1.data_ultimo_cadastro,
            tb1_1.dt_ultima_consulta,
            tb1_1.se_faleceu,
            tb1_1.se_mudou,
            tb1_1.periodo_data_transmissao AS criacao_data
           FROM impulso_previne_dados_nominais.lista_nominal_diabeticos_historico tb1_1
             JOIN transmissao_recente_por_municipio_dia tb2_1 ON tb1_1.municipio_id_sus::text = tb2_1.municipio_id_sus::text AND tb1_1.periodo_data_transmissao = tb2_1.ultima_transmissao
        ), lista_diabetes AS (
         SELECT tb1.municipio_id_sus,
            tb1.cidadao_nome,
            tb1.dt_nascimento,
                CASE
                    WHEN tb1.realizou_consulta_ultimos_6_meses IS FALSE OR tb1.realizou_solicitacao_hemoglobina_ultimos_6_meses IS FALSE THEN 'Não está em dia'::text
                    WHEN tb1.realizou_consulta_ultimos_6_meses AND tb1.realizou_solicitacao_hemoglobina_ultimos_6_meses THEN 'Em dia'::text
                    ELSE NULL::text
                END AS status_em_dia,
                CASE
                    WHEN tb1.cidadao_cpf IS NULL THEN tb1.dt_nascimento::text::character varying::text::character varying
                    ELSE tb1.cidadao_cpf
                END AS cidadao_cpf_dt_nascimento,
            tb1.dt_consulta_mais_recente,
                CASE
                    WHEN tb1.realizou_consulta_ultimos_6_meses THEN 'Em dia'::text
                    ELSE impulso_previne_dados_nominais.prazo_proximo_dia()
                END AS prazo_proxima_consulta,
            tb1.dt_solicitacao_hemoglobina_glicada_mais_recente::date AS dt_solicitacao_hemoglobina_glicada_mais_recente,
                CASE
                    WHEN tb1.realizou_solicitacao_hemoglobina_ultimos_6_meses THEN 'Em dia'::text
                    ELSE impulso_previne_dados_nominais.prazo_proximo_dia()
                END AS prazo_proxima_solicitacao_hemoglobina,
            COALESCE(tb1.acs_nome_visita, tb1.acs_nome_cadastro) AS acs_nome,
            COALESCE(tb1.estabelecimento_cnes_cadastro, tb1.estabelecimento_cnes_atendimento) AS estabelecimento_cnes,
            COALESCE(tb1.estabelecimento_nome_cadastro, tb1.estabelecimento_nome_atendimento) AS estabelecimento_nome,
            impulso_previne_dados_nominais.equipe_ine(tb1.municipio_id_sus::text, COALESCE(tb1.equipe_ine_cadastro, tb1.equipe_ine_atendimento)) AS equipe_ine,
            impulso_previne_dados_nominais.equipe_ine(tb1.municipio_id_sus::text, COALESCE(tb1.equipe_nome_cadastro, tb1.equipe_nome_atendimento)) AS equipe_nome,
                CASE
                    WHEN tb1.possui_diabetes_diagnosticada THEN 2
                    WHEN tb1.possui_diabetes_autoreferida AND tb1.possui_diabetes_diagnosticada IS FALSE THEN 1
                    WHEN tb1.possui_diabetes_autoreferida AND tb1.possui_diabetes_diagnosticada IS NULL THEN 1
                    ELSE 0
                END AS id_tipo_de_diagnostico,
                CASE
                    WHEN date_part('year'::text, age(CURRENT_DATE::timestamp with time zone, tb1.dt_nascimento::timestamp with time zone)) <= 40::double precision THEN 1
                    WHEN date_part('year'::text, age(CURRENT_DATE::timestamp with time zone, tb1.dt_nascimento::timestamp with time zone)) > 40::double precision AND date_part('year'::text, age(CURRENT_DATE::timestamp with time zone, tb1.dt_nascimento::timestamp with time zone)) <= 49::double precision THEN 2
                    WHEN date_part('year'::text, age(CURRENT_DATE::timestamp with time zone, tb1.dt_nascimento::timestamp with time zone)) > 49::double precision AND date_part('year'::text, age(CURRENT_DATE::timestamp with time zone, tb1.dt_nascimento::timestamp with time zone)) <= 59::double precision THEN 3
                    WHEN date_part('year'::text, age(CURRENT_DATE::timestamp with time zone, tb1.dt_nascimento::timestamp with time zone)) > 59::double precision AND date_part('year'::text, age(CURRENT_DATE::timestamp with time zone, tb1.dt_nascimento::timestamp with time zone)) <= 70::double precision THEN 4
                    WHEN tb1.dt_nascimento IS NULL THEN 0
                    ELSE 5
                END AS id_faixa_etaria,
                CASE
                    WHEN tb1.realizou_solicitacao_hemoglobina_ultimos_6_meses AND tb1.realizou_consulta_ultimos_6_meses THEN 1
                    WHEN tb1.realizou_solicitacao_hemoglobina_ultimos_6_meses IS FALSE AND tb1.realizou_consulta_ultimos_6_meses IS FALSE THEN 2
                    WHEN tb1.realizou_solicitacao_hemoglobina_ultimos_6_meses AND tb1.realizou_consulta_ultimos_6_meses IS FALSE THEN 3
                    WHEN tb1.realizou_solicitacao_hemoglobina_ultimos_6_meses IS FALSE AND tb1.realizou_consulta_ultimos_6_meses THEN 4
                    ELSE 0
                END AS id_status_usuario,
            tb1.criacao_data,
            CURRENT_TIMESTAMP AS atualizacao_data
           FROM dados_transmissoes_recentes_dia tb1
          WHERE tb1.se_faleceu = 0
        ), aux_dia AS (
         SELECT concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
            lnd.municipio_id_sus,
            lnd.cidadao_nome::text || lnd.dt_nascimento AS id_paciente,
            lnd.status_em_dia
           FROM lista_diabetes lnd
             JOIN listas_de_codigos.municipios m ON lnd.municipio_id_sus::bpchar = m.id_sus
        ), base_dia AS (
         SELECT b.municipio_id_sus,
            b.municipio_uf,
            count(
                CASE
                    WHEN b.status_em_dia = 'Em dia'::text THEN b.id_paciente
                    ELSE NULL::text
                END) AS ind7_numerador,
            s_ind7.indicador_numerador AS ind7_numerador_sisab,
            count(DISTINCT b.id_paciente) AS ind7_denominador_identificado_atual,
            s_ind7.indicador_denominador_informado AS ind7_denominador_informado_sisab,
            s_ind7.indicador_denominador_estimado AS ind7_denominador_estimado_sisab,
                CASE
                    WHEN count(DISTINCT b.id_paciente)::numeric >= (0.85 * s_ind7.indicador_denominador_estimado::numeric) THEN count(DISTINCT b.id_paciente)
                    WHEN count(DISTINCT b.id_paciente)::numeric < (0.85 * s_ind7.indicador_denominador_estimado::numeric) THEN s_ind7.indicador_denominador_estimado::bigint
                    ELSE NULL::bigint
                END AS ind7_denominador_utilizado,
            s_ind7.indicador_nota_porcentagem AS ind7_resultado_sisab,
            s_ind7.indicador_meta AS ind7_meta_sisab
           FROM aux_dia b
             LEFT JOIN ind_sisab s_ind7 ON s_ind7.municipio_uf = b.municipio_uf AND s_ind7.indicador_ordem = 7
          WHERE b.municipio_id_sus::text <> ALL (ARRAY['100111'::text, '111111'::text])
          GROUP BY b.municipio_id_sus, b.municipio_uf, s_ind7.indicador_numerador, s_ind7.indicador_denominador_informado, s_ind7.indicador_denominador_estimado, s_ind7.indicador_nota_porcentagem, s_ind7.indicador_meta
        ), base_diabetes AS (
         SELECT b.municipio_id_sus,
            b.municipio_uf,
            b.ind7_numerador,
            b.ind7_numerador_sisab,
            b.ind7_numerador_sisab - b.ind7_numerador AS ind7_dif_numerador,
            round(b.ind7_numerador::numeric * 1.00 / b.ind7_numerador_sisab::numeric - 1::numeric, 2) AS ind7_dif_numerador_perc,
            b.ind7_denominador_identificado_atual,
            b.ind7_denominador_utilizado,
            b.ind7_denominador_informado_sisab,
            b.ind7_denominador_identificado_atual - b.ind7_denominador_informado_sisab AS ind7_dif_denominador,
            b.ind7_denominador_estimado_sisab,
            b.ind7_resultado_sisab,
            round(b.ind7_numerador::numeric * 1.00 / b.ind7_denominador_utilizado::numeric * 100::numeric, 2) AS ind7_hipertensao,
            round(b.ind7_numerador::numeric * 1.00 / b.ind7_denominador_utilizado::numeric * 100::numeric - b.ind7_resultado_sisab::numeric, 2) AS ind7_dif_perc,
            b.ind7_meta_sisab
           FROM base_dia b
          ORDER BY b.municipio_uf
        )
 SELECT bg.municipio_id_sus,
    bg.municipio_uf,
    bg.ind1_numerador_sisab,
    bg.gestantes_6consultas_1consulta_em_12semanas,
    bg.ind1_dif_numerador,
    bg.ind1_dif_numerador_perc,
    bg.ind2_numerador_sisab,
    bg.gestantes_com_sifilis_hiv_realizado,
    bg.ind2_dif_numerador,
    bg.ind2_dif_numerador_perc,
    bg.ind3_numerador_sisab,
    bg.gestantes_odonto_realizado,
    bg.ind3_dif_numerador,
    bg.ind3_dif_numerador_perc,
    bg.denominador_estimado_sisab,
    bg.denominador_informado_sisab,
    bg.gestantes_denominador,
    bg.dif_denominador,
    bg.dif_denominador_perc,
    bg.gestantes_denominador_utilizado,
    bg.tipo_denominador_utilizado,
    bg.ind1_resultado_sisab,
    bg.ind1_6consultas_prenatal,
    bg.ind1_dif_perc,
    bg.ind2_resultado_sisab,
    bg.ind2_exame_sifilis_hiv,
    bg.ind2_dif_perc,
    bg.ind3_resultado_sisab,
    bg.ind3_atend_odonto,
    bg.ind3_dif_perc,
    bh.ind6_numerador,
    bh.ind6_numerador_sisab,
    bh.ind6_dif_numerador,
    bh.ind6_dif_numerador_perc,
    bh.ind6_denominador_identificado_atual,
    bh.ind6_denominador_utilizado,
    bh.ind6_denominador_informado_sisab,
    bh.ind6_dif_denominador,
    bh.ind6_denominador_estimado_sisab,
    bh.ind6_resultado_sisab,
    bh.ind6_hipertensao,
    bh.ind6_dif_perc,
    bh.ind6_meta_sisab,
    bd.ind7_numerador,
    bd.ind7_numerador_sisab,
    bd.ind7_dif_numerador,
    bd.ind7_dif_numerador_perc,
    bd.ind7_denominador_identificado_atual,
    bd.ind7_denominador_utilizado,
    bd.ind7_denominador_informado_sisab,
    bd.ind7_dif_denominador,
    bd.ind7_denominador_estimado_sisab,
    bd.ind7_resultado_sisab,
    bd.ind7_hipertensao,
    bd.ind7_dif_perc,
    bd.ind7_meta_sisab
   FROM base_gestantes bg
     JOIN base_hipertensao bh ON bg.municipio_id_sus::text = bh.municipio_id_sus::text
     JOIN base_diabetes bd ON bg.municipio_id_sus::text = bd.municipio_id_sus::text
WITH DATA;