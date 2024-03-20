-- impulso_previne_dados_nominais.relatorio_pre_natal_indicadores_2023q2 source

CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.relatorio_pre_natal_indicadores_2023q2
TABLESPACE pg_default
AS WITH gestantes_totais AS (
         SELECT concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
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
          WHERE l.gestacao_quadrimestre = '2023.Q2'::text
          GROUP BY (concat(m.nome, ' - ', m.uf_sigla)), l.gestacao_quadrimestre
        ), gestantes_validas AS (
         SELECT concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
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
          WHERE l.possui_registro_aborto = 'Não'::text AND l.gestacao_quadrimestre = '2023.Q2'::text
          GROUP BY (concat(m.nome, ' - ', m.uf_sigla)), l.gestacao_quadrimestre
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
         SELECT gv.municipio_uf,
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
            s_ind1.indicador_denominador_estimado AS denominador_estimado_sisab,
            s_ind1.indicador_nota_porcentagem AS ind1_resultado_sisab,
            s_ind1.indicador_meta AS ind1_meta_sisab,
            s_ind2.indicador_nota_porcentagem AS ind2_resultado_sisab,
            s_ind2.indicador_meta AS ind2_meta_sisab,
            s_ind3.indicador_nota_porcentagem AS ind3_resultado_sisab,
            s_ind3.indicador_meta AS ind3_meta_sisab
           FROM gestantes_validas gv
             LEFT JOIN ind_sisab s_ind1 ON s_ind1.municipio_uf = gv.municipio_uf AND s_ind1.indicador_ordem = 1
             LEFT JOIN ind_sisab s_ind2 ON s_ind2.municipio_uf = gv.municipio_uf AND s_ind2.indicador_ordem = 2
             LEFT JOIN ind_sisab s_ind3 ON s_ind3.municipio_uf = gv.municipio_uf AND s_ind3.indicador_ordem = 3
             LEFT JOIN gestantes_totais gt ON gt.municipio_uf = gv.municipio_uf AND gt.gestacao_quadrimestre = gv.gestacao_quadrimestre
        ), base_com_denominador_utilizado AS (
         SELECT b_1.municipio_uf,
            b_1.gestacao_quadrimestre,
            b_1.gestantes_identificadas_unicas,
            b_1.gestantes_identificadas,
            b_1.gestantes_identificadas_com_aborto,
            b_1.gestantes_identificadas_com_parto,
            b_1.gestantes_denominador,
            b_1.denominador_estimado_sisab,
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
         SELECT b_1.municipio_uf,
            b_1.gestacao_quadrimestre,
            b_1.gestantes_identificadas_unicas,
            b_1.gestantes_identificadas,
            b_1.gestantes_identificadas_com_aborto,
            b_1.gestantes_identificadas_com_parto,
            b_1.gestantes_denominador,
            b_1.denominador_estimado_sisab,
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
            b_1.gestantes_denominador_utilizado,
            b_1.tipo_denominador_utilizado,
                CASE
                    WHEN b_1.gestantes_denominador::numeric < (0.85 * b_1.denominador_estimado_sisab::numeric) THEN round(b_1.denominador_estimado_sisab::numeric * 0.85, 0) - b_1.gestantes_denominador::numeric
                    WHEN b_1.gestantes_denominador::numeric >= (0.85 * b_1.denominador_estimado_sisab::numeric) THEN 0::numeric
                    ELSE NULL::numeric
                END AS pacientes_a_identificar,
            b_1.gestantes_6consultas_1consulta_em_12semanas::numeric * 1.00 / b_1.gestantes_denominador_utilizado::numeric AS ind1_6consultas_prenatal,
            b_1.gestantes_com_sifilis_hiv_realizado::numeric * 1.00 / b_1.gestantes_denominador_utilizado::numeric AS ind2_exame_sifilis_hiv,
            b_1.gestantes_odonto_realizado::numeric * 1.00 / b_1.gestantes_denominador_utilizado::numeric AS ind3_atend_odonto
           FROM base_com_denominador_utilizado b_1
        )
 SELECT b.municipio_uf,
    b.gestacao_quadrimestre,
    b.gestantes_identificadas_unicas,
    b.gestantes_identificadas,
    b.gestantes_identificadas_com_aborto,
    b.gestantes_identificadas_com_parto,
    b.gestantes_encerradas,
    b.gestantes_1consulta_em_12semanas,
    b.gestantes_1consulta_apos_12semanas,
    b.gestantes_6consultas,
    b.gestantes_6consultas_1consulta_em_12semanas,
    b.gestantes_ativas_abaixo6consultas,
    b.gestantes_ativas_abaixo6consultas_1consulta_em_12semanas,
    b.gestantes_com_sifilis_hiv_realizado,
    b.gestantes_ativas_sem_sifilis_hiv_realizado,
    b.gestantes_odonto_realizado,
    b.gestantes_ativas_sem_odonto_realizado,
    b.gestantes_denominador_utilizado,
    b.tipo_denominador_utilizado,
    b.pacientes_a_identificar,
    b.gestantes_denominador,
    b.denominador_estimado_sisab,
    b.ind1_resultado_sisab,
    b.ind2_resultado_sisab,
    b.ind3_resultado_sisab,
    b.ind1_6consultas_prenatal,
    b.ind2_exame_sifilis_hiv,
    b.ind3_atend_odonto,
    round(abs(1::numeric - b.gestantes_denominador::numeric / b.denominador_estimado_sisab::numeric), 2) * 100::numeric AS indicador_diferenca_estimado
   FROM base_com_indicadores b
  ORDER BY b.municipio_uf, b.gestacao_quadrimestre DESC
WITH DATA;

-- View indexes:
CREATE INDEX relatorio_pre_natal_indicadores_2023q2_municipio_uf_idx ON impulso_previne_dados_nominais.relatorio_pre_natal_indicadores_2023q2 USING btree (municipio_uf);
