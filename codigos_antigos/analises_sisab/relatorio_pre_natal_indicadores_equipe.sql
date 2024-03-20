-- impulso_previne_dados_nominais.relatorio_pre_natal_indicadores_equipe source

CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.relatorio_pre_natal_indicadores_equipe
TABLESPACE pg_default
AS WITH gestantes_totais AS (
         SELECT concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
            l.gestacao_quadrimestre,
            l.equipe_nome,
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
          GROUP BY (concat(m.nome, ' - ', m.uf_sigla)), l.gestacao_quadrimestre, l.equipe_nome
        ), gestantes_validas AS (
         SELECT concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
            l.gestacao_quadrimestre,
            l.equipe_nome,
            count(DISTINCT l.chave_gestacao) AS gestantes_denominador,
            count(DISTINCT
                CASE
                    WHEN l.possui_registro_parto = 'Sim'::text OR l.gestacao_data_dpp < CURRENT_DATE THEN l.chave_gestacao::character varying
                    ELSE NULL::character varying
                END) AS gestantes_encerradas,
            count(DISTINCT
                CASE
                    WHEN l.possui_registro_parto = 'Não'::text AND l.gestacao_data_dpp >= CURRENT_DATE THEN l.chave_gestacao::character varying
                    ELSE NULL::character varying
                END) AS gestantes_ativas,
            count(DISTINCT
                CASE
                    WHEN l.gestacao_idade_gestacional_primeiro_atendimento >= 0 AND l.gestacao_idade_gestacional_primeiro_atendimento <= 12 THEN l.chave_gestacao::character varying
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
                    WHEN l.consultas_prenatal_total <= 5 AND l.possui_registro_parto = 'Não'::text AND l.gestacao_data_dpp >= CURRENT_DATE THEN l.chave_gestacao::character varying
                    ELSE NULL::character varying
                END) AS gestantes_ativas_abaixo6consultas,
            count(DISTINCT
                CASE
                    WHEN l.gestacao_idade_gestacional_primeiro_atendimento >= 0 AND l.gestacao_idade_gestacional_primeiro_atendimento <= 12 AND l.consultas_prenatal_total > 5 THEN l.chave_gestacao::character varying
                    ELSE NULL::character varying
                END) AS gestantes_6consultas_1consulta_em_12semanas,
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
          GROUP BY (concat(m.nome, ' - ', m.uf_sigla)), l.gestacao_quadrimestre, l.equipe_nome
        )
 SELECT gv.municipio_uf,
    gv.gestacao_quadrimestre,
    gv.equipe_nome,
    gt.gestantes_identificadas_unicas::numeric AS gestantes_identificadas_unicas,
    gt.gestantes_identificadas::numeric AS gestantes_identificadas,
    gt.gestantes_identificadas_com_aborto,
    gt.gestantes_identificadas_com_parto,
    gv.gestantes_denominador::numeric AS gestantes_denominador,
    gv.gestantes_ativas::numeric AS gestantes_ativas,
    gv.gestantes_encerradas::numeric AS gestantes_encerradas,
    gv.gestantes_1consulta_em_12semanas,
    gv.gestantes_1consulta_apos_12semanas,
    gv.gestantes_6consultas::numeric AS gestantes_6consultas,
    gv.gestantes_6consultas_1consulta_em_12semanas::numeric AS gestantes_6consultas_1consulta_em_12semanas,
    gv.gestantes_6consultas_1consulta_em_12semanas::numeric * 1.00 / gv.gestantes_denominador::numeric AS ind1_6consultas_prenatal,
    gv.gestantes_com_sifilis_hiv_realizado::numeric AS gestantes_com_sifilis_hiv_realizado,
    gv.gestantes_com_sifilis_hiv_realizado::numeric * 1.00 / gv.gestantes_denominador::numeric AS ind2_exame_sifilis_hiv,
    gv.gestantes_odonto_realizado::numeric AS gestantes_odonto_realizado,
    gv.gestantes_odonto_realizado::numeric * 1.00 / gv.gestantes_denominador::numeric AS ind3_atend_odonto,
    gv.gestantes_ativas_abaixo6consultas::numeric AS gestantes_ativas_abaixo6consultas,
    gv.gestantes_ativas_abaixo6consultas_1consulta_em_12semanas::numeric AS gestantes_ativas_abaixo6consultas_1consulta_em_12semanas,
    gv.gestantes_ativas_sem_sifilis_hiv_realizado::numeric AS gestantes_ativas_sem_sifilis_hiv_realizado,
    gv.gestantes_ativas_sem_odonto_realizado::numeric AS gestantes_ativas_sem_odonto_realizado,
    gv.gestantes_1consulta_em_12semanas::numeric - gv.gestantes_6consultas_1consulta_em_12semanas::numeric AS gestantes_abaixo_6consultas_1consulta_em_12semanas
   FROM gestantes_validas gv
     LEFT JOIN gestantes_totais gt ON gv.municipio_uf = gt.municipio_uf AND gv.gestacao_quadrimestre = gt.gestacao_quadrimestre AND gv.equipe_nome = gt.equipe_nome AND gv.gestacao_quadrimestre = '2023.Q2'::text
WITH DATA;

-- impulso_previne_dados_nominais.painel_cadastros_gestantes_duplicadas source

CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.painel_cadastros_gestantes_duplicadas
TABLESPACE pg_default
AS WITH rel_duplicadas AS (
         SELECT lista_nominal_gestantes_duplicadas_por_erro_grafia.municipio_id_sus,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.gestante_nome,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.gestante_data_de_nascimento,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.gestante_documento_cpf,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.gestante_documento_cns,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.periodo_data_transmissao,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.gestante_dum,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.gestante_dpp,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.equipe_ine,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.equipe_nome,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.estabelecimento_cnes,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.estabelecimento_nome,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.acs_nome,
            'Erro de grafia'::text AS duplicacao_motivo
           FROM impulso_previne_dados_nominais.lista_nominal_gestantes_duplicadas_por_erro_grafia
        UNION ALL
         SELECT '100111'::character varying AS municipio_id_sus,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.gestante_nome,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.gestante_data_de_nascimento,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.gestante_documento_cpf,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.gestante_documento_cns,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.periodo_data_transmissao,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.gestante_dum,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.gestante_dpp,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.equipe_ine,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.equipe_nome,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.estabelecimento_cnes,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.estabelecimento_nome,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.acs_nome,
            'Erro de grafia'::text AS duplicacao_motivo
           FROM impulso_previne_dados_nominais.lista_nominal_gestantes_duplicadas_por_erro_grafia
          WHERE lista_nominal_gestantes_duplicadas_por_erro_grafia.municipio_id_sus::text <> '111111'::text
        UNION ALL
         SELECT lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.municipio_id_sus,
            lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.gestante_nome,
            lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.gestante_data_de_nascimento,
            lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.gestante_documento_cpf,
            lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.gestante_documento_cns,
            lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.periodo_data_transmissao,
            lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.gestante_dum,
            lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.gestante_dpp,
            lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.equipe_ine,
            lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.equipe_nome,
            lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.estabelecimento_cnes,
            lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.estabelecimento_nome,
            lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.acs_nome,
            'Erro na data de nascimento'::text AS duplicacao_motivo
           FROM impulso_previne_dados_nominais.lista_nominal_gestantes_duplicadas_por_erro_data_nascimento
        )
 SELECT tb1.municipio_id_sus,
    concat(tb2.nome, ' - ', tb2.uf_sigla) AS municipio_uf,
    tb1.gestante_nome,
    tb1.gestante_data_de_nascimento,
    tb1.gestante_documento_cpf,
    tb1.gestante_documento_cns,
    tb1.periodo_data_transmissao,
    tb1.gestante_dum,
    tb1.gestante_dpp,
    tb1.duplicacao_motivo,
    CURRENT_TIMESTAMP AS atualizacao_data,
    tb1.equipe_ine,
    tb1.equipe_nome,
    tb1.estabelecimento_cnes,
    tb1.estabelecimento_nome,
    tb1.acs_nome
   FROM rel_duplicadas tb1
     JOIN listas_de_codigos.municipios tb2 ON tb1.municipio_id_sus::bpchar = tb2.id_sus
WITH DATA;