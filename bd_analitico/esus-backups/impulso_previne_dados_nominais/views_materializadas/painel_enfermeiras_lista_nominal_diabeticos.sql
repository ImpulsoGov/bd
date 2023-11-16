
CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.painel_enfermeiras_lista_nominal_diabeticos
TABLESPACE pg_default
AS WITH dados_anonimizados_demo_vicosa AS (
         SELECT '100111'::character varying AS municipio_id_sus,
            res.quadrimestre_atual,
            res.realizou_solicitacao_hemoglobina_ultimos_6_meses,
            res.dt_solicitacao_hemoglobina_glicada_mais_recente,
            res.realizou_consulta_ultimos_6_meses,
            res.dt_consulta_mais_recente,
            res.co_seq_fat_cidadao_pec,
            res.cidadao_cpf,
            res.cidadao_cns,
            upper(nomes.nome_ficticio) AS cidadao_nome,
            res.cidadao_nome_social,
            res.cidadao_sexo,
            res.dt_nascimento,
            res.estabelecimento_cnes_atendimento,
            res.estabelecimento_cnes_cadastro,
            res.estabelecimento_nome_atendimento,
            res.estabelecimento_nome_cadastro,
            res.equipe_ine_atendimento,
            res.equipe_ine_cadastro,
            res.equipe_nome_atendimento,
            res.equipe_nome_cadastro,
            upper(nomes2.nome_ficticio) AS acs_nome_cadastro,
            upper(nomes2.nome_ficticio) AS acs_nome_visita,
            res.possui_diabetes_autoreferida,
            res.possui_diabetes_diagnosticada,
            res.data_ultimo_cadastro,
            res.dt_ultima_consulta,
            res.se_faleceu,
            res.se_mudou,
            res.criacao_data
           FROM ( SELECT tb1_1.municipio_id_sus,
                    tb1_1.quadrimestre_atual,
                    tb1_1.realizou_solicitacao_hemoglobina_ultimos_6_meses,
                    tb1_1.dt_solicitacao_hemoglobina_glicada_mais_recente,
                    tb1_1.realizou_consulta_ultimos_6_meses,
                    tb1_1.dt_consulta_mais_recente,
                    tb1_1.co_seq_fat_cidadao_pec,
                    concat(impulso_previne_dados_nominais.random_between(100000000, 999999999)::text, impulso_previne_dados_nominais.random_between(10, 99)::text) AS cidadao_cpf,
                    concat('7', impulso_previne_dados_nominais.random_between(100000000, 999999999)::text, impulso_previne_dados_nominais.random_between(10000, 99999)::text) AS cidadao_cns,
                    tb1_1.cidadao_nome,
                    row_number() OVER (PARTITION BY 0::integer) AS seq,
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
                    tb1_1.criacao_data,
                    tb1_1.atualizacao_data
                   FROM impulso_previne_dados_nominais.lista_nominal_diabeticos_unificada tb1_1
                  WHERE tb1_1.municipio_id_sus::text = '317130'::text
                  and tb1_1.equipe_ine_atendimento is not null and tb1_1.equipe_ine_cadastro is not null) res
             JOIN configuracoes.nomes_ficticios_diabeticos nomes ON res.seq = nomes.seq
             JOIN configuracoes.nomes_ficticios_hipertensos nomes2 ON res.seq = nomes2.seq
        ), dados_anonimizados_impulsolandia AS (
         SELECT '111111'::character varying AS municipio_id_sus,
            res.quadrimestre_atual,
            res.realizou_solicitacao_hemoglobina_ultimos_6_meses,
            res.dt_solicitacao_hemoglobina_glicada_mais_recente,
            res.realizou_consulta_ultimos_6_meses,
            res.dt_consulta_mais_recente,
            res.co_seq_fat_cidadao_pec,
            res.cidadao_cpf,
            res.cidadao_cns,
            upper(nomes.nome_ficticio) AS cidadao_nome,
            res.cidadao_nome_social,
            res.cidadao_sexo,
            res.dt_nascimento,
            res.estabelecimento_cnes_atendimento,
            res.estabelecimento_cnes_cadastro,
            res.estabelecimento_nome_atendimento,
            res.estabelecimento_nome_cadastro,
            res.equipe_ine_atendimento,
            res.equipe_ine_cadastro,
            res.equipe_nome_atendimento,
            res.equipe_nome_cadastro,
            upper(nomes2.nome_ficticio) AS acs_nome_cadastro,
            upper(nomes2.nome_ficticio) AS acs_nome_visita,
            res.possui_diabetes_autoreferida,
            res.possui_diabetes_diagnosticada,
            res.data_ultimo_cadastro,
            res.dt_ultima_consulta,
            res.se_faleceu,
            res.se_mudou,
            res.criacao_data
           FROM ( SELECT tb1_1.municipio_id_sus,
                    tb1_1.quadrimestre_atual,
                    tb1_1.realizou_solicitacao_hemoglobina_ultimos_6_meses,
                    tb1_1.dt_solicitacao_hemoglobina_glicada_mais_recente,
                    tb1_1.realizou_consulta_ultimos_6_meses,
                    tb1_1.dt_consulta_mais_recente,
                    tb1_1.co_seq_fat_cidadao_pec,
                    concat(impulso_previne_dados_nominais.random_between(100000000, 999999999)::text, impulso_previne_dados_nominais.random_between(10, 99)::text) AS cidadao_cpf,
                    concat('7', impulso_previne_dados_nominais.random_between(100000000, 999999999)::text, impulso_previne_dados_nominais.random_between(10000, 99999)::text) AS cidadao_cns,
                    tb1_1.cidadao_nome,
                    row_number() OVER (PARTITION BY 0::integer) AS seq,
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
                    tb1_1.criacao_data,
                    tb1_1.atualizacao_data
                   FROM impulso_previne_dados_nominais.lista_nominal_diabeticos_unificada tb1_1
                  WHERE tb1_1.municipio_id_sus::text = '317130'::text
                  and tb1_1.equipe_ine_atendimento is not null and tb1_1.equipe_ine_cadastro is not null) res
             JOIN configuracoes.nomes_ficticios_diabeticos nomes ON res.seq = nomes.seq
             JOIN configuracoes.nomes_ficticios_hipertensos nomes2 ON res.seq = nomes2.seq
        ), dados_transmissoes_recentes AS (
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
            tb1_1.criacao_data
           FROM impulso_previne_dados_nominais.lista_nominal_diabeticos_unificada tb1_1
          WHERE tb1_1.municipio_id_sus::text <> ALL (ARRAY['210280'::character varying::text, '315210'::character varying::text, '111111'::text])
        ), une_as_bases AS (
         SELECT dados_anonimizados_demo_vicosa.municipio_id_sus,
            dados_anonimizados_demo_vicosa.quadrimestre_atual,
            dados_anonimizados_demo_vicosa.realizou_solicitacao_hemoglobina_ultimos_6_meses,
            dados_anonimizados_demo_vicosa.dt_solicitacao_hemoglobina_glicada_mais_recente,
            dados_anonimizados_demo_vicosa.realizou_consulta_ultimos_6_meses,
            dados_anonimizados_demo_vicosa.dt_consulta_mais_recente,
            dados_anonimizados_demo_vicosa.co_seq_fat_cidadao_pec,
            dados_anonimizados_demo_vicosa.cidadao_cpf,
            dados_anonimizados_demo_vicosa.cidadao_cns,
            dados_anonimizados_demo_vicosa.cidadao_nome,
            dados_anonimizados_demo_vicosa.cidadao_nome_social,
            dados_anonimizados_demo_vicosa.cidadao_sexo,
            dados_anonimizados_demo_vicosa.dt_nascimento,
            dados_anonimizados_demo_vicosa.estabelecimento_cnes_atendimento,
            dados_anonimizados_demo_vicosa.estabelecimento_cnes_cadastro,
            dados_anonimizados_demo_vicosa.estabelecimento_nome_atendimento,
            dados_anonimizados_demo_vicosa.estabelecimento_nome_cadastro,
            dados_anonimizados_demo_vicosa.equipe_ine_atendimento,
            dados_anonimizados_demo_vicosa.equipe_ine_cadastro,
            dados_anonimizados_demo_vicosa.equipe_nome_atendimento,
            dados_anonimizados_demo_vicosa.equipe_nome_cadastro,
            dados_anonimizados_demo_vicosa.acs_nome_cadastro,
            dados_anonimizados_demo_vicosa.acs_nome_visita,
            dados_anonimizados_demo_vicosa.possui_diabetes_autoreferida,
            dados_anonimizados_demo_vicosa.possui_diabetes_diagnosticada,
            dados_anonimizados_demo_vicosa.data_ultimo_cadastro,
            dados_anonimizados_demo_vicosa.dt_ultima_consulta,
            dados_anonimizados_demo_vicosa.se_faleceu,
            dados_anonimizados_demo_vicosa.se_mudou,
            dados_anonimizados_demo_vicosa.criacao_data
           FROM dados_anonimizados_demo_vicosa
        UNION ALL
         SELECT dados_anonimizados_impulsolandia.municipio_id_sus,
            dados_anonimizados_impulsolandia.quadrimestre_atual,
            dados_anonimizados_impulsolandia.realizou_solicitacao_hemoglobina_ultimos_6_meses,
            dados_anonimizados_impulsolandia.dt_solicitacao_hemoglobina_glicada_mais_recente,
            dados_anonimizados_impulsolandia.realizou_consulta_ultimos_6_meses,
            dados_anonimizados_impulsolandia.dt_consulta_mais_recente,
            dados_anonimizados_impulsolandia.co_seq_fat_cidadao_pec,
            dados_anonimizados_impulsolandia.cidadao_cpf,
            dados_anonimizados_impulsolandia.cidadao_cns,
            dados_anonimizados_impulsolandia.cidadao_nome,
            dados_anonimizados_impulsolandia.cidadao_nome_social,
            dados_anonimizados_impulsolandia.cidadao_sexo,
            dados_anonimizados_impulsolandia.dt_nascimento,
            dados_anonimizados_impulsolandia.estabelecimento_cnes_atendimento,
            dados_anonimizados_impulsolandia.estabelecimento_cnes_cadastro,
            dados_anonimizados_impulsolandia.estabelecimento_nome_atendimento,
            dados_anonimizados_impulsolandia.estabelecimento_nome_cadastro,
            dados_anonimizados_impulsolandia.equipe_ine_atendimento,
            dados_anonimizados_impulsolandia.equipe_ine_cadastro,
            dados_anonimizados_impulsolandia.equipe_nome_atendimento,
            dados_anonimizados_impulsolandia.equipe_nome_cadastro,
            dados_anonimizados_impulsolandia.acs_nome_cadastro,
            dados_anonimizados_impulsolandia.acs_nome_visita,
            dados_anonimizados_impulsolandia.possui_diabetes_autoreferida,
            dados_anonimizados_impulsolandia.possui_diabetes_diagnosticada,
            dados_anonimizados_impulsolandia.data_ultimo_cadastro,
            dados_anonimizados_impulsolandia.dt_ultima_consulta,
            dados_anonimizados_impulsolandia.se_faleceu,
            dados_anonimizados_impulsolandia.se_mudou,
            dados_anonimizados_impulsolandia.criacao_data
           FROM dados_anonimizados_impulsolandia
        UNION ALL
         SELECT dados_transmissoes_recentes.municipio_id_sus,
            dados_transmissoes_recentes.quadrimestre_atual,
            dados_transmissoes_recentes.realizou_solicitacao_hemoglobina_ultimos_6_meses,
            dados_transmissoes_recentes.dt_solicitacao_hemoglobina_glicada_mais_recente,
            dados_transmissoes_recentes.realizou_consulta_ultimos_6_meses,
            dados_transmissoes_recentes.dt_consulta_mais_recente,
            dados_transmissoes_recentes.co_seq_fat_cidadao_pec,
            dados_transmissoes_recentes.cidadao_cpf,
            dados_transmissoes_recentes.cidadao_cns,
            dados_transmissoes_recentes.cidadao_nome,
            dados_transmissoes_recentes.cidadao_nome_social,
            dados_transmissoes_recentes.cidadao_sexo,
            dados_transmissoes_recentes.dt_nascimento,
            dados_transmissoes_recentes.estabelecimento_cnes_atendimento,
            dados_transmissoes_recentes.estabelecimento_cnes_cadastro,
            dados_transmissoes_recentes.estabelecimento_nome_atendimento,
            dados_transmissoes_recentes.estabelecimento_nome_cadastro,
            dados_transmissoes_recentes.equipe_ine_atendimento,
            dados_transmissoes_recentes.equipe_ine_cadastro,
            dados_transmissoes_recentes.equipe_nome_atendimento,
            dados_transmissoes_recentes.equipe_nome_cadastro,
            dados_transmissoes_recentes.acs_nome_cadastro,
            dados_transmissoes_recentes.acs_nome_visita,
            dados_transmissoes_recentes.possui_diabetes_autoreferida,
            dados_transmissoes_recentes.possui_diabetes_diagnosticada,
            dados_transmissoes_recentes.data_ultimo_cadastro,
            dados_transmissoes_recentes.dt_ultima_consulta,
            dados_transmissoes_recentes.se_faleceu,
            dados_transmissoes_recentes.se_mudou,
            dados_transmissoes_recentes.criacao_data
           FROM dados_transmissoes_recentes
        ), data_registro_producao AS (
         SELECT une_as_bases.municipio_id_sus,
            impulso_previne_dados_nominais.equipe_ine(une_as_bases.municipio_id_sus::text, COALESCE(une_as_bases.equipe_ine_cadastro, une_as_bases.equipe_ine_atendimento)) AS equipe_ine_cadastro,
            max(GREATEST(une_as_bases.dt_solicitacao_hemoglobina_glicada_mais_recente::date, une_as_bases.dt_consulta_mais_recente, une_as_bases.data_ultimo_cadastro, une_as_bases.dt_ultima_consulta)) AS dt_registro_producao_mais_recente,
            min(LEAST(une_as_bases.dt_solicitacao_hemoglobina_glicada_mais_recente::date, une_as_bases.dt_consulta_mais_recente, une_as_bases.data_ultimo_cadastro, une_as_bases.dt_ultima_consulta)) AS dt_registro_producao_mais_antigo
           FROM une_as_bases
          GROUP BY une_as_bases.municipio_id_sus, (impulso_previne_dados_nominais.equipe_ine(une_as_bases.municipio_id_sus::text, COALESCE(une_as_bases.equipe_ine_cadastro, une_as_bases.equipe_ine_atendimento)))
        ), tabela_aux AS (
         SELECT tb1.municipio_id_sus,
            concat(tb2.nome, ' - ', tb2.uf_sigla) AS municipio_uf,
            tb1.quadrimestre_atual,
            tb1.realizou_solicitacao_hemoglobina_ultimos_6_meses,
            tb1.dt_solicitacao_hemoglobina_glicada_mais_recente,
            tb1.realizou_consulta_ultimos_6_meses,
            tb1.dt_consulta_mais_recente,
                CASE
                    WHEN tb1.realizou_solicitacao_hemoglobina_ultimos_6_meses THEN 'Em dia'::text
                    ELSE impulso_previne_dados_nominais.prazo_proximo_dia()
                END AS prazo_proxima_solicitacao_hemoglobina,
                CASE
                    WHEN tb1.realizou_consulta_ultimos_6_meses THEN 'Em dia'::text
                    ELSE impulso_previne_dados_nominais.prazo_proximo_dia()
                END AS prazo_proxima_consulta,
                CASE
                    WHEN tb1.realizou_solicitacao_hemoglobina_ultimos_6_meses AND tb1.realizou_consulta_ultimos_6_meses THEN 1
                    ELSE 0
                END AS consulta_e_solicitacao_hemoglobina_em_dia,
                CASE
                    WHEN tb1.realizou_consulta_ultimos_6_meses IS FALSE OR tb1.realizou_solicitacao_hemoglobina_ultimos_6_meses IS FALSE THEN 'Não está em dia'::text
                    WHEN tb1.realizou_consulta_ultimos_6_meses AND tb1.realizou_solicitacao_hemoglobina_ultimos_6_meses THEN 'Em dia'::text
                    ELSE NULL::text
                END AS status_em_dia,
                CASE
                    WHEN tb1.realizou_solicitacao_hemoglobina_ultimos_6_meses AND tb1.realizou_consulta_ultimos_6_meses THEN 'Em dia com consulta e solicitação de hemoglobina'::text
                    WHEN tb1.realizou_solicitacao_hemoglobina_ultimos_6_meses IS FALSE AND tb1.realizou_consulta_ultimos_6_meses IS FALSE THEN 'Nada em dia'::text
                    WHEN tb1.realizou_solicitacao_hemoglobina_ultimos_6_meses AND tb1.realizou_consulta_ultimos_6_meses IS FALSE THEN 'Apenas solicitação de hemoglobina em dia'::text
                    WHEN tb1.realizou_solicitacao_hemoglobina_ultimos_6_meses IS FALSE AND tb1.realizou_consulta_ultimos_6_meses THEN 'Apenas consulta em dia'::text
                    ELSE NULL::text
                END AS status_usuario,
                CASE
                    WHEN tb1.possui_diabetes_diagnosticada THEN 'Diagnóstico Clínico'::text
                    WHEN tb1.possui_diabetes_autoreferida AND tb1.possui_diabetes_diagnosticada IS FALSE THEN 'Autorreferida'::text
                    WHEN tb1.possui_diabetes_autoreferida AND tb1.possui_diabetes_diagnosticada IS NULL THEN 'Autorreferida'::text
                    ELSE NULL::text
                END AS identificacao_condicao_diabetes,
            tb1.cidadao_cpf,
                CASE
                    WHEN tb1.cidadao_cpf IS NULL THEN tb1.dt_nascimento::text::character varying::text
                    ELSE tb1.cidadao_cpf
                END AS cidadao_cpf_dt_nascimento,
            tb1.cidadao_cns,
            tb1.cidadao_nome,
            tb1.cidadao_nome_social,
            tb1.cidadao_sexo,
            tb1.dt_nascimento,
            date_part('year'::text, age(CURRENT_DATE::timestamp with time zone, tb1.dt_nascimento::timestamp with time zone))::integer AS cidadao_idade,
                CASE
                    WHEN date_part('year'::text, age(CURRENT_DATE::timestamp with time zone, tb1.dt_nascimento::timestamp with time zone)) <= 40::double precision THEN '0 a 40 anos'::text
                    WHEN date_part('year'::text, age(CURRENT_DATE::timestamp with time zone, tb1.dt_nascimento::timestamp with time zone)) > 40::double precision AND date_part('year'::text, age(CURRENT_DATE::timestamp with time zone, tb1.dt_nascimento::timestamp with time zone)) <= 49::double precision THEN '41 a 49 anos'::text
                    WHEN date_part('year'::text, age(CURRENT_DATE::timestamp with time zone, tb1.dt_nascimento::timestamp with time zone)) > 49::double precision AND date_part('year'::text, age(CURRENT_DATE::timestamp with time zone, tb1.dt_nascimento::timestamp with time zone)) <= 59::double precision THEN '50 a 59 anos'::text
                    WHEN date_part('year'::text, age(CURRENT_DATE::timestamp with time zone, tb1.dt_nascimento::timestamp with time zone)) > 59::double precision AND date_part('year'::text, age(CURRENT_DATE::timestamp with time zone, tb1.dt_nascimento::timestamp with time zone)) <= 70::double precision THEN '60 a 70 anos'::text
                    WHEN tb1.dt_nascimento IS NULL THEN NULL::text
                    ELSE '70 anos ou mais'::text
                END AS cidadao_faixa_etaria,
            tb1.estabelecimento_cnes_atendimento,
            tb1.estabelecimento_cnes_cadastro,
            tb1.estabelecimento_nome_atendimento,
            tb1.estabelecimento_nome_cadastro,
            tb1.equipe_ine_atendimento,
            impulso_previne_dados_nominais.equipe_ine(tb1.municipio_id_sus::text, COALESCE(tb1.equipe_ine_cadastro, tb1.equipe_ine_atendimento)) AS equipe_ine_cadastro,
            tb1.equipe_nome_atendimento,
            impulso_previne_dados_nominais.equipe_ine(tb1.municipio_id_sus::text, COALESCE(tb1.equipe_nome_cadastro, tb1.equipe_nome_atendimento)) AS equipe_nome_cadastro,
            tb1.acs_nome_cadastro,
            tb1.acs_nome_visita,
            tb1.possui_diabetes_autoreferida AS possui_diabetes_autorreferida,
            tb1.possui_diabetes_diagnosticada,
                CASE
                    WHEN tb1.possui_diabetes_autoreferida AND tb1.possui_diabetes_diagnosticada IS FALSE THEN 1
                    WHEN tb1.possui_diabetes_autoreferida AND tb1.possui_diabetes_diagnosticada IS NULL THEN 1
                    ELSE 0
                END AS apenas_autorreferida,
                CASE
                    WHEN tb1.possui_diabetes_diagnosticada THEN 1
                    ELSE 0
                END AS diagnostico_clinico,
            tb1.data_ultimo_cadastro,
            tb1.dt_ultima_consulta,
            tb1.se_faleceu,
            tb1.se_mudou,
            tb1.criacao_data,
            CURRENT_TIMESTAMP AS atualizacao_data
           FROM une_as_bases tb1
             LEFT JOIN listas_de_codigos.municipios tb2 ON tb1.municipio_id_sus::bpchar = tb2.id_sus
          WHERE COALESCE(tb1.se_faleceu, 0) <> 1
        )
 SELECT tabela_aux.municipio_id_sus,
    tabela_aux.municipio_uf,
    tabela_aux.quadrimestre_atual,
    tabela_aux.realizou_solicitacao_hemoglobina_ultimos_6_meses,
    tabela_aux.dt_solicitacao_hemoglobina_glicada_mais_recente,
    tabela_aux.realizou_consulta_ultimos_6_meses,
    tabela_aux.dt_consulta_mais_recente,
    tabela_aux.prazo_proxima_solicitacao_hemoglobina,
    tabela_aux.prazo_proxima_consulta,
    tabela_aux.consulta_e_solicitacao_hemoglobina_em_dia,
    tabela_aux.status_em_dia,
    tabela_aux.status_usuario,
    tabela_aux.identificacao_condicao_diabetes,
    tabela_aux.cidadao_cpf,
    tabela_aux.cidadao_cpf_dt_nascimento,
    tabela_aux.cidadao_cns,
    tabela_aux.cidadao_nome,
    tabela_aux.cidadao_nome_social,
    tabela_aux.cidadao_sexo,
    tabela_aux.dt_nascimento,
    tabela_aux.cidadao_idade,
    tabela_aux.cidadao_faixa_etaria,
    tabela_aux.estabelecimento_cnes_atendimento,
    tabela_aux.estabelecimento_cnes_cadastro,
    tabela_aux.estabelecimento_nome_atendimento,
    tabela_aux.estabelecimento_nome_cadastro,
    tabela_aux.equipe_ine_atendimento,
    tabela_aux.equipe_ine_cadastro,
    tabela_aux.equipe_nome_atendimento,
    tabela_aux.equipe_nome_cadastro,
    tabela_aux.acs_nome_cadastro,
    tabela_aux.acs_nome_visita,
    tabela_aux.possui_diabetes_autorreferida,
    tabela_aux.possui_diabetes_diagnosticada,
    tabela_aux.apenas_autorreferida,
    tabela_aux.diagnostico_clinico,
    tabela_aux.data_ultimo_cadastro,
    tabela_aux.dt_ultima_consulta,
    tabela_aux.se_faleceu,
    tabela_aux.se_mudou,
    tabela_aux.criacao_data,
    tabela_aux.atualizacao_data,
    drp.dt_registro_producao_mais_recente
   FROM tabela_aux
     LEFT JOIN data_registro_producao drp ON drp.municipio_id_sus::text = tabela_aux.municipio_id_sus::text AND drp.equipe_ine_cadastro = tabela_aux.equipe_ine_cadastro
WITH DATA;