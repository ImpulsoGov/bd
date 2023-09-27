
CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.api_futuro_painel_hipertensos_lista_nominal
TABLESPACE pg_default
AS WITH dados_anonimizados_demo_vicosa AS (
         SELECT '100111'::character varying AS municipio_id_sus,
            res.quadrimestre_atual,
            res.realizou_afericao_ultimos_6_meses,
            res.dt_afericao_pressao_mais_recente,
            res.realizou_consulta_ultimos_6_meses,
            res.dt_consulta_mais_recente,
            res.co_seq_fat_cidadao_pec::text AS co_seq_fat_cidadao_pec,
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
            res.possui_hipertensao_autorreferida,
            res.possui_hipertensao_diagnosticada,
            res.data_ultimo_cadastro,
            res.dt_ultima_consulta,
            res.se_faleceu,
            res.se_mudou,
            res.criacao_data
           FROM ( SELECT tb1_1.municipio_id_sus,
                    tb1_1.quadrimestre_atual,
                    tb1_1.realizou_afericao_ultimos_6_meses,
                    tb1_1.dt_afericao_pressao_mais_recente,
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
                    tb1_1.possui_hipertensao_autorreferida,
                    tb1_1.possui_hipertensao_diagnosticada,
                    tb1_1.data_ultimo_cadastro,
                    tb1_1.dt_ultima_consulta,
                    tb1_1.se_faleceu,
                    tb1_1.se_mudou,
                    tb1_1.criacao_data,
                    tb1_1.atualizacao_data
                   FROM dados_nominais_mg_vicosa.lista_nominal_hipertensos tb1_1) res
             JOIN configuracoes.nomes_ficticios_hipertensos nomes ON res.seq = nomes.seq
             JOIN configuracoes.nomes_ficticios_diabeticos nomes2 ON res.seq = nomes2.seq
        ), dados_anonimizados_impulsolandia AS (
         SELECT '111111'::character varying AS municipio_id_sus,
            res.quadrimestre_atual,
            res.realizou_afericao_ultimos_6_meses,
            res.dt_afericao_pressao_mais_recente,
            res.realizou_consulta_ultimos_6_meses,
            res.dt_consulta_mais_recente,
            res.co_seq_fat_cidadao_pec::text AS co_seq_fat_cidadao_pec,
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
            res.possui_hipertensao_autorreferida,
            res.possui_hipertensao_diagnosticada,
            res.data_ultimo_cadastro,
            res.dt_ultima_consulta,
            res.se_faleceu,
            res.se_mudou,
            res.criacao_data
           FROM ( SELECT tb1_1.municipio_id_sus,
                    tb1_1.quadrimestre_atual,
                    tb1_1.realizou_afericao_ultimos_6_meses,
                    tb1_1.dt_afericao_pressao_mais_recente,
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
                    tb1_1.possui_hipertensao_autorreferida,
                    tb1_1.possui_hipertensao_diagnosticada,
                    tb1_1.data_ultimo_cadastro,
                    tb1_1.dt_ultima_consulta,
                    tb1_1.se_faleceu,
                    tb1_1.se_mudou,
                    tb1_1.criacao_data,
                    tb1_1.atualizacao_data
                   FROM dados_nominais_mg_vicosa.lista_nominal_hipertensos tb1_1) res
             JOIN configuracoes.nomes_ficticios_hipertensos nomes ON res.seq = nomes.seq
             JOIN configuracoes.nomes_ficticios_diabeticos nomes2 ON res.seq = nomes2.seq
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
            tb1_1.criacao_data
           FROM impulso_previne_dados_nominais.lista_nominal_hipertensos_unificada tb1_1
        ), une_as_bases AS (
         SELECT dados_anonimizados_demo_vicosa.municipio_id_sus,
            dados_anonimizados_demo_vicosa.quadrimestre_atual,
            dados_anonimizados_demo_vicosa.realizou_afericao_ultimos_6_meses,
            dados_anonimizados_demo_vicosa.dt_afericao_pressao_mais_recente,
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
            dados_anonimizados_demo_vicosa.possui_hipertensao_autorreferida,
            dados_anonimizados_demo_vicosa.possui_hipertensao_diagnosticada,
            dados_anonimizados_demo_vicosa.data_ultimo_cadastro,
            dados_anonimizados_demo_vicosa.dt_ultima_consulta,
            dados_anonimizados_demo_vicosa.se_faleceu,
            dados_anonimizados_demo_vicosa.se_mudou,
            dados_anonimizados_demo_vicosa.criacao_data
           FROM dados_anonimizados_demo_vicosa
        UNION ALL
         SELECT dados_anonimizados_impulsolandia.municipio_id_sus,
            dados_anonimizados_impulsolandia.quadrimestre_atual,
            dados_anonimizados_impulsolandia.realizou_afericao_ultimos_6_meses,
            dados_anonimizados_impulsolandia.dt_afericao_pressao_mais_recente,
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
            dados_anonimizados_impulsolandia.possui_hipertensao_autorreferida,
            dados_anonimizados_impulsolandia.possui_hipertensao_diagnosticada,
            dados_anonimizados_impulsolandia.data_ultimo_cadastro,
            dados_anonimizados_impulsolandia.dt_ultima_consulta,
            dados_anonimizados_impulsolandia.se_faleceu,
            dados_anonimizados_impulsolandia.se_mudou,
            dados_anonimizados_impulsolandia.criacao_data
           FROM dados_anonimizados_impulsolandia
        UNION ALL
         SELECT dados_transmissoes_recentes.municipio_id_sus,
            dados_transmissoes_recentes.quadrimestre_atual,
            dados_transmissoes_recentes.realizou_afericao_ultimos_6_meses,
            dados_transmissoes_recentes.dt_afericao_pressao_mais_recente,
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
            dados_transmissoes_recentes.possui_hipertensao_autorreferida,
            dados_transmissoes_recentes.possui_hipertensao_diagnosticada,
            dados_transmissoes_recentes.data_ultimo_cadastro,
            dados_transmissoes_recentes.dt_ultima_consulta,
            dados_transmissoes_recentes.se_faleceu,
            dados_transmissoes_recentes.se_mudou,
            dados_transmissoes_recentes.criacao_data
           FROM dados_transmissoes_recentes
        )
 SELECT tb1.municipio_id_sus,
    tb1.cidadao_nome,
    tb1.dt_nascimento,
        CASE
            WHEN tb1.realizou_consulta_ultimos_6_meses IS FALSE OR tb1.realizou_afericao_ultimos_6_meses IS FALSE THEN 'Não está em dia'::text
            WHEN tb1.realizou_consulta_ultimos_6_meses AND tb1.realizou_afericao_ultimos_6_meses THEN 'Em dia'::text
            ELSE NULL::text
        END AS status_em_dia,
        CASE
            WHEN tb1.cidadao_cpf IS NULL THEN tb1.dt_nascimento::text::character varying::text
            ELSE tb1.cidadao_cpf
        END AS cidadao_cpf_dt_nascimento,
    tb1.dt_consulta_mais_recente,
        CASE
            WHEN tb1.realizou_consulta_ultimos_6_meses THEN 'Em dia'::text
            ELSE esus_160050_oiapoque_ap_20230405.prazo_proximo_dia()
        END AS prazo_proxima_consulta,
    tb1.dt_afericao_pressao_mais_recente::date AS dt_afericao_pressao_mais_recente,
        CASE
            WHEN tb1.realizou_afericao_ultimos_6_meses THEN 'Em dia'::text
            ELSE esus_160050_oiapoque_ap_20230405.prazo_proximo_dia()
        END AS prazo_proxima_afericao_pa,
    COALESCE(tb1.acs_nome_visita, tb1.acs_nome_cadastro) AS acs_nome,
    COALESCE(tb1.estabelecimento_cnes_cadastro, tb1.estabelecimento_cnes_atendimento) AS estabelecimento_cnes,
    COALESCE(tb1.estabelecimento_nome_cadastro, tb1.estabelecimento_nome_atendimento) AS estabelecimento_nome,
    esus_160050_oiapoque_ap_20230405.equipe_ine(tb1.municipio_id_sus::text, COALESCE(tb1.equipe_ine_cadastro, tb1.equipe_ine_atendimento)) AS equipe_ine,
    esus_160050_oiapoque_ap_20230405.equipe_ine(tb1.municipio_id_sus::text, COALESCE(tb1.equipe_nome_cadastro, tb1.equipe_nome_atendimento)) AS equipe_nome,
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
   FROM une_as_bases tb1
  WHERE COALESCE(tb1.se_faleceu, 0) <> 1
WITH DATA;