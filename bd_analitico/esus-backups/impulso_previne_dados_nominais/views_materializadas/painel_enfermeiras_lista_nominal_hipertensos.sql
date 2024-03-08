
CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.painel_enfermeiras_lista_nominal_hipertensos
TABLESPACE pg_default
AS WITH dados_transmissoes_recentes AS (
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
            CASE 
                WHEN TRIM(tb1_1.equipe_ine_atendimento) = '-' OR tb1_1.equipe_ine_atendimento = ' ' OR tb1_1.equipe_ine_atendimento IS NULL 
                    THEN NULL
                ELSE TRIM(tb1_1.equipe_ine_atendimento)
            END AS equipe_ine_atendimento,
            CASE 
                WHEN TRIM(tb1_1.equipe_ine_cadastro) = '-' OR tb1_1.equipe_ine_cadastro = ' ' OR tb1_1.equipe_ine_cadastro IS NULL 
                    THEN NULL
                ELSE TRIM(tb1_1.equipe_ine_cadastro)
            END AS equipe_ine_cadastro,
            CASE 
                WHEN TRIM(tb1_1.equipe_ine_procedimento) = '-' OR tb1_1.equipe_ine_procedimento = ' ' OR tb1_1.equipe_ine_procedimento IS NULL 
                    THEN NULL
                ELSE TRIM(tb1_1.equipe_ine_procedimento)
            END AS equipe_ine_procedimento,
            CASE 
                WHEN tb1_1.equipe_nome_atendimento = ' ' OR tb1_1.equipe_nome_atendimento IS NULL OR tb1_1.equipe_nome_atendimento LIKE '%SEM EQUIPE%' 
                    THEN NULL 
                ELSE TRIM(tb1_1.equipe_nome_atendimento)
            END AS equipe_nome_atendimento,
            CASE 
                WHEN tb1_1.equipe_nome_cadastro = ' ' OR tb1_1.equipe_nome_cadastro IS NULL OR tb1_1.equipe_nome_cadastro LIKE '%SEM EQUIPE%' 
                    THEN NULL 
                ELSE TRIM(tb1_1.equipe_nome_cadastro)
            END AS equipe_nome_cadastro,
            CASE 
                WHEN tb1_1.equipe_nome_procedimento = ' ' OR tb1_1.equipe_nome_procedimento IS NULL OR tb1_1.equipe_nome_procedimento LIKE '%SEM EQUIPE%' 
                    THEN NULL 
                ELSE TRIM(tb1_1.equipe_nome_procedimento)
            END AS equipe_nome_procedimento,
            CASE 
                WHEN UPPER(tb1_1.acs_nome_cadastro) LIKE '%PROFISSIONAL NÃO CADASTRADO%'
                    THEN 'ERRO CADASTRO PROFISSIONAL'
                WHEN tb1_1.acs_nome_cadastro = ' ' OR tb1_1.acs_nome_cadastro IS NULL OR UPPER(tb1_1.acs_nome_cadastro) LIKE '%NÃO INFORMADO%'
                    THEN NULL 
                ELSE TRIM(UPPER(tb1_1.acs_nome_cadastro))
            END AS acs_nome_cadastro,
            CASE 
                WHEN UPPER(tb1_1.acs_nome_visita) LIKE '%PROFISSIONAL NÃO CADASTRADO%'
                    THEN 'ERRO CADASTRO PROFISSIONAL'
                WHEN tb1_1.acs_nome_visita = ' ' OR tb1_1.acs_nome_visita IS NULL OR UPPER(tb1_1.acs_nome_visita) LIKE '%NÃO INFORMADO%'
                    THEN NULL 
                ELSE TRIM(UPPER(tb1_1.acs_nome_visita))
            END AS acs_nome_visita,
            CASE 
                WHEN UPPER(tb1_1.profissional_nome_atendimento) LIKE '%PROFISSIONAL NÃO CADASTRADO%'
                    THEN 'ERRO CADASTRO PROFISSIONAL'
                WHEN tb1_1.profissional_nome_atendimento = ' ' OR tb1_1.profissional_nome_atendimento IS NULL OR UPPER(tb1_1.profissional_nome_atendimento) LIKE '%NÃO INFORMADO%'
                    THEN NULL 
                ELSE TRIM(UPPER(tb1_1.profissional_nome_atendimento))
            END AS profissional_nome_atendimento,
            CASE 
                WHEN UPPER(tb1_1.profissional_nome_procedimento) LIKE '%PROFISSIONAL NÃO CADASTRADO%'
                    THEN 'ERRO CADASTRO PROFISSIONAL'
                WHEN tb1_1.profissional_nome_procedimento = ' ' OR tb1_1.profissional_nome_procedimento IS NULL OR UPPER(tb1_1.profissional_nome_procedimento) LIKE '%NÃO INFORMADO%'
                    THEN NULL 
                ELSE TRIM(UPPER(tb1_1.profissional_nome_procedimento))
            END AS profissional_nome_procedimento,
            tb1_1.possui_hipertensao_autorreferida,
            tb1_1.possui_hipertensao_diagnosticada,
            tb1_1.data_ultimo_cadastro,
            tb1_1.dt_ultima_consulta,
            tb1_1.se_faleceu,
            tb1_1.se_mudou,
            tb1_1.criacao_data
           FROM impulso_previne_dados_nominais.lista_nominal_hipertensos_unificada tb1_1
        ), data_registro_producao AS (
            SELECT dtr.municipio_id_sus,
                impulso_previne_dados_nominais.equipe_ine(dtr.municipio_id_sus::text, COALESCE(dtr.equipe_ine_cadastro, dtr.equipe_ine_atendimento, dtr.equipe_ine_procedimento, '0')) AS equipe_ine_cadastro,
                max(GREATEST(dtr.dt_afericao_pressao_mais_recente::date, dtr.dt_consulta_mais_recente, dtr.data_ultimo_cadastro, dtr.dt_ultima_consulta)) AS dt_registro_producao_mais_recente,
                min(LEAST(dtr.dt_afericao_pressao_mais_recente::date, dtr.dt_consulta_mais_recente, dtr.data_ultimo_cadastro, dtr.dt_ultima_consulta)) AS dt_registro_producao_mais_antigo
            FROM dados_transmissoes_recentes dtr
            GROUP BY 1, 2
        ), tabela_aux AS (
         SELECT tb1.municipio_id_sus,
            concat(tb2.nome, ' - ', tb2.uf_sigla) AS municipio_uf,
            tb1.quadrimestre_atual,
            tb1.realizou_afericao_ultimos_6_meses,
            tb1.dt_afericao_pressao_mais_recente,
            tb1.realizou_consulta_ultimos_6_meses,
            tb1.dt_consulta_mais_recente,
                CASE
                    WHEN tb1.realizou_afericao_ultimos_6_meses THEN 'Em dia'::text
                    ELSE impulso_previne_dados_nominais.prazo_proximo_dia()
                END AS prazo_proxima_afericao_pa,
                CASE
                    WHEN tb1.realizou_consulta_ultimos_6_meses THEN 'Em dia'::text
                    ELSE impulso_previne_dados_nominais.prazo_proximo_dia()
                END AS prazo_proxima_consulta,
                CASE
                    WHEN tb1.realizou_afericao_ultimos_6_meses AND tb1.realizou_consulta_ultimos_6_meses THEN 1
                    ELSE 0
                END AS consulta_e_afericao_em_dia,
                CASE
                    WHEN tb1.realizou_consulta_ultimos_6_meses IS FALSE OR tb1.realizou_afericao_ultimos_6_meses IS FALSE THEN 'Não está em dia'::text
                    WHEN tb1.realizou_consulta_ultimos_6_meses AND tb1.realizou_afericao_ultimos_6_meses THEN 'Em dia'::text
                    ELSE NULL::text
                END AS status_em_dia,
                CASE
                    WHEN tb1.realizou_afericao_ultimos_6_meses AND tb1.realizou_consulta_ultimos_6_meses THEN 'Em dia com consulta e aferição de PA'::text
                    WHEN tb1.realizou_afericao_ultimos_6_meses IS FALSE AND tb1.realizou_consulta_ultimos_6_meses IS FALSE THEN 'Nada em dia'::text
                    WHEN tb1.realizou_afericao_ultimos_6_meses AND tb1.realizou_consulta_ultimos_6_meses IS FALSE THEN 'Apenas aferição de PA em dia'::text
                    WHEN tb1.realizou_afericao_ultimos_6_meses IS FALSE AND tb1.realizou_consulta_ultimos_6_meses THEN 'Apenas consulta em dia'::text
                    ELSE NULL::text
                END AS status_usuario,
                CASE
                    WHEN tb1.possui_hipertensao_diagnosticada THEN 'Diagnóstico Clínico'::text
                    WHEN tb1.possui_hipertensao_autorreferida AND tb1.possui_hipertensao_diagnosticada IS FALSE THEN 'Autorreferida'::text
                    WHEN tb1.possui_hipertensao_autorreferida AND tb1.possui_hipertensao_diagnosticada IS NULL THEN 'Autorreferida'::text
                    ELSE NULL::text
                END AS identificacao_condicao_hipertensao,
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
            tb1.estabelecimento_cnes_cadastro AS estabelecimento_cnes,
            tb1.estabelecimento_nome_atendimento,
            tb1.estabelecimento_nome_cadastro AS estabelecimento_nome,
            tb1.equipe_ine_atendimento,
            -- coluna usada na vinculação de equipe
            impulso_previne_dados_nominais.equipe_ine(tb1.municipio_id_sus::text, COALESCE(tb1.equipe_ine_cadastro, tb1.equipe_ine_atendimento, tb1.equipe_ine_procedimento, '0')) AS equipe_ine_cadastro,
            tb1.equipe_nome_atendimento,
            -- coluna usada na vinculação de equipe
            impulso_previne_dados_nominais.equipe_ine(tb1.municipio_id_sus::text, COALESCE(tb1.equipe_nome_cadastro, tb1.equipe_nome_atendimento, tb1.equipe_nome_procedimento, 'SEM EQUIPE RESPONSÁVEL')) AS equipe_nome_cadastro,
            -- coluna usada na vinculação de profissional
            COALESCE(tb1.acs_nome_cadastro,tb1.acs_nome_visita, tb1.profissional_nome_atendimento, tb1.profissional_nome_procedimento, 'SEM PROFISSIONAL RESPONSÁVEL') AS acs_nome_cadastro,
            tb1.acs_nome_visita,
            tb1.possui_hipertensao_autorreferida,
            tb1.possui_hipertensao_diagnosticada,
                CASE
                    WHEN tb1.possui_hipertensao_autorreferida AND tb1.possui_hipertensao_diagnosticada IS FALSE THEN 1
                    WHEN tb1.possui_hipertensao_autorreferida AND tb1.possui_hipertensao_diagnosticada IS NULL THEN 1
                    ELSE 0
                END AS apenas_autorreferida,
                CASE
                    WHEN tb1.possui_hipertensao_diagnosticada THEN 1
                    ELSE 0
                END AS diagnostico_clinico,
            tb1.data_ultimo_cadastro,
            tb1.dt_ultima_consulta,
            tb1.se_faleceu,
            tb1.se_mudou,
            tb1.criacao_data,
            CURRENT_TIMESTAMP AS atualizacao_data
        FROM dados_transmissoes_recentes tb1
        LEFT JOIN listas_de_codigos.municipios tb2 
            ON tb1.municipio_id_sus::bpchar = tb2.id_sus
        WHERE COALESCE(tb1.se_faleceu, 0) <> 1
        ), tabela_final AS (
        SELECT 
            tabela_aux.municipio_id_sus,
            tabela_aux.municipio_uf,
            tabela_aux.quadrimestre_atual,
            tabela_aux.realizou_afericao_ultimos_6_meses,
            tabela_aux.dt_afericao_pressao_mais_recente,
            tabela_aux.realizou_consulta_ultimos_6_meses,
            tabela_aux.dt_consulta_mais_recente,
            tabela_aux.prazo_proxima_afericao_pa,
            tabela_aux.prazo_proxima_consulta,
            tabela_aux.consulta_e_afericao_em_dia,
            tabela_aux.status_em_dia,
            tabela_aux.status_usuario,
            tabela_aux.identificacao_condicao_hipertensao,
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
            tabela_aux.estabelecimento_cnes,
            tabela_aux.estabelecimento_nome_atendimento,
            tabela_aux.estabelecimento_nome,
            tabela_aux.equipe_ine_atendimento,
            tabela_aux.equipe_ine_cadastro,
            tabela_aux.equipe_nome_atendimento,
            tabela_aux.equipe_nome_cadastro,
            tabela_aux.acs_nome_cadastro,
            tabela_aux.acs_nome_visita,
            tabela_aux.possui_hipertensao_autorreferida,
            tabela_aux.possui_hipertensao_diagnosticada,
            tabela_aux.apenas_autorreferida,
            tabela_aux.diagnostico_clinico,
            tabela_aux.data_ultimo_cadastro,
            tabela_aux.dt_ultima_consulta,
            tabela_aux.se_faleceu,
            tabela_aux.se_mudou,
            tabela_aux.criacao_data,
            tabela_aux.atualizacao_data,
            drp.dt_registro_producao_mais_recente,
            ROW_NUMBER() OVER (PARTITION BY tabela_aux.municipio_id_sus) AS seq_demo_viscosa
        FROM tabela_aux
        LEFT JOIN data_registro_producao drp 
            ON drp.municipio_id_sus::text = tabela_aux.municipio_id_sus::text 
            AND drp.equipe_ine_cadastro = tabela_aux.equipe_ine_cadastro
    ), dados_demo_vicosa AS (
            SELECT 
                '111111' AS municipio_id_sus,
                'Demo - Viçosa - MG' AS municipio_uf,
                tf.quadrimestre_atual,
                tf.realizou_afericao_ultimos_6_meses,
                tf.dt_afericao_pressao_mais_recente,
                tf.realizou_consulta_ultimos_6_meses,
                tf.dt_consulta_mais_recente,
                tf.prazo_proxima_afericao_pa,
                tf.prazo_proxima_consulta,
                tf.consulta_e_afericao_em_dia,
                tf.status_em_dia,
                tf.status_usuario,
                tf.identificacao_condicao_hipertensao,
                concat(impulso_previne_dados_nominais.random_between(100000000, 999999999)::text, impulso_previne_dados_nominais.random_between(10, 99)::text) AS cidadao_cpf,
                concat(impulso_previne_dados_nominais.random_between(100000000, 999999999)::text, impulso_previne_dados_nominais.random_between(10, 99)::text) AS cidadao_cpf_dt_nascimento,
                concat('7', impulso_previne_dados_nominais.random_between(100000000, 999999999)::text, impulso_previne_dados_nominais.random_between(10000, 99999)::text)  AS cidadao_cns,
                upper(nomes.nome_ficticio) AS cidadao_nome,
                tf.cidadao_nome_social,
                tf.cidadao_sexo,
                tf.dt_nascimento,
                tf.cidadao_idade,
                tf.cidadao_faixa_etaria,
                tf.estabelecimento_cnes_atendimento,
                tf.estabelecimento_cnes,
                tf.estabelecimento_nome_atendimento,
                tf.estabelecimento_nome,
                tf.equipe_ine_atendimento,
                tf.equipe_ine_cadastro,
                tf.equipe_nome_atendimento,
                tf.equipe_nome_cadastro,
                upper(nomes2.nome_ficticio) AS acs_nome_cadastro,
                upper(nomes2.nome_ficticio) AS acs_nome_visita,
                tf.possui_hipertensao_autorreferida,
                tf.possui_hipertensao_diagnosticada,
                tf.apenas_autorreferida,
                tf.diagnostico_clinico,
                tf.data_ultimo_cadastro,
                tf.dt_ultima_consulta,
                tf.se_faleceu,
                tf.se_mudou,
                tf.criacao_data,
                tf.atualizacao_data,
                tf.dt_registro_producao_mais_recente
            FROM tabela_final tf
            LEFT JOIN configuracoes.nomes_ficticios_citopatologico nomes 
                ON tf.seq_demo_viscosa = nomes.seq
            LEFT JOIN configuracoes.nomes_ficticios_hipertensos nomes2 
                ON tf.seq_demo_viscosa = nomes2.seq
            WHERE municipio_id_sus = '140015' -- BONFIM - RR
    )
    SELECT
        ddv.municipio_id_sus,
        ddv.municipio_uf,
        ddv.quadrimestre_atual,
        ddv.realizou_afericao_ultimos_6_meses,
        ddv.dt_afericao_pressao_mais_recente,
        ddv.realizou_consulta_ultimos_6_meses,
        ddv.dt_consulta_mais_recente,
        ddv.prazo_proxima_afericao_pa,
        ddv.prazo_proxima_consulta,
        ddv.consulta_e_afericao_em_dia,
        ddv.status_em_dia,
        ddv.status_usuario,
        ddv.identificacao_condicao_hipertensao,
        ddv.cidadao_cpf,
        ddv.cidadao_cpf_dt_nascimento,
        ddv.cidadao_cns,
        ddv.cidadao_nome,
        ddv.cidadao_nome_social,
        ddv.cidadao_sexo,
        ddv.dt_nascimento,
        ddv.cidadao_idade,
        ddv.cidadao_faixa_etaria,
        ddv.estabelecimento_cnes_atendimento,
        ddv.estabelecimento_cnes,
        ddv.estabelecimento_nome_atendimento,
        ddv.estabelecimento_nome,
        ddv.equipe_ine_atendimento,
        ddv.equipe_ine_cadastro,
        ddv.equipe_nome_atendimento,
        ddv.equipe_nome_cadastro,
        ddv.acs_nome_cadastro,
        ddv.acs_nome_visita,
        ddv.possui_hipertensao_autorreferida,
        ddv.possui_hipertensao_diagnosticada,
        ddv.apenas_autorreferida,
        ddv.diagnostico_clinico,
        ddv.data_ultimo_cadastro,
        ddv.dt_ultima_consulta,
        ddv.se_faleceu,
        ddv.se_mudou,
        ddv.criacao_data,
        ddv.atualizacao_data,
        ddv.dt_registro_producao_mais_recente
    FROM dados_demo_vicosa ddv 
UNION ALL 
    SELECT
        tf.municipio_id_sus,
        tf.municipio_uf,
        tf.quadrimestre_atual,
        tf.realizou_afericao_ultimos_6_meses,
        tf.dt_afericao_pressao_mais_recente,
        tf.realizou_consulta_ultimos_6_meses,
        tf.dt_consulta_mais_recente,
        tf.prazo_proxima_afericao_pa,
        tf.prazo_proxima_consulta,
        tf.consulta_e_afericao_em_dia,
        tf.status_em_dia,
        tf.status_usuario,
        tf.identificacao_condicao_hipertensao,
        tf.cidadao_cpf,
        tf.cidadao_cpf_dt_nascimento,
        tf.cidadao_cns,
        tf.cidadao_nome,
        tf.cidadao_nome_social,
        tf.cidadao_sexo,
        tf.dt_nascimento,
        tf.cidadao_idade,
        tf.cidadao_faixa_etaria,
        tf.estabelecimento_cnes_atendimento,
        tf.estabelecimento_cnes,
        tf.estabelecimento_nome_atendimento,
        tf.estabelecimento_nome,
        tf.equipe_ine_atendimento,
        tf.equipe_ine_cadastro,
        tf.equipe_nome_atendimento,
        tf.equipe_nome_cadastro,
        tf.acs_nome_cadastro,
        tf.acs_nome_visita,
        tf.possui_hipertensao_autorreferida,
        tf.possui_hipertensao_diagnosticada,
        tf.apenas_autorreferida,
        tf.diagnostico_clinico,
        tf.data_ultimo_cadastro,
        tf.dt_ultima_consulta,
        tf.se_faleceu,
        tf.se_mudou,
        tf.criacao_data,
        tf.atualizacao_data,
        tf.dt_registro_producao_mais_recente
    FROM tabela_final tf
    WITH DATA;