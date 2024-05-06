
CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.api_futuro_painel_hipertensos_lista_nominal
TABLESPACE pg_default
AS WITH dados_transmissoes_recentes AS (
         SELECT 
            tb1_1.municipio_id_sus,
            tb1_1.realizou_afericao_ultimos_6_meses,
            tb1_1.dt_afericao_pressao_mais_recente,
            tb1_1.realizou_consulta_ultimos_6_meses,
            tb1_1.dt_consulta_mais_recente,
            tb1_1.co_seq_fat_cidadao_pec,
            tb1_1.cidadao_cpf,
            tb1_1.cidadao_nome,
            tb1_1.cidadao_sexo,
            tb1_1.dt_nascimento,
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
                impulso_previne_dados_nominais.equipe_ine(dtr.municipio_id_sus::text, COALESCE(dtr.equipe_ine_cadastro, dtr.equipe_ine_atendimento, dtr.equipe_ine_procedimento, '0')) AS equipe_ine,
                max(GREATEST(dtr.dt_afericao_pressao_mais_recente::date, dtr.dt_consulta_mais_recente, dtr.data_ultimo_cadastro, dtr.dt_ultima_consulta)) AS dt_registro_producao_mais_recente,
                min(LEAST(dtr.dt_afericao_pressao_mais_recente::date, dtr.dt_consulta_mais_recente, dtr.data_ultimo_cadastro, dtr.dt_ultima_consulta)) AS dt_registro_producao_mais_antigo
            FROM dados_transmissoes_recentes dtr
            GROUP BY 1, 2
        ), tabela_aux as (
            SELECT 
                tb1.municipio_id_sus,
                concat(tb2.nome, ' - ', tb2.uf_sigla) AS municipio_uf,
                tb1.cidadao_nome,
                tb1.dt_nascimento,
                CASE
                    WHEN tb1.cidadao_cpf IS NULL THEN tb1.dt_nascimento::text
                    ELSE tb1.cidadao_cpf
                END AS cidadao_cpf_dt_nascimento,
                CASE
                    WHEN tb1.possui_hipertensao_diagnosticada THEN 2 -- Diagnóstico Clínico tem preferência em relação a Autorreferência
                    WHEN tb1.possui_hipertensao_autorreferida THEN 1 -- Apenas autorreferido
                    ELSE 0
                END AS id_tipo_de_diagnostico,
                EXTRACT(YEAR FROM age(CURRENT_DATE, tb1.dt_nascimento))  AS cidadao_idade_anos,
                tb1.dt_consulta_mais_recente,
                CASE
                    WHEN tb1.realizou_consulta_ultimos_6_meses THEN 'Em dia'
                    -- Prazo: data fim do quadrimestre
                    WHEN date_part('month', CURRENT_DATE) >= 1 AND date_part('month', CURRENT_DATE) <= 4 THEN 'Até 30/Abril'
                    WHEN date_part('month', CURRENT_DATE) >= 5 AND date_part('month', CURRENT_DATE) <= 8 THEN 'Até 31/Agosto'
                    WHEN date_part('month', CURRENT_DATE) >= 9 AND date_part('month', CURRENT_DATE) <= 12 THEN 'Até 31/Dezembro'
                            ELSE NULL
                END AS prazo_proxima_consulta,
                tb1.dt_afericao_pressao_mais_recente AS dt_afericao_pressao_mais_recente,
                CASE
                    WHEN tb1.realizou_afericao_ultimos_6_meses THEN 'Em dia'
                    WHEN date_part('month', CURRENT_DATE) >= 1 AND date_part('month', CURRENT_DATE) <= 4 THEN 'Até 30/Abril'
                    WHEN date_part('month', CURRENT_DATE) >= 5 AND date_part('month', CURRENT_DATE) <= 8 THEN 'Até 31/Agosto'
                    WHEN date_part('month', CURRENT_DATE) >= 9 AND date_part('month', CURRENT_DATE) <= 12 THEN 'Até 31/Dezembro'
                            ELSE NULL
                END AS prazo_proxima_afericao_pa,
                COALESCE(tb1.acs_nome_cadastro,tb1.acs_nome_visita, tb1.profissional_nome_atendimento, tb1.profissional_nome_procedimento, 'SEM PROFISSIONAL RESPONSÁVEL') AS acs_nome,
                impulso_previne_dados_nominais.equipe_ine(tb1.municipio_id_sus::text, COALESCE(tb1.equipe_ine_cadastro, tb1.equipe_ine_atendimento, tb1.equipe_ine_procedimento, '0')) AS equipe_ine,
                impulso_previne_dados_nominais.equipe_ine(tb1.municipio_id_sus::text, COALESCE(tb1.equipe_nome_cadastro, tb1.equipe_nome_atendimento, tb1.equipe_nome_procedimento, 'SEM EQUIPE RESPONSÁVEL')) AS equipe_nome,
                CASE
                    WHEN date_part('year', age(CURRENT_DATE, tb1.dt_nascimento)) <= 17 THEN 1
                    WHEN date_part('year', age(CURRENT_DATE, tb1.dt_nascimento)) > 17 AND date_part('year'::text, age(CURRENT_DATE, tb1.dt_nascimento)) <= 24 THEN 2
                    WHEN date_part('year', age(CURRENT_DATE, tb1.dt_nascimento)) > 24 AND date_part('year'::text, age(CURRENT_DATE, tb1.dt_nascimento)) <= 34 THEN 3
                    WHEN date_part('year', age(CURRENT_DATE, tb1.dt_nascimento)) > 34 AND date_part('year'::text, age(CURRENT_DATE, tb1.dt_nascimento)) <= 44 THEN 4
                    WHEN date_part('year', age(CURRENT_DATE, tb1.dt_nascimento)) > 44 AND date_part('year'::text, age(CURRENT_DATE, tb1.dt_nascimento)) <= 54 THEN 5
                    WHEN date_part('year', age(CURRENT_DATE, tb1.dt_nascimento)) > 54 AND date_part('year'::text, age(CURRENT_DATE, tb1.dt_nascimento)) <= 64 THEN 6
                    WHEN date_part('year', age(CURRENT_DATE, tb1.dt_nascimento)) > 64 THEN 4
                    WHEN tb1.dt_nascimento IS NULL THEN 0
                    ELSE 7
                END AS id_faixa_etaria,
                CASE
                    -- Menos de 40 anos
                    WHEN date_part('year', age(CURRENT_DATE, tb1.dt_nascimento)) <= 40 THEN 1
                    -- Entre 41 e 49 anos 
                    WHEN date_part('year', age(CURRENT_DATE, tb1.dt_nascimento)) > 40 AND date_part('year', age(CURRENT_DATE, tb1.dt_nascimento)) <= 49 THEN 2
                    -- Entre 50 e 59 anos 
                    WHEN date_part('year', age(CURRENT_DATE, tb1.dt_nascimento)) > 49 AND date_part('year', age(CURRENT_DATE, tb1.dt_nascimento)) <= 59 THEN 3
                    -- Entre 60 e 70 anos 
                    WHEN date_part('year', age(CURRENT_DATE, tb1.dt_nascimento)) > 59 AND date_part('year', age(CURRENT_DATE, tb1.dt_nascimento)) <= 70 THEN 4
                    -- Sem idade
                    WHEN tb1.dt_nascimento IS NULL THEN 0
                    -- Mais de 70 anos
                    ELSE 5
                END AS id_faixa_etaria2,
                CASE
                    -- Consulta e aferição de PA em dia
                    WHEN tb1.realizou_afericao_ultimos_6_meses AND tb1.realizou_consulta_ultimos_6_meses THEN 1
                    -- Os dois a fazer
                    WHEN tb1.realizou_afericao_ultimos_6_meses IS FALSE AND tb1.realizou_consulta_ultimos_6_meses IS FALSE THEN 2
                    -- Apenas a consulta a fazer
                    WHEN tb1.realizou_afericao_ultimos_6_meses AND tb1.realizou_consulta_ultimos_6_meses IS FALSE THEN 3
                    -- Apenas a aferição de PA a fazer
                    WHEN tb1.realizou_afericao_ultimos_6_meses IS FALSE AND tb1.realizou_consulta_ultimos_6_meses THEN 4
                    ELSE 0
                END AS id_status_usuario,
                tb1.criacao_data,
                CURRENT_TIMESTAMP AS atualizacao_data
            FROM dados_transmissoes_recentes tb1
            LEFT JOIN listas_de_codigos.municipios tb2 
                ON tb1.municipio_id_sus::bpchar = tb2.id_sus
            WHERE COALESCE(tb1.se_faleceu, 0) <> 1
), tabela_final AS (
    SELECT
        tabela_aux.*,
        drp.dt_registro_producao_mais_recente,
        ROW_NUMBER() OVER (PARTITION BY tabela_aux.municipio_id_sus) AS seq_demo_viscosa
    FROM tabela_aux
    LEFT JOIN data_registro_producao drp 
        ON drp.municipio_id_sus = tabela_aux.municipio_id_sus
        AND drp.equipe_ine = tabela_aux.equipe_ine
)
, dados_demo_vicosa AS (
            SELECT 
                '111111' AS municipio_id_sus,
                'Demo - Bonfim - RR' AS municipio_uf,
                upper(nomes.nome_ficticio) AS cidadao_nome,
                tf.dt_nascimento,
                concat(impulso_previne_dados_nominais.random_between(100000000, 999999999)::text, impulso_previne_dados_nominais.random_between(10, 99)::text) AS cidadao_cpf_dt_nascimento,
                tf.cidadao_idade_anos,
                tf.dt_consulta_mais_recente,
                tf.prazo_proxima_consulta,
                tf.dt_afericao_pressao_mais_recente,
                tf.prazo_proxima_afericao_pa,
                upper(nomes2.nome_ficticio) AS acs_nome,
                tf.equipe_ine,
                tf.equipe_nome,
                tf.id_tipo_de_diagnostico,
                tf.id_faixa_etaria,
                tf.id_status_usuario,
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
        ddv.cidadao_nome,
        ddv.dt_nascimento,
        ddv.cidadao_cpf_dt_nascimento,
        ddv.cidadao_idade_anos,
        ddv.dt_consulta_mais_recente,
        ddv.prazo_proxima_consulta,
        ddv.dt_afericao_pressao_mais_recente,
        ddv.prazo_proxima_afericao_pa,
        ddv.acs_nome,
        ddv.equipe_ine,
        ddv.equipe_nome,
        ddv.id_tipo_de_diagnostico,
        ddv.id_faixa_etaria,
        ddv.id_status_usuario,
        ddv.criacao_data,
        ddv.atualizacao_data,
        ddv.dt_registro_producao_mais_recente
    FROM dados_demo_vicosa ddv 
UNION ALL 
    SELECT 
        tf.municipio_id_sus,
        tf.municipio_uf,
        tf.cidadao_nome,
        tf.dt_nascimento,
        tf.cidadao_cpf_dt_nascimento,
        tf.cidadao_idade_anos,
        tf.dt_consulta_mais_recente,
        tf.prazo_proxima_consulta,
        tf.dt_afericao_pressao_mais_recente,
        tf.prazo_proxima_afericao_pa,
        tf.acs_nome,
        tf.equipe_ine,
        tf.equipe_nome,
        tf.id_tipo_de_diagnostico,
        tf.id_faixa_etaria,
        tf.id_status_usuario,
        tf.criacao_data,
        tf.atualizacao_data,
        tf.dt_registro_producao_mais_recente
    FROM tabela_final tf
WITH DATA;