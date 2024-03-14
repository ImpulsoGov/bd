-- impulso_previne_dados_nominais.painel_citopatologico_lista_nominal source

CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.painel_citopatologico_lista_nominal
TABLESPACE pg_default
AS WITH dados_transmissoes_recentes AS (
        SELECT tb1_1.municipio_id_sus,
            tb1_1.quadrimestre_atual,
            tb1_1.paciente_nome,
            tb1_1.cidadao_cpf,
            tb1_1.cidadao_cns,
            tb1_1.paciente_idade_atual,
            tb1_1.dt_nascimento,
            tb1_1.dt_ultimo_exame,
            tb1_1.realizou_exame_ultimos_36_meses,
            tb1_1.data_projetada_proximo_exame,
            tb1_1.status_exame,
            tb1_1.data_limite_a_realizar_proximo_exame,
            tb1_1.cnes_estabelecimento_exame,
            tb1_1.nome_estabelecimento_exame,
            CASE 
                WHEN TRIM(tb1_1.ine_equipe_exame) = '-' OR tb1_1.ine_equipe_exame = ' ' OR tb1_1.ine_equipe_exame IS NULL 
                    THEN NULL
                ELSE TRIM(tb1_1.ine_equipe_exame)
            END AS ine_equipe_exame,
            CASE 
                WHEN tb1_1.nome_equipe_exame = ' ' OR tb1_1.nome_equipe_exame IS NULL OR tb1_1.nome_equipe_exame LIKE '%SEM EQUIPE%' 
                    THEN NULL 
                ELSE TRIM(tb1_1.nome_equipe_exame)
            END AS nome_equipe_exame,
            CASE 
                WHEN UPPER(tb1_1.nome_profissional_exame) LIKE '%PROFISSIONAL NÃO CADASTRADO%'
                    THEN 'ERRO CADASTRO PROFISSIONAL'
                WHEN tb1_1.nome_profissional_exame = ' ' OR tb1_1.nome_profissional_exame IS NULL OR UPPER(tb1_1.nome_profissional_exame) LIKE '%NÃO INFORMADO%'
                    THEN NULL 
                ELSE TRIM(UPPER(tb1_1.nome_profissional_exame))
            END AS nome_profissional_exame,
            tb1_1.dt_ultimo_cadastro,
            tb1_1.estabelecimento_nome_cadastro,
            tb1_1.estabelecimento_cnes_cadastro,
            CASE 
                WHEN TRIM(tb1_1.equipe_ine_cadastro) = '-' OR tb1_1.equipe_ine_cadastro = ' ' OR tb1_1.equipe_ine_cadastro IS NULL
                    THEN NULL
                ELSE TRIM(tb1_1.equipe_ine_cadastro)
            END AS equipe_ine_cadastro,
            CASE 
                WHEN tb1_1.equipe_nome_cadastro = ' ' OR tb1_1.equipe_nome_cadastro IS NULL OR tb1_1.equipe_nome_cadastro LIKE '%SEM EQUIPE%' 
                    THEN NULL 
                ELSE TRIM(tb1_1.equipe_nome_cadastro)
            END AS equipe_nome_cadastro,
            CASE 
                WHEN UPPER(tb1_1.acs_nome_cadastro) LIKE '%PROFISSIONAL NÃO CADASTRADO%'
                    THEN 'ERRO CADASTRO PROFISSIONAL'
                WHEN tb1_1.acs_nome_cadastro = ' ' OR tb1_1.acs_nome_cadastro IS NULL OR UPPER(tb1_1.acs_nome_cadastro) LIKE '%NÃO INFORMADO%'
                    THEN NULL 
                ELSE TRIM(UPPER(tb1_1.acs_nome_cadastro))
            END AS acs_nome_cadastro,
            tb1_1.dt_ultimo_atendimento,
            tb1_1.estabelecimento_nome_ultimo_atendimento,
            tb1_1.estabelecimento_cnes_ultimo_atendimento,
            CASE 
                WHEN TRIM(tb1_1.equipe_ine_ultimo_atendimento) = '-' OR tb1_1.equipe_ine_ultimo_atendimento = ' ' OR tb1_1.equipe_ine_ultimo_atendimento IS NULL
                    THEN NULL
                ELSE TRIM(tb1_1.equipe_ine_ultimo_atendimento)
            END AS equipe_ine_ultimo_atendimento,
            CASE 
                WHEN tb1_1.equipe_nome_ultimo_atendimento = ' ' OR tb1_1.equipe_nome_ultimo_atendimento IS NULL OR tb1_1.equipe_nome_ultimo_atendimento LIKE '%SEM EQUIPE%' 
                    THEN NULL 
                ELSE TRIM(tb1_1.equipe_nome_ultimo_atendimento)
            END AS equipe_nome_ultimo_atendimento,
            CASE 
                WHEN UPPER(tb1_1.acs_nome_ultimo_atendimento) LIKE '%PROFISSIONAL NÃO CADASTRADO%'
                    THEN 'ERRO CADASTRO PROFISSIONAL'
                WHEN tb1_1.acs_nome_ultimo_atendimento = ' '  OR tb1_1.acs_nome_ultimo_atendimento IS NULL OR UPPER(tb1_1.acs_nome_ultimo_atendimento) LIKE '%NÃO INFORMADO%'
                    THEN NULL 
                ELSE TRIM(UPPER(tb1_1.acs_nome_ultimo_atendimento))
            END AS acs_nome_ultimo_atendimento,
            CASE 
                WHEN UPPER(tb1_1.acs_nome_visita) LIKE '%PROFISSIONAL NÃO CADASTRADO%'
                    THEN 'ERRO CADASTRO PROFISSIONAL'
                WHEN tb1_1.acs_nome_visita = ' '  OR tb1_1.acs_nome_visita IS NULL OR UPPER(tb1_1.acs_nome_visita) LIKE '%NÃO INFORMADO%'
                    THEN NULL 
                ELSE TRIM(UPPER(tb1_1.acs_nome_visita))
            END AS acs_nome_visita,
            tb1_1.criacao_data
           FROM impulso_previne_dados_nominais.lista_nominal_citopatologico_unificada tb1_1
        ), data_registro_producao AS (
         SELECT 
                dtr.municipio_id_sus,
                impulso_previne_dados_nominais.equipe_ine(dtr.municipio_id_sus, COALESCE(dtr.equipe_ine_cadastro, dtr.equipe_ine_ultimo_atendimento, dtr.ine_equipe_exame,'0')) AS ine_master,
                max(GREATEST(dtr.dt_ultimo_exame, dtr.dt_ultimo_atendimento, dtr.dt_ultimo_cadastro)) AS dt_registro_producao_mais_recente,
                min(LEAST(dtr.dt_ultimo_exame, dtr.dt_ultimo_atendimento, dtr.dt_ultimo_cadastro)) AS dt_registro_producao_mais_antigo
               FROM dados_transmissoes_recentes dtr
            GROUP BY 1, 2
        ), tabela_aux AS (
         SELECT tb1.municipio_id_sus,
            concat(tb2.nome, ' - ', tb2.uf_sigla) AS municipio_uf,
            tb1.paciente_nome,
                CASE
                    WHEN tb1.cidadao_cpf IS NULL THEN to_char(tb1.dt_nascimento::timestamp with time zone, 'DD/MM/YYYY'::text)
                    ELSE concat("substring"(tb1.cidadao_cpf, 1, 3), '.', "substring"(tb1.cidadao_cpf, 4, 3), '.', "substring"(tb1.cidadao_cpf, 7, 3), '-', "substring"(tb1.cidadao_cpf, 10, 2))
                END AS cidadao_cpf_dt_nascimento,
                CASE
                    WHEN tb1.status_exame::text = ANY (ARRAY['exame_realizado_antes_dos_25'::character varying::text, 'exame_nunca_realizado'::character varying::text]) THEN '-'::text
                    ELSE to_char(tb1.data_projetada_proximo_exame::timestamp with time zone, 'DD/MM/YYYY'::text)
                END AS vencimento_da_coleta,
                CASE
                    WHEN tb1.status_exame::text = 'exame_em_dia'::text THEN 'Em dia'::text
                    ELSE to_char(tb1.data_limite_a_realizar_proximo_exame::timestamp with time zone, 'DD/MM/YYYY'::text)
                END AS prazo_proxima_coleta,
            tb1.paciente_idade_atual AS idade,
            COALESCE(tb1.acs_nome_cadastro, tb1.acs_nome_visita, tb1.acs_nome_ultimo_atendimento, tb1.nome_profissional_exame, 'SEM PROFISSIONAL RESPONSÁVEL') AS acs_nome,
            COALESCE(tb1.estabelecimento_cnes_cadastro, tb1.estabelecimento_cnes_ultimo_atendimento) AS estabelecimento_cnes,
            COALESCE(tb1.estabelecimento_nome_cadastro, tb1.estabelecimento_nome_ultimo_atendimento) AS estabelecimento_nome,
            COALESCE(tb1.equipe_ine_cadastro, tb1.equipe_ine_ultimo_atendimento, tb1.ine_equipe_exame, '0') AS equipe_ine,
            impulso_previne_dados_nominais.equipe_ine(tb1.municipio_id_sus::text, COALESCE(tb1.equipe_ine_cadastro, tb1.equipe_ine_ultimo_atendimento, tb1.ine_equipe_exame,'0')::text) AS ine_master,
            COALESCE(tb1.equipe_nome_cadastro, tb1.equipe_nome_ultimo_atendimento, tb1.nome_equipe_exame, 'SEM EQUIPE RESPONSÁVEL') AS equipe_nome,
                CASE
                    WHEN tb1.status_exame::text = 'exame_em_dia'::text THEN 12
                    WHEN tb1.status_exame::text = 'exame_nunca_realizado'::text THEN 13
                    WHEN tb1.status_exame::text = 'exame_realizado_antes_dos_25'::text THEN 14
                    WHEN tb1.status_exame::text = 'exame_vence_no_quadrimestre_atual'::text THEN 15
                    WHEN tb1.status_exame::text = 'exame_vencido'::text THEN 16
                    ELSE NULL::integer
                END AS id_status_usuario,
                CASE
                    WHEN tb1.paciente_idade_atual <= 39 THEN 6
                    WHEN tb1.paciente_idade_atual >= 40 AND tb1.paciente_idade_atual <= 49 THEN 7
                    WHEN tb1.paciente_idade_atual >= 50 AND tb1.paciente_idade_atual <= 64 THEN 8
                    ELSE NULL::integer
                END AS id_faixa_etaria,
            tb1.criacao_data,
            CURRENT_TIMESTAMP AS atualizacao_data,
            ROW_NUMBER() OVER (PARTITION BY municipio_id_sus) AS seq_demo_viscosa
        FROM dados_transmissoes_recentes tb1
        LEFT JOIN listas_de_codigos.municipios tb2 
            ON tb1.municipio_id_sus = tb2.id_sus
        )
    , tabela_final AS (
        SELECT 
            tabela_aux.municipio_id_sus,
            tabela_aux.municipio_uf,
            tabela_aux.paciente_nome,
            tabela_aux.cidadao_cpf_dt_nascimento,
            tabela_aux.vencimento_da_coleta,
            tabela_aux.prazo_proxima_coleta,
            tabela_aux.idade,
            tabela_aux.acs_nome,
            tabela_aux.estabelecimento_cnes,
            tabela_aux.estabelecimento_nome,
            tabela_aux.equipe_ine,
            tabela_aux.ine_master,
            tabela_aux.equipe_nome,
            tabela_aux.id_status_usuario,
            tabela_aux.id_faixa_etaria,
            tabela_aux.criacao_data,
            tabela_aux.atualizacao_data,
            drp.dt_registro_producao_mais_recente,
            ROW_NUMBER() OVER (PARTITION BY tabela_aux.municipio_id_sus) AS seq_demo_viscosa
        FROM tabela_aux
        LEFT JOIN data_registro_producao drp 
            ON drp.municipio_id_sus = tabela_aux.municipio_id_sus 
            AND drp.ine_master = tabela_aux.ine_master
    ) , dados_demo_bonfim AS (
        SELECT 
            '111111' AS municipio_id_sus,
            'Demo - Bonfim - RR' AS municipio_uf,
            upper(nomes.nome_ficticio) AS paciente_nome,
            concat(impulso_previne_dados_nominais.random_between(100000000, 999999999)::text, impulso_previne_dados_nominais.random_between(10, 99)::text) AS cidadao_cpf_dt_nascimento,
            tf.vencimento_da_coleta,
            tf.prazo_proxima_coleta,
            tf.idade,
            upper(nomes2.nome_ficticio) AS acs_nome,
            tf.estabelecimento_cnes,
            tf.estabelecimento_nome,
            tf.equipe_ine,
            tf.ine_master,
            tf.equipe_nome,
            tf.id_status_usuario,
            tf.id_faixa_etaria,
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
        ddv.paciente_nome,
        ddv.cidadao_cpf_dt_nascimento,
        ddv.vencimento_da_coleta,
        ddv.prazo_proxima_coleta,
        ddv.idade,
        ddv.acs_nome,
        ddv.estabelecimento_cnes,
        ddv.estabelecimento_nome,
        ddv.equipe_ine,
        ddv.ine_master,
        ddv.equipe_nome,
        ddv.id_status_usuario,
        ddv.id_faixa_etaria,
        ddv.criacao_data,
        ddv.atualizacao_data,
        ddv.dt_registro_producao_mais_recente
    FROM dados_demo_bonfim ddv
UNION ALL 
    SELECT 
       tf.municipio_id_sus,
       tf.municipio_uf,
       tf.paciente_nome,
       tf.cidadao_cpf_dt_nascimento,
       tf.vencimento_da_coleta,
       tf.prazo_proxima_coleta,
       tf.idade,
       tf.acs_nome,
       tf.estabelecimento_cnes,
       tf.estabelecimento_nome,
       tf.equipe_ine,
       tf.ine_master,
       tf.equipe_nome,
       tf.id_status_usuario,
       tf.id_faixa_etaria,
       tf.criacao_data,
       tf.atualizacao_data,
       tf.dt_registro_producao_mais_recente
    FROM tabela_final tf
WITH DATA;