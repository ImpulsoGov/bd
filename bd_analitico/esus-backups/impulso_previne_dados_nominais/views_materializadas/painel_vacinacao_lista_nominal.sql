-- impulso_previne_dados_nominais.painel_vacinacao_lista_nominal source

CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.painel_vacinacao_lista_nominal
TABLESPACE pg_default
AS WITH dados_transmissoes_recentes AS (
         SELECT tb1_1.chave_cidadao,
            tb1_1.cidadao_nome,
            tb1_1.cidadao_cpf,
            tb1_1.cidadao_cns,
            tb1_1.dt_nascimento,
            tb1_1.cidadao_idade_meses_atual,
            tb1_1.status_idade,
            tb1_1.quadrimestre_completa_1_ano,
            tb1_1.data_1dose_polio,
            tb1_1.data_2dose_polio,
            tb1_1.data_3dose_polio,
            tb1_1.qtde_vacinas_polio_registradas,
            tb1_1.quantidade_polio_validas,
            tb1_1.idade_meses_1dose_polio,
            tb1_1.idade_meses_2dose_polio,
            tb1_1.idade_meses_3dose_polio,
            tb1_1.prazo_1dose_polio,
            tb1_1.prazo_limite_1dose_polio,
            tb1_1.prazo_2dose_polio,
            tb1_1.prazo_3dose_polio,
            tb1_1.data_1dose_penta,
            tb1_1.data_2dose_penta,
            tb1_1.data_3dose_penta,
            tb1_1.qtde_vacinas_penta_registradas,
            tb1_1.quantidade_penta_validas,
            tb1_1.idade_meses_1dose_penta,
            tb1_1.idade_meses_2dose_penta,
            tb1_1.idade_meses_3dose_penta,
            tb1_1.prazo_1dose_penta,
            tb1_1.prazo_limite_1dose_penta,
            tb1_1.prazo_2dose_penta,
            tb1_1.prazo_3dose_penta,
            tb1_1.cidadao_nome_responsavel,
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
                WHEN TRIM(tb1_1.equipe_ine_aplicacao_vacina) = '-' OR tb1_1.equipe_ine_aplicacao_vacina = ' ' OR tb1_1.equipe_ine_aplicacao_vacina IS NULL 
                    THEN NULL
                ELSE TRIM(tb1_1.equipe_ine_aplicacao_vacina)
            END AS equipe_ine_aplicacao_vacina,
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
                WHEN tb1_1.equipe_nome_aplicacao_vacina = ' ' OR tb1_1.equipe_nome_aplicacao_vacina IS NULL OR tb1_1.equipe_nome_aplicacao_vacina LIKE '%SEM EQUIPE%' 
                    THEN NULL 
                ELSE TRIM(tb1_1.equipe_nome_aplicacao_vacina)
            END AS equipe_nome_aplicacao_vacina,
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
                WHEN UPPER(tb1_1.profissional_nome_aplicacao_vacina) LIKE '%PROFISSIONAL NÃO CADASTRADO%'
                    THEN 'ERRO CADASTRO PROFISSIONAL'
                WHEN tb1_1.profissional_nome_aplicacao_vacina = ' ' OR tb1_1.profissional_nome_aplicacao_vacina IS NULL OR UPPER(tb1_1.profissional_nome_aplicacao_vacina) LIKE '%NÃO INFORMADO%'
                    THEN NULL 
                ELSE TRIM(UPPER(tb1_1.profissional_nome_aplicacao_vacina))
            END AS profissional_nome_aplicacao_vacina,
            tb1_1.data_ultimo_cadastro_individual,
            tb1_1.data_ultimo_atendimento_individual,
            tb1_1.data_ultima_vista_domiciliar,
            tb1_1.criacao_data,
            tb1_1.municipio_id_sus,
            tb1_1.quadrimestre_atual
           FROM impulso_previne_dados_nominais.lista_nominal_vacinacao_unificada tb1_1
    ), data_registro_producao AS (
         SELECT dtr.municipio_id_sus,
            impulso_previne_dados_nominais.equipe_ine(dtr.municipio_id_sus, COALESCE(dtr.equipe_ine_cadastro, dtr.equipe_ine_atendimento, dtr.equipe_ine_aplicacao_vacina, '0')::text) AS equipe_ine,
            max(GREATEST(dtr.data_1dose_polio, 
			            dtr.data_2dose_polio, 
			            dtr.data_3dose_polio, 
			            dtr.data_1dose_penta, 
			            dtr.data_2dose_penta, 
			            dtr.data_3dose_penta, 
			            dtr.data_ultimo_cadastro_individual,
			            dtr.data_ultimo_atendimento_individual,
			            dtr.data_ultima_vista_domiciliar
			            )) AS dt_registro_producao_mais_recente,
            min(LEAST(dtr.data_1dose_polio, 
			            dtr.data_2dose_polio, 
			            dtr.data_3dose_polio, 
			            dtr.data_1dose_penta, 
			            dtr.data_2dose_penta, 
			            dtr.data_3dose_penta, 
			            dtr.data_ultimo_cadastro_individual,
			            dtr.data_ultimo_atendimento_individual,
			            dtr.data_ultima_vista_domiciliar
			            )) AS dt_registro_producao_mais_antigo
           FROM dados_transmissoes_recentes dtr
          GROUP BY 1,2
        ), tabela_aux AS (
         SELECT tb1.municipio_id_sus,
            tb1.cidadao_nome,
            COALESCE(tb1.cidadao_nome_responsavel, 'RESPONSÁVEL NÃO IDENTIFICADO'::character varying) AS cidadao_nome_responsavel,
            (tb1.cidadao_nome || tb1.municipio_id_sus::text) || tb1.dt_nascimento AS chave_cidadao,
            COALESCE(tb1.cidadao_cpf, tb1.dt_nascimento::text::character varying::text) AS cidadao_cpf_dt_nascimento,
            tb1.cidadao_idade_meses_atual AS cidadao_idade_meses,
            tb1.quadrimestre_completa_1_ano,
            tb1.quadrimestre_atual,
                CASE
                    WHEN split_part(tb1.quadrimestre_atual, '.'::text, 2) = 'Q1'::text AND split_part(tb1.quadrimestre_atual, '.'::text, 1) = date_part('year'::text, CURRENT_DATE)::text THEN concat(date_part('year'::text, CURRENT_DATE), '.Q2')
                    WHEN split_part(tb1.quadrimestre_atual, '.'::text, 2) = 'Q2'::text AND split_part(tb1.quadrimestre_atual, '.'::text, 1) = date_part('year'::text, CURRENT_DATE)::text THEN concat(date_part('year'::text, CURRENT_DATE), '.Q3')
                    WHEN split_part(tb1.quadrimestre_atual, '.'::text, 1) = date_part('year'::text, CURRENT_DATE)::text AND split_part(tb1.quadrimestre_atual, '.'::text, 2) = 'Q3'::text THEN concat(date_part('year'::text, CURRENT_DATE) + 1::double precision, '.Q1')
                    ELSE NULL::text
                END AS quadrimestre_futuro,
            tb1.data_1dose_polio,
            tb1.data_2dose_polio,
            tb1.data_3dose_polio,
            tb1.data_1dose_penta,
            tb1.data_2dose_penta,
            tb1.data_3dose_penta,
            tb1.idade_meses_1dose_polio,
            tb1.idade_meses_2dose_polio,
            tb1.idade_meses_3dose_polio,
            tb1.prazo_1dose_polio,
            tb1.prazo_limite_1dose_polio,
            tb1.prazo_2dose_polio,
            tb1.prazo_3dose_polio,
            tb1.prazo_1dose_penta,
            tb1.prazo_2dose_penta,
            tb1.prazo_3dose_penta,
            COALESCE(tb1.data_1dose_polio, tb1.prazo_1dose_polio) AS data_ou_prazo_1dose_polio,
            COALESCE(tb1.data_2dose_polio, tb1.prazo_2dose_polio) AS data_ou_prazo_2dose_polio,
            COALESCE(tb1.data_3dose_polio, tb1.prazo_3dose_polio) AS data_ou_prazo_3dose_polio,
                CASE
                    WHEN tb1.data_1dose_polio IS NOT NULL AND tb1.data_2dose_polio IS NOT NULL AND tb1.data_3dose_polio IS NOT NULL THEN 1
                    WHEN tb1.data_1dose_polio IS NOT NULL AND tb1.prazo_2dose_polio >= CURRENT_DATE AND tb1.prazo_3dose_polio >= CURRENT_DATE THEN 2
                    WHEN tb1.data_1dose_polio IS NOT NULL AND tb1.data_2dose_polio IS NOT NULL AND tb1.prazo_3dose_polio >= CURRENT_DATE THEN 2
                    WHEN tb1.prazo_1dose_polio < CURRENT_DATE OR tb1.prazo_2dose_polio < CURRENT_DATE OR tb1.prazo_3dose_polio < CURRENT_DATE THEN 3
                    WHEN tb1.data_1dose_polio IS NULL AND tb1.data_2dose_polio IS NULL AND tb1.data_3dose_polio IS NULL AND tb1.prazo_1dose_polio >= CURRENT_DATE AND tb1.prazo_2dose_polio >= CURRENT_DATE AND tb1.prazo_3dose_polio >= CURRENT_DATE THEN 4
                    ELSE NULL::integer
                END AS id_status_polio,
            COALESCE(tb1.data_1dose_penta, tb1.prazo_1dose_penta) AS data_ou_prazo_1dose_penta,
            COALESCE(tb1.data_2dose_penta, tb1.prazo_2dose_penta) AS data_ou_prazo_2dose_penta,
            COALESCE(tb1.data_3dose_penta, tb1.prazo_3dose_penta) AS data_ou_prazo_3dose_penta,
                CASE
                    WHEN tb1.data_1dose_penta IS NOT NULL AND tb1.data_2dose_penta IS NOT NULL AND tb1.data_3dose_penta IS NOT NULL THEN 1
                    WHEN tb1.data_1dose_penta IS NOT NULL AND tb1.prazo_2dose_penta >= CURRENT_DATE AND tb1.prazo_3dose_penta >= CURRENT_DATE THEN 2
                    WHEN tb1.data_1dose_penta IS NOT NULL AND tb1.data_2dose_penta IS NOT NULL AND tb1.prazo_3dose_penta >= CURRENT_DATE THEN 2
                    WHEN tb1.prazo_1dose_penta < CURRENT_DATE OR tb1.prazo_2dose_penta < CURRENT_DATE OR tb1.prazo_3dose_penta < CURRENT_DATE THEN 3
                    WHEN tb1.data_1dose_penta IS NULL AND tb1.data_2dose_penta IS NULL AND tb1.data_3dose_penta IS NULL AND tb1.prazo_1dose_penta >= CURRENT_DATE AND tb1.prazo_2dose_penta >= CURRENT_DATE AND tb1.prazo_3dose_penta >= CURRENT_DATE THEN 4
                    ELSE NULL::integer
                END AS id_status_penta,
            COALESCE(tb1.acs_nome_cadastro, tb1.acs_nome_visita, tb1.profissional_nome_atendimento, tb1.profissional_nome_aplicacao_vacina, 'SEM PROFISSIONAL RESPONSÁVEL') AS acs_nome,
            impulso_previne_dados_nominais.equipe_ine(tb1.municipio_id_sus::text, COALESCE(tb1.equipe_ine_cadastro, tb1.equipe_ine_atendimento, tb1.equipe_ine_aplicacao_vacina, '0')::text) AS equipe_ine,
            COALESCE(tb1.equipe_nome_cadastro, tb1.equipe_nome_atendimento, tb1.equipe_nome_aplicacao_vacina, 'SEM EQUIPE RESPONSÁVEL') AS equipe_nome,
            CURRENT_DATE AS atualizacao_data,
            tb1.criacao_data::date AS criacao_data
           FROM dados_transmissoes_recentes tb1
        ), cores AS (
         SELECT tb_1.municipio_id_sus,
            tb_1.cidadao_nome,
            tb_1.chave_cidadao,
            tb_1.cidadao_idade_meses,
            tb_1.data_1dose_polio,
            tb_1.data_2dose_polio,
            tb_1.data_3dose_polio,
            tb_1.prazo_1dose_polio,
            tb_1.prazo_2dose_polio,
            tb_1.prazo_3dose_polio,
            tb_1.id_status_polio,
            tb_1.data_ou_prazo_1dose_polio,
                CASE
                    WHEN tb_1.data_ou_prazo_1dose_polio = tb_1.data_1dose_polio THEN 2
                    WHEN tb_1.id_status_polio = 4 AND tb_1.data_ou_prazo_1dose_polio >= CURRENT_DATE THEN 1
                    WHEN tb_1.id_status_polio = 3 AND tb_1.data_ou_prazo_1dose_polio < CURRENT_DATE OR tb_1.data_1dose_polio IS NULL AND tb_1.data_ou_prazo_1dose_polio < CURRENT_DATE THEN 3
                    ELSE NULL::integer
                END AS id_cor_1dose_polio,
            tb_1.data_ou_prazo_2dose_polio,
                CASE
                    WHEN tb_1.data_ou_prazo_2dose_polio = tb_1.data_2dose_polio THEN 2
                    WHEN tb_1.id_status_polio = 4 AND tb_1.data_ou_prazo_2dose_polio >= CURRENT_DATE THEN 1
                    WHEN tb_1.data_2dose_polio IS NULL AND tb_1.data_ou_prazo_2dose_polio >= CURRENT_DATE THEN 1
                    WHEN tb_1.data_2dose_polio IS NULL AND tb_1.data_ou_prazo_2dose_polio < CURRENT_DATE THEN 3
                    ELSE NULL::integer
                END AS id_cor_2dose_polio,
            tb_1.data_ou_prazo_3dose_polio,
                CASE
                    WHEN tb_1.data_ou_prazo_3dose_polio = tb_1.data_3dose_polio THEN 2
                    WHEN tb_1.id_status_polio = 4 AND tb_1.data_ou_prazo_3dose_polio >= CURRENT_DATE THEN 1
                    WHEN tb_1.data_3dose_polio IS NULL AND tb_1.data_ou_prazo_3dose_polio >= CURRENT_DATE THEN 1
                    WHEN tb_1.data_3dose_polio IS NULL AND tb_1.data_ou_prazo_3dose_polio < CURRENT_DATE THEN 3
                    ELSE NULL::integer
                END AS id_cor_3dose_polio,
            tb_1.data_1dose_penta,
            tb_1.data_2dose_penta,
            tb_1.data_3dose_penta,
            tb_1.prazo_1dose_penta,
            tb_1.prazo_2dose_penta,
            tb_1.prazo_3dose_penta,
            tb_1.id_status_penta,
            tb_1.data_ou_prazo_1dose_penta,
                CASE
                    WHEN tb_1.data_ou_prazo_1dose_penta = tb_1.data_1dose_penta THEN 2
                    WHEN tb_1.id_status_penta = 4 AND tb_1.data_ou_prazo_1dose_penta >= CURRENT_DATE THEN 1
                    WHEN tb_1.id_status_penta = 3 AND tb_1.data_ou_prazo_1dose_penta < CURRENT_DATE OR tb_1.data_1dose_penta IS NULL AND tb_1.data_ou_prazo_1dose_penta < CURRENT_DATE THEN 3
                    ELSE NULL::integer
                END AS id_cor_1dose_penta,
            tb_1.data_ou_prazo_2dose_penta,
                CASE
                    WHEN tb_1.data_ou_prazo_2dose_penta = tb_1.data_2dose_penta THEN 2
                    WHEN tb_1.id_status_penta = 4 AND tb_1.data_ou_prazo_2dose_penta >= CURRENT_DATE THEN 1
                    WHEN tb_1.data_2dose_penta IS NULL AND tb_1.data_ou_prazo_2dose_penta >= CURRENT_DATE THEN 1
                    WHEN tb_1.data_2dose_penta IS NULL AND tb_1.data_ou_prazo_2dose_penta < CURRENT_DATE THEN 3
                    ELSE NULL::integer
                END AS id_cor_2dose_penta,
            tb_1.data_ou_prazo_3dose_penta,
                CASE
                    WHEN tb_1.data_ou_prazo_3dose_penta = tb_1.data_3dose_penta THEN 2
                    WHEN tb_1.id_status_penta = 4 AND tb_1.data_ou_prazo_3dose_penta >= CURRENT_DATE THEN 1
                    WHEN tb_1.data_3dose_penta IS NULL AND tb_1.data_ou_prazo_3dose_penta >= CURRENT_DATE THEN 1
                    WHEN tb_1.data_3dose_penta IS NULL AND tb_1.data_ou_prazo_3dose_penta < CURRENT_DATE THEN 3
                    ELSE NULL::integer
                END AS id_cor_3dose_penta
           FROM tabela_aux tb_1
), tabela_final AS ( 
            SELECT 
                tb.municipio_id_sus,
                concat(tb2.nome, ' - ', tb2.uf_sigla) AS municipio_uf,
                tb.cidadao_nome,
                tb.cidadao_nome_responsavel,
                tb.cidadao_cpf_dt_nascimento,
                tb.cidadao_idade_meses,
                tb.quadrimestre_completa_1_ano,
                    CASE
                        WHEN tb.quadrimestre_completa_1_ano::text = tb.quadrimestre_atual THEN 1
                        WHEN tb.quadrimestre_completa_1_ano::text = tb.quadrimestre_futuro THEN 2
                        ELSE 3
                    END AS id_status_quadrimestre,
                tb.data_ou_prazo_1dose_polio,
                tb.data_ou_prazo_2dose_polio,
                tb.data_ou_prazo_3dose_polio,
                tb.id_status_polio,
                c.id_cor_1dose_polio,
                c.id_cor_2dose_polio,
                c.id_cor_3dose_polio,
                tb.data_ou_prazo_1dose_penta,
                tb.data_ou_prazo_2dose_penta,
                tb.data_ou_prazo_3dose_penta,
                tb.id_status_penta,
                c.id_cor_1dose_penta,
                c.id_cor_2dose_penta,
                c.id_cor_3dose_penta,
                tb.acs_nome,
                tb.equipe_ine,
                tb.equipe_nome,
                tb.criacao_data,
                tb.atualizacao_data,
                drp.dt_registro_producao_mais_recente,
                ROW_NUMBER() OVER (PARTITION BY tb.municipio_id_sus) AS seq_demo_viscosa
            FROM tabela_aux tb
            LEFT JOIN data_registro_producao drp 
                ON drp.municipio_id_sus::text = tb.municipio_id_sus::text 
                AND drp.equipe_ine = tb.equipe_ine
            LEFT JOIN cores c 
                ON c.chave_cidadao = tb.chave_cidadao
            LEFT JOIN listas_de_codigos.municipios tb2 
                ON tb.municipio_id_sus::bpchar = tb2.id_sus
            LEFT JOIN listas_de_codigos.periodos p 
                ON tb.quadrimestre_completa_1_ano::text = p.codigo::text
            WHERE p.data_fim >= CURRENT_DATE
        ), dados_demo_vicosa AS (
            SELECT 
                '111111' AS municipio_id_sus,
                'Demo - Viçosa - MG' AS municipio_uf,
                upper(nomes.nome_ficticio) AS cidadao_nome,
                upper(nomes2.nome_ficticio) AS cidadao_nome_responsavel,
                concat(impulso_previne_dados_nominais.random_between(100000000, 999999999)::text, impulso_previne_dados_nominais.random_between(10, 99)::text) AS cidadao_cpf_dt_nascimento,
                tf.cidadao_idade_meses,
                tf.quadrimestre_completa_1_ano,
                tf.id_status_quadrimestre,
                tf.data_ou_prazo_1dose_polio,
                tf.data_ou_prazo_2dose_polio,
                tf.data_ou_prazo_3dose_polio,
                tf.id_status_polio,
                tf.id_cor_1dose_polio,
                tf.id_cor_2dose_polio,
                tf.id_cor_3dose_polio,
                tf.data_ou_prazo_1dose_penta,
                tf.data_ou_prazo_2dose_penta,
                tf.data_ou_prazo_3dose_penta,
                tf.id_status_penta,
                tf.id_cor_1dose_penta,
                tf.id_cor_2dose_penta,
                tf.id_cor_3dose_penta,
                upper(nomes2.nome_ficticio) AS acs_nome,
                tf.equipe_ine,
                tf.equipe_nome,
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
        ddv.cidadao_nome_responsavel,
        ddv.cidadao_cpf_dt_nascimento,
        ddv.cidadao_idade_meses,
        ddv.quadrimestre_completa_1_ano,
        ddv.id_status_quadrimestre,
        ddv.data_ou_prazo_1dose_polio,
        ddv.data_ou_prazo_2dose_polio,
        ddv.data_ou_prazo_3dose_polio,
        ddv.id_status_polio,
        ddv.id_cor_1dose_polio,
        ddv.id_cor_2dose_polio,
        ddv.id_cor_3dose_polio,
        ddv.data_ou_prazo_1dose_penta,
        ddv.data_ou_prazo_2dose_penta,
        ddv.data_ou_prazo_3dose_penta,
        ddv.id_status_penta,
        ddv.id_cor_1dose_penta,
        ddv.id_cor_2dose_penta,
        ddv.id_cor_3dose_penta,
        ddv.acs_nome,
        ddv.equipe_ine,
        ddv.equipe_nome,
        ddv.criacao_data,
        ddv.atualizacao_data,
        ddv.dt_registro_producao_mais_recente
    FROM dados_demo_vicosa ddv
UNION ALL 
    SELECT 
        tf.municipio_id_sus,
        tf.municipio_uf,
        tf.cidadao_nome,
        tf.cidadao_nome_responsavel,
        tf.cidadao_cpf_dt_nascimento,
        tf.cidadao_idade_meses,
        tf.quadrimestre_completa_1_ano,
        tf.id_status_quadrimestre,
        tf.data_ou_prazo_1dose_polio,
        tf.data_ou_prazo_2dose_polio,
        tf.data_ou_prazo_3dose_polio,
        tf.id_status_polio,
        tf.id_cor_1dose_polio,
        tf.id_cor_2dose_polio,
        tf.id_cor_3dose_polio,
        tf.data_ou_prazo_1dose_penta,
        tf.data_ou_prazo_2dose_penta,
        tf.data_ou_prazo_3dose_penta,
        tf.id_status_penta,
        tf.id_cor_1dose_penta,
        tf.id_cor_2dose_penta,
        tf.id_cor_3dose_penta,
        tf.acs_nome,
        tf.equipe_ine,
        tf.equipe_nome,
        tf.criacao_data,
        tf.atualizacao_data,
        tf.dt_registro_producao_mais_recente
    FROM tabela_final tf
WITH DATA;