CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.painel_gestantes_lista_nominal
TABLESPACE pg_default
AS WITH dados_transmissoes_recentes AS (
        SELECT
            tb1_1.chave_gestacao,
            CASE 
                WHEN TRIM(tb1_1.equipe_ine_atendimento) = '-' OR tb1_1.equipe_ine_atendimento = ' ' OR tb1_1.equipe_ine_atendimento IS NULL 
                    THEN NULL
                ELSE TRIM(tb1_1.equipe_ine_atendimento)
            END AS equipe_ine_atendimento,
            CASE 
                WHEN TRIM(tb1_1.equipe_nome_cad_individual) = '-' OR tb1_1.equipe_nome_cad_individual = ' ' OR tb1_1.equipe_nome_cad_individual IS NULL 
                    THEN NULL
                ELSE TRIM(tb1_1.equipe_nome_cad_individual)
            END AS equipe_ine_cadastro,
            CASE 
                WHEN tb1_1.equipe_nome_atendimento = ' ' OR tb1_1.equipe_nome_atendimento IS NULL OR tb1_1.equipe_nome_atendimento LIKE '%SEM EQUIPE%' 
                    THEN NULL 
                ELSE TRIM(tb1_1.equipe_nome_atendimento)
            END AS equipe_nome_atendimento,
            CASE 
                WHEN tb1_1.equipe_nome_cad_individual = ' ' OR tb1_1.equipe_nome_cad_individual IS NULL OR tb1_1.equipe_nome_cad_individual LIKE '%SEM EQUIPE%' 
                    THEN NULL 
                ELSE TRIM(tb1_1.equipe_nome_cad_individual)
            END AS equipe_nome_cadastro,
            CASE 
                WHEN UPPER(tb1_1.acs_cad_individual) LIKE '%PROFISSIONAL NÃO CADASTRADO%'
                    THEN 'ERRO CADASTRO PROFISSIONAL'
                WHEN tb1_1.acs_cad_individual = ' ' OR tb1_1.acs_cad_individual IS NULL OR UPPER(tb1_1.acs_cad_individual) LIKE '%NÃO INFORMADO%'
                    THEN NULL 
                ELSE TRIM(UPPER(tb1_1.acs_cad_individual))
            END AS acs_nome_cadastro,
            CASE 
                WHEN UPPER(tb1_1.acs_visita_domiciliar) LIKE '%PROFISSIONAL NÃO CADASTRADO%'
                    THEN 'ERRO CADASTRO PROFISSIONAL'
                WHEN tb1_1.acs_visita_domiciliar = ' ' OR tb1_1.acs_visita_domiciliar IS NULL OR UPPER(tb1_1.acs_visita_domiciliar) LIKE '%NÃO INFORMADO%'
                    THEN NULL 
                ELSE TRIM(UPPER(tb1_1.acs_visita_domiciliar))
            END AS acs_nome_visita,
            CASE 
                WHEN UPPER(tb1_1.profissional_nome_atendimento) LIKE '%PROFISSIONAL NÃO CADASTRADO%'
                    THEN 'ERRO CADASTRO PROFISSIONAL'
                WHEN tb1_1.profissional_nome_atendimento = ' ' OR tb1_1.profissional_nome_atendimento IS NULL OR UPPER(tb1_1.profissional_nome_atendimento) LIKE '%NÃO INFORMADO%'
                    THEN NULL 
                ELSE TRIM(UPPER(tb1_1.profissional_nome_atendimento))
            END AS profissional_nome_atendimento,
            tb1_1.acs_data_ultima_visita,
            tb1_1.gestante_documento_cpf,
            tb1_1.gestante_documento_cns,
            tb1_1.gestante_nome,
            tb1_1.gestante_data_de_nascimento,
            tb1_1.gestante_telefone,
            tb1_1.gestacao_data_dum,
            tb1_1.gestacao_idade_gestacional_atual,
            tb1_1.gestacao_idade_gestacional_primeiro_atendimento,
            tb1_1.gestacao_data_dpp,
            tb1_1.gestacao_data_dpp AS gestacao_consulta_prenatal_data_limite,
            tb1_1.gestacao_dpp_dias_para,
            tb1_1.consultas_pre_natal_validas,
            tb1_1.consulta_prenatal_ultima_data,
            tb1_1.consulta_prenatal_ultima_dias_desde,
            tb1_1.atendimento_odontologico_realizado_valido,
            tb1_1.exame_hiv_realizado_valido,
            tb1_1.exame_sifilis_realizado_valido,
            tb1_1.exame_sifilis_hiv_realizado_valido,
            tb1_1.possui_registro_aborto,
            tb1_1.possui_registro_parto,
            tb1_1.criacao_data,
            tb1_1.municipio_id_sus
        FROM impulso_previne_dados_nominais.lista_nominal_gestantes_unificada tb1_1
        ), data_registro_producao AS (
            SELECT 
                dtr.municipio_id_sus,
                impulso_previne_dados_nominais.equipe_ine(dtr.municipio_id_sus::text, COALESCE(dtr.equipe_ine_cadastro, dtr.equipe_ine_atendimento, '0')) AS equipe_ine,
                max(GREATEST(dtr.consulta_prenatal_ultima_data)) AS dt_registro_producao_mais_recente,
                min(LEAST(dtr.consulta_prenatal_ultima_data)) AS dt_registro_producao_mais_antigo
            FROM dados_transmissoes_recentes dtr
            GROUP BY 1, 2
        ), tabela_aux AS (
        SELECT 
            tb1.chave_gestacao AS chave_id_gestacao,
            tb1.municipio_id_sus,
            impulso_previne_dados_nominais.equipe_ine(tb1.municipio_id_sus::text, COALESCE(tb1.equipe_ine_cadastro, tb1.equipe_ine_atendimento, '0')) AS equipe_ine,
            impulso_previne_dados_nominais.equipe_ine(tb1.municipio_id_sus::text, COALESCE(tb1.equipe_nome_cadastro, tb1.equipe_nome_atendimento, 'SEM EQUIPE RESPONSÁVEL')) AS equipe_nome,
            tb1.gestante_nome AS cidadao_nome,
            COALESCE(tb1.gestante_documento_cpf, tb1.gestante_data_de_nascimento::text) AS cidadao_cpf_dt_nascimento,
            tb1.gestacao_data_dpp,
                CASE
                    WHEN date_part('month', tb1.gestacao_data_dpp) >= 1 AND date_part('month', tb1.gestacao_data_dpp) <= 4 THEN concat(date_part('year', tb1.gestacao_data_dpp), '.Q1')
                    WHEN date_part('month', tb1.gestacao_data_dpp) >= 5 AND date_part('month', tb1.gestacao_data_dpp) <= 8 THEN concat(date_part('year', tb1.gestacao_data_dpp), '.Q2')
                    WHEN date_part('month', tb1.gestacao_data_dpp) >= 9 AND date_part('month', tb1.gestacao_data_dpp) <= 12 THEN concat(date_part('year', tb1.gestacao_data_dpp), '.Q3')
                    ELSE 'sem DUM'
                END AS gestacao_quadrimestre,
                CASE
                    WHEN tb1.gestacao_data_dum IS NULL 
                        OR tb1.gestacao_data_dpp < (CASE -- Ou DPP é menor que a data de início do quadrimestre anterior
                            WHEN date_part('month', CURRENT_DATE) >= 1 AND date_part('month', CURRENT_DATE) <= 4 THEN concat((date_part('year', CURRENT_DATE) - 1), '-09-01')::DATE
                            WHEN date_part('month', CURRENT_DATE) >= 5 AND date_part('month', CURRENT_DATE) <= 8 THEN concat(date_part('year', CURRENT_DATE), '-01-01')::DATE
                            WHEN date_part('month', CURRENT_DATE) >= 9 AND date_part('month', CURRENT_DATE) <= 12 THEN concat(date_part('year', CURRENT_DATE), '-05-01')::DATE
                         END)
                        THEN NULL::integer
                    ELSE tb1.gestacao_idade_gestacional_atual
                END AS gestacao_idade_gestacional_atual,
                CASE
                    WHEN tb1.gestacao_data_dum IS NULL 
                        OR tb1.gestacao_data_dpp < (CASE -- Ou DPP é menor que a data de início do quadrimestre anterior
                            WHEN date_part('month', CURRENT_DATE) >= 1 AND date_part('month', CURRENT_DATE) <= 4 THEN concat((date_part('year', CURRENT_DATE) - 1), '-09-01')::DATE
                            WHEN date_part('month', CURRENT_DATE) >= 5 AND date_part('month', CURRENT_DATE) <= 8 THEN concat(date_part('year', CURRENT_DATE), '-01-01')::DATE
                            WHEN date_part('month', CURRENT_DATE) >= 9 AND date_part('month', CURRENT_DATE) <= 12 THEN concat(date_part('year', CURRENT_DATE), '-05-01')::DATE
                         END)                        
                        THEN NULL::integer
                    ELSE tb1.gestacao_idade_gestacional_primeiro_atendimento
                END AS gestacao_idade_gestacional_primeiro_atendimento,
            tb1.consulta_prenatal_ultima_data,
                CASE
                    WHEN tb1.gestacao_data_dum IS NULL 
                        OR tb1.gestacao_data_dpp < (CASE -- Ou DPP é menor que a data de início do quadrimestre anterior
                            WHEN date_part('month', CURRENT_DATE) >= 1 AND date_part('month', CURRENT_DATE) <= 4 THEN concat((date_part('year', CURRENT_DATE) - 1), '-09-01')::DATE
                            WHEN date_part('month', CURRENT_DATE) >= 5 AND date_part('month', CURRENT_DATE) <= 8 THEN concat(date_part('year', CURRENT_DATE), '-01-01')::DATE
                            WHEN date_part('month', CURRENT_DATE) >= 9 AND date_part('month', CURRENT_DATE) <= 12 THEN concat(date_part('year', CURRENT_DATE), '-05-01')::DATE
                         END)     
                        THEN NULL::bigint
                    ELSE tb1.consultas_pre_natal_validas
                END AS consultas_pre_natal_validas,
                CASE
                    WHEN tb1.gestacao_data_dum IS NULL 
                        OR tb1.gestacao_data_dpp < (CASE -- Ou DPP é menor que a data de início do quadrimestre anterior
                            WHEN date_part('month', CURRENT_DATE) >= 1 AND date_part('month', CURRENT_DATE) <= 4 THEN concat((date_part('year', CURRENT_DATE) - 1), '-09-01')::DATE
                            WHEN date_part('month', CURRENT_DATE) >= 5 AND date_part('month', CURRENT_DATE) <= 8 THEN concat(date_part('year', CURRENT_DATE), '-01-01')::DATE
                            WHEN date_part('month', CURRENT_DATE) >= 9 AND date_part('month', CURRENT_DATE) <= 12 THEN concat(date_part('year', CURRENT_DATE), '-05-01')::DATE
                         END) 
                         THEN 3 -- Retona '-' para gestantes sem DUM
                    WHEN tb1.atendimento_odontologico_realizado_valido THEN 1 -- Atend. odontológico identificado
                    WHEN tb1.atendimento_odontologico_realizado_valido IS FALSE THEN 2  -- Atend. odontológico não identificado
                    ELSE 0
                END AS id_atendimento_odontologico,
                CASE
                    WHEN tb1.gestacao_data_dum IS NULL 
                        OR tb1.gestacao_data_dpp < (CASE -- Ou DPP é menor que a data de início do quadrimestre anterior
                            WHEN date_part('month', CURRENT_DATE) >= 1 AND date_part('month', CURRENT_DATE) <= 4 THEN concat((date_part('year', CURRENT_DATE) - 1), '-09-01')::DATE
                            WHEN date_part('month', CURRENT_DATE) >= 5 AND date_part('month', CURRENT_DATE) <= 8 THEN concat(date_part('year', CURRENT_DATE), '-01-01')::DATE
                            WHEN date_part('month', CURRENT_DATE) >= 9 AND date_part('month', CURRENT_DATE) <= 12 THEN concat(date_part('year', CURRENT_DATE), '-05-01')::DATE
                         END) 
                         THEN 5 -- Retona '-' para gestantes sem DUM
                    WHEN tb1.exame_hiv_realizado_valido AND tb1.exame_sifilis_realizado_valido IS FALSE THEN 1 -- Apenas Ex. de HIV realizados
                    WHEN tb1.exame_sifilis_realizado_valido AND tb1.exame_hiv_realizado_valido IS FALSE THEN 2 -- Apenas Ex. de Sífilis realizados
                    WHEN tb1.exame_sifilis_realizado_valido IS FALSE AND tb1.exame_hiv_realizado_valido IS FALSE THEN 3 -- Nenhum exame realizado
                    WHEN tb1.exame_sifilis_realizado_valido AND tb1.exame_hiv_realizado_valido THEN 4 -- Os dois exames realizados
                    ELSE NULL::integer
                END AS id_exame_hiv_sifilis,
                CASE
                    WHEN tb1.possui_registro_aborto = 'Sim'::text THEN 10 -- Gestantes com registro de aborto
                    WHEN tb1.gestacao_data_dpp IS NULL
                        OR tb1.gestacao_data_dpp < (CASE -- Ou DPP é menor que a data de início do quadrimestre anterior
                            WHEN date_part('month', CURRENT_DATE) >= 1 AND date_part('month', CURRENT_DATE) <= 4 THEN concat((date_part('year', CURRENT_DATE) - 1), '-09-01')::DATE
                            WHEN date_part('month', CURRENT_DATE) >= 5 AND date_part('month', CURRENT_DATE) <= 8 THEN concat(date_part('year', CURRENT_DATE), '-01-01')::DATE
                            WHEN date_part('month', CURRENT_DATE) >= 9 AND date_part('month', CURRENT_DATE) <= 12 THEN concat(date_part('year', CURRENT_DATE), '-05-01')::DATE
                         END) 
                         THEN 11 -- Gestantes sem DUM 
                    WHEN tb1.gestacao_data_dpp > CURRENT_DATE THEN 8 -- Gestantes ativas
                    WHEN tb1.gestacao_data_dpp <= CURRENT_DATE THEN 9 -- Gestantes encerradas
                    ELSE NULL::integer
                END AS id_status_usuario,
                CASE
                    WHEN tb1.possui_registro_parto = 'Sim'::text THEN 1
                    WHEN tb1.possui_registro_parto = 'Não'::text THEN 2
                    ELSE 0
                END AS id_registro_parto,
                CASE
                    WHEN tb1.possui_registro_aborto = 'Sim'::text THEN 1
                    WHEN tb1.possui_registro_aborto = 'Não'::text THEN 2
                    ELSE 0
                END AS id_registro_aborto,
            COALESCE(tb1.acs_nome_cadastro,tb1.acs_nome_visita, tb1.profissional_nome_atendimento, 'SEM PROFISSIONAL RESPONSÁVEL') AS acs_nome,
            CURRENT_DATE AS atualizacao_data,
            tb1.criacao_data::date AS criacao_data
        FROM dados_transmissoes_recentes tb1
        WHERE tb1.possui_registro_aborto = 'Não'::text
        )
    , tabela_final AS (
        SELECT  
            tabela_aux.*,
            drp.dt_registro_producao_mais_recente,
            concat(tb2.nome, ' - ', tb2.uf_sigla) AS municipio_uf,
            ROW_NUMBER() OVER (PARTITION BY tabela_aux.municipio_id_sus) AS seq_demo_viscosa
        FROM tabela_aux
        LEFT JOIN data_registro_producao drp 
            ON drp.municipio_id_sus::text = tabela_aux.municipio_id_sus::text 
                AND drp.equipe_ine = tabela_aux.equipe_ine
        LEFT JOIN listas_de_codigos.municipios tb2 
            ON tabela_aux.municipio_id_sus::bpchar = tb2.id_sus
    ), dados_demo_vicosa AS (
        SELECT 
            tf.chave_id_gestacao,
            '111111' AS municipio_id_sus,
            tf.equipe_ine,
            tf.equipe_nome,
            upper(nomes.nome_ficticio) AS cidadao_nome,
            concat(impulso_previne_dados_nominais.random_between(100000000, 999999999)::text, impulso_previne_dados_nominais.random_between(10, 99)::text) AS cidadao_cpf_dt_nascimento,
            tf.gestacao_data_dpp,
            tf.gestacao_quadrimestre,
            tf.gestacao_idade_gestacional_atual,
            tf.gestacao_idade_gestacional_primeiro_atendimento,
            tf.consulta_prenatal_ultima_data,
            tf.consultas_pre_natal_validas,
            tf.id_atendimento_odontologico,
            tf.id_exame_hiv_sifilis,
            tf.id_status_usuario,
            tf.id_registro_parto,
            tf.id_registro_aborto,
            upper(nomes2.nome_ficticio) AS acs_nome,
            tf.atualizacao_data,
            tf.criacao_data,
            tf.dt_registro_producao_mais_recente,
            'Demo - Viçosa - MG' AS municipio_uf
        FROM tabela_final tf
        LEFT JOIN configuracoes.nomes_ficticios_citopatologico nomes 
            ON tf.seq_demo_viscosa = nomes.seq
        LEFT JOIN configuracoes.nomes_ficticios_hipertensos nomes2 
            ON tf.seq_demo_viscosa = nomes2.seq
        WHERE municipio_id_sus = '140015' -- BONFIM - RR
    )
    SELECT 
        ddv.chave_id_gestacao,
        ddv.municipio_id_sus,
        ddv.equipe_ine,
        ddv.equipe_nome,
        ddv.cidadao_nome,
        ddv.cidadao_cpf_dt_nascimento,
        ddv.gestacao_data_dpp,
        ddv.gestacao_quadrimestre,
        ddv.gestacao_idade_gestacional_atual,
        ddv.gestacao_idade_gestacional_primeiro_atendimento,
        ddv.consulta_prenatal_ultima_data,
        ddv.consultas_pre_natal_validas,
        ddv.id_atendimento_odontologico,
        ddv.id_exame_hiv_sifilis,
        ddv.id_status_usuario,
        ddv.id_registro_parto,
        ddv.id_registro_aborto,
        ddv.acs_nome,
        ddv.atualizacao_data,
        ddv.criacao_data,
        ddv.dt_registro_producao_mais_recente,
        ddv.municipio_uf
    FROM dados_demo_vicosa ddv 
UNION ALL 
    SELECT 
        tf.chave_id_gestacao,
        tf.municipio_id_sus,
        tf.equipe_ine,
        tf.equipe_nome,
        tf.cidadao_nome,
        tf.cidadao_cpf_dt_nascimento,
        tf.gestacao_data_dpp,
        tf.gestacao_quadrimestre,
        tf.gestacao_idade_gestacional_atual,
        tf.gestacao_idade_gestacional_primeiro_atendimento,
        tf.consulta_prenatal_ultima_data,
        tf.consultas_pre_natal_validas,
        tf.id_atendimento_odontologico,
        tf.id_exame_hiv_sifilis,
        tf.id_status_usuario,
        tf.id_registro_parto,
        tf.id_registro_aborto,
        tf.acs_nome,
        tf.atualizacao_data,
        tf.criacao_data,
        tf.dt_registro_producao_mais_recente,
        tf.municipio_uf
    FROM tabela_final tf
WITH DATA;

-- View indexes:
CREATE INDEX painel_gestantes_lista_nominal_chave_id_gestacao_idx ON impulso_previne_dados_nominais.painel_gestantes_lista_nominal USING btree (chave_id_gestacao);
