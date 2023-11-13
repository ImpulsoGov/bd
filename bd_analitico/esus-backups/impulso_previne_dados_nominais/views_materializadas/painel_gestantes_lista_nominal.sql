CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.painel_gestantes_lista_nominal
TABLESPACE pg_default
AS WITH dados_anonimizados_demo_vicosa AS (
         SELECT res.chave_gestacao,
            res.estabelecimento_cnes,
            res.estabelecimento_nome,
            res.equipe_ine,
            res.equipe_nome,
            upper(nomes2.nome_ficticio) AS acs_nome,
            res.acs_data_ultima_visita,
            res.gestante_documento_cpf,
            res.gestante_documento_cns,
            upper(nomes.nome_ficticio) AS gestante_nome,
            res.gestante_data_de_nascimento,
            res.gestante_telefone,
            res.gestacao_data_dum,
            res.gestacao_idade_gestacional_atual,
            res.gestacao_idade_gestacional_primeiro_atendimento,
            res.gestacao_data_dpp,
            res.gestacao_data_dpp AS gestacao_consulta_prenatal_data_limite,
            res.gestacao_dpp_dias_para,
            res.consultas_pre_natal_validas,
            res.consulta_prenatal_ultima_data,
            res.consulta_prenatal_ultima_dias_desde,
            res.atendimento_odontologico_realizado_valido,
            res.exame_hiv_realizado_valido,
            res.exame_sifilis_realizado_valido,
            res.exame_sifilis_hiv_realizado_valido,
            res.possui_registro_aborto,
            res.possui_registro_parto,
            res.criacao_data,
            '100111'::character varying AS municipio_id_sus
           FROM ( SELECT row_number() OVER (PARTITION BY 0::integer) AS seq,
                    lista_nominal_gestantes.chave_gestacao,
                    lista_nominal_gestantes.gestante_telefone,
                    lista_nominal_gestantes.gestante_nome,
                    lista_nominal_gestantes.gestante_data_de_nascimento,
                    lista_nominal_gestantes.estabelecimento_cnes,
                    lista_nominal_gestantes.estabelecimento_nome,
                    lista_nominal_gestantes.equipe_ine,
                    lista_nominal_gestantes.equipe_nome,
                    lista_nominal_gestantes.acs_nome,
                    lista_nominal_gestantes.acs_data_ultima_visita,
                    lista_nominal_gestantes.gestacao_data_dum,
                    lista_nominal_gestantes.gestacao_data_dpp,
                    lista_nominal_gestantes.gestacao_dpp_dias_para,
                    lista_nominal_gestantes.gestacao_quadrimestre,
                    lista_nominal_gestantes.gestacao_idade_gestacional_primeiro_atendimento,
                    lista_nominal_gestantes.consulta_prenatal_primeira_data,
                    lista_nominal_gestantes.consulta_prenatal_ultima_data,
                    lista_nominal_gestantes.consulta_prenatal_ultima_dias_desde,
                    concat(impulso_previne_dados_nominais.random_between(100000000, 999999999)::text, impulso_previne_dados_nominais.random_between(10, 99)::text) AS gestante_documento_cpf,
                    concat('7', impulso_previne_dados_nominais.random_between(100000000, 999999999)::text, impulso_previne_dados_nominais.random_between(10000, 99999)::text) AS gestante_documento_cns,
                    lista_nominal_gestantes.gestacao_idade_gestacional_atual,
                    lista_nominal_gestantes.consultas_pre_natal_validas,
                    lista_nominal_gestantes.atendimento_odontologico_realizado_valido,
                    lista_nominal_gestantes.exame_hiv_realizado_valido,
                    lista_nominal_gestantes.exame_sifilis_realizado_valido,
                    lista_nominal_gestantes.possui_registro_aborto,
                    lista_nominal_gestantes.possui_registro_parto,
                    lista_nominal_gestantes.exame_sifilis_hiv_realizado_valido,
                    lista_nominal_gestantes.criacao_data
                   FROM impulso_previne_dados_nominais.lista_nominal_gestantes_unificada lista_nominal_gestantes
                  WHERE lista_nominal_gestantes.municipio_id_sus::text = '317130'::text
                  and lista_nominal_gestantes.equipe_ine is not null) res
             JOIN configuracoes.nomes_ficticios_gestantes nomes ON res.seq = nomes.seq
             JOIN configuracoes.nomes_ficticios_diabeticos nomes2 ON res.seq = nomes2.seq
        ), dados_anonimizados_impulsolandia AS (
         SELECT res.chave_gestacao,
            res.estabelecimento_cnes,
            res.estabelecimento_nome,
            res.equipe_ine,
            res.equipe_nome,
            upper(nomes2.nome_ficticio) AS acs_nome,
            res.acs_data_ultima_visita,
            res.gestante_documento_cpf,
            res.gestante_documento_cns,
            upper(nomes.nome_ficticio) AS gestante_nome,
            res.gestante_data_de_nascimento,
            res.gestante_telefone,
            res.gestacao_data_dum,
            res.gestacao_idade_gestacional_atual,
            res.gestacao_idade_gestacional_primeiro_atendimento,
            res.gestacao_data_dpp,
            res.gestacao_data_dpp AS gestacao_consulta_prenatal_data_limite,
            res.gestacao_dpp_dias_para,
            res.consultas_pre_natal_validas,
            res.consulta_prenatal_ultima_data,
            res.consulta_prenatal_ultima_dias_desde,
            res.atendimento_odontologico_realizado_valido,
            res.exame_hiv_realizado_valido,
            res.exame_sifilis_realizado_valido,
            res.exame_sifilis_hiv_realizado_valido,
            res.possui_registro_aborto,
            res.possui_registro_parto,
            res.criacao_data,
            '111111'::character varying AS municipio_id_sus
           FROM ( SELECT row_number() OVER (PARTITION BY 0::integer) AS seq,
                    lista_nominal_gestantes.chave_gestacao,
                    lista_nominal_gestantes.gestante_telefone,
                    lista_nominal_gestantes.gestante_nome,
                    lista_nominal_gestantes.gestante_data_de_nascimento,
                    lista_nominal_gestantes.estabelecimento_cnes,
                    lista_nominal_gestantes.estabelecimento_nome,
                    lista_nominal_gestantes.equipe_ine,
                    lista_nominal_gestantes.equipe_nome,
                    lista_nominal_gestantes.acs_nome,
                    lista_nominal_gestantes.acs_data_ultima_visita,
                    lista_nominal_gestantes.gestacao_data_dum,
                    lista_nominal_gestantes.gestacao_data_dpp,
                    lista_nominal_gestantes.gestacao_dpp_dias_para,
                    lista_nominal_gestantes.gestacao_quadrimestre,
                    lista_nominal_gestantes.gestacao_idade_gestacional_primeiro_atendimento,
                    lista_nominal_gestantes.consulta_prenatal_primeira_data,
                    lista_nominal_gestantes.consulta_prenatal_ultima_data,
                    lista_nominal_gestantes.consulta_prenatal_ultima_dias_desde,
                    concat(impulso_previne_dados_nominais.random_between(100000000, 999999999)::text, impulso_previne_dados_nominais.random_between(10, 99)::text) AS gestante_documento_cpf,
                    concat('7', impulso_previne_dados_nominais.random_between(100000000, 999999999)::text, impulso_previne_dados_nominais.random_between(10000, 99999)::text) AS gestante_documento_cns,
                    lista_nominal_gestantes.gestacao_idade_gestacional_atual,
                    lista_nominal_gestantes.consultas_pre_natal_validas,
                    lista_nominal_gestantes.atendimento_odontologico_realizado_valido,
                    lista_nominal_gestantes.exame_hiv_realizado_valido,
                    lista_nominal_gestantes.exame_sifilis_realizado_valido,
                    lista_nominal_gestantes.possui_registro_aborto,
                    lista_nominal_gestantes.possui_registro_parto,
                    lista_nominal_gestantes.exame_sifilis_hiv_realizado_valido,
                    lista_nominal_gestantes.criacao_data
                   FROM impulso_previne_dados_nominais.lista_nominal_gestantes_unificada lista_nominal_gestantes
                  WHERE lista_nominal_gestantes.municipio_id_sus::text = '317130'::text
                  and lista_nominal_gestantes.equipe_ine is not null) res
             JOIN configuracoes.nomes_ficticios_gestantes nomes ON res.seq = nomes.seq
             JOIN configuracoes.nomes_ficticios_diabeticos nomes2 ON res.seq = nomes2.seq
        ), dados_transmissoes_recentes AS (
         SELECT tb1_1.chave_gestacao,
            tb1_1.estabelecimento_cnes,
            tb1_1.estabelecimento_nome,
            tb1_1.equipe_ine,
            tb1_1.equipe_nome,
            tb1_1.acs_nome,
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
        ), une_as_bases AS (
         SELECT dados_anonimizados_demo_vicosa.chave_gestacao,
            dados_anonimizados_demo_vicosa.estabelecimento_cnes,
            dados_anonimizados_demo_vicosa.estabelecimento_nome,
            dados_anonimizados_demo_vicosa.equipe_ine,
            dados_anonimizados_demo_vicosa.equipe_nome,
            dados_anonimizados_demo_vicosa.acs_nome,
            dados_anonimizados_demo_vicosa.acs_data_ultima_visita,
            dados_anonimizados_demo_vicosa.gestante_documento_cpf,
            dados_anonimizados_demo_vicosa.gestante_documento_cns,
            dados_anonimizados_demo_vicosa.gestante_nome,
            dados_anonimizados_demo_vicosa.gestante_data_de_nascimento,
            dados_anonimizados_demo_vicosa.gestante_telefone,
            dados_anonimizados_demo_vicosa.gestacao_data_dum,
            dados_anonimizados_demo_vicosa.gestacao_idade_gestacional_atual,
            dados_anonimizados_demo_vicosa.gestacao_idade_gestacional_primeiro_atendimento,
            dados_anonimizados_demo_vicosa.gestacao_data_dpp,
            dados_anonimizados_demo_vicosa.gestacao_consulta_prenatal_data_limite,
            dados_anonimizados_demo_vicosa.gestacao_dpp_dias_para,
            dados_anonimizados_demo_vicosa.consultas_pre_natal_validas,
            dados_anonimizados_demo_vicosa.consulta_prenatal_ultima_data,
            dados_anonimizados_demo_vicosa.consulta_prenatal_ultima_dias_desde,
            dados_anonimizados_demo_vicosa.atendimento_odontologico_realizado_valido,
            dados_anonimizados_demo_vicosa.exame_hiv_realizado_valido,
            dados_anonimizados_demo_vicosa.exame_sifilis_realizado_valido,
            dados_anonimizados_demo_vicosa.exame_sifilis_hiv_realizado_valido,
            dados_anonimizados_demo_vicosa.possui_registro_aborto,
            dados_anonimizados_demo_vicosa.possui_registro_parto,
            dados_anonimizados_demo_vicosa.criacao_data,
            dados_anonimizados_demo_vicosa.municipio_id_sus
           FROM dados_anonimizados_demo_vicosa
        UNION ALL
         SELECT dados_anonimizados_impulsolandia.chave_gestacao,
            dados_anonimizados_impulsolandia.estabelecimento_cnes,
            dados_anonimizados_impulsolandia.estabelecimento_nome,
            dados_anonimizados_impulsolandia.equipe_ine,
            dados_anonimizados_impulsolandia.equipe_nome,
            dados_anonimizados_impulsolandia.acs_nome,
            dados_anonimizados_impulsolandia.acs_data_ultima_visita,
            dados_anonimizados_impulsolandia.gestante_documento_cpf,
            dados_anonimizados_impulsolandia.gestante_documento_cns,
            dados_anonimizados_impulsolandia.gestante_nome,
            dados_anonimizados_impulsolandia.gestante_data_de_nascimento,
            dados_anonimizados_impulsolandia.gestante_telefone,
            dados_anonimizados_impulsolandia.gestacao_data_dum,
            dados_anonimizados_impulsolandia.gestacao_idade_gestacional_atual,
            dados_anonimizados_impulsolandia.gestacao_idade_gestacional_primeiro_atendimento,
            dados_anonimizados_impulsolandia.gestacao_data_dpp,
            dados_anonimizados_impulsolandia.gestacao_consulta_prenatal_data_limite,
            dados_anonimizados_impulsolandia.gestacao_dpp_dias_para,
            dados_anonimizados_impulsolandia.consultas_pre_natal_validas,
            dados_anonimizados_impulsolandia.consulta_prenatal_ultima_data,
            dados_anonimizados_impulsolandia.consulta_prenatal_ultima_dias_desde,
            dados_anonimizados_impulsolandia.atendimento_odontologico_realizado_valido,
            dados_anonimizados_impulsolandia.exame_hiv_realizado_valido,
            dados_anonimizados_impulsolandia.exame_sifilis_realizado_valido,
            dados_anonimizados_impulsolandia.exame_sifilis_hiv_realizado_valido,
            dados_anonimizados_impulsolandia.possui_registro_aborto,
            dados_anonimizados_impulsolandia.possui_registro_parto,
            dados_anonimizados_impulsolandia.criacao_data,
            dados_anonimizados_impulsolandia.municipio_id_sus
           FROM dados_anonimizados_impulsolandia
        UNION ALL
         SELECT dados_transmissoes_recentes.chave_gestacao,
            dados_transmissoes_recentes.estabelecimento_cnes,
            dados_transmissoes_recentes.estabelecimento_nome,
            dados_transmissoes_recentes.equipe_ine,
            dados_transmissoes_recentes.equipe_nome,
            dados_transmissoes_recentes.acs_nome,
            dados_transmissoes_recentes.acs_data_ultima_visita,
            dados_transmissoes_recentes.gestante_documento_cpf,
            dados_transmissoes_recentes.gestante_documento_cns,
            dados_transmissoes_recentes.gestante_nome,
            dados_transmissoes_recentes.gestante_data_de_nascimento,
            dados_transmissoes_recentes.gestante_telefone,
            dados_transmissoes_recentes.gestacao_data_dum,
            dados_transmissoes_recentes.gestacao_idade_gestacional_atual,
            dados_transmissoes_recentes.gestacao_idade_gestacional_primeiro_atendimento,
            dados_transmissoes_recentes.gestacao_data_dpp,
            dados_transmissoes_recentes.gestacao_consulta_prenatal_data_limite,
            dados_transmissoes_recentes.gestacao_dpp_dias_para,
            dados_transmissoes_recentes.consultas_pre_natal_validas,
            dados_transmissoes_recentes.consulta_prenatal_ultima_data,
            dados_transmissoes_recentes.consulta_prenatal_ultima_dias_desde,
            dados_transmissoes_recentes.atendimento_odontologico_realizado_valido,
            dados_transmissoes_recentes.exame_hiv_realizado_valido,
            dados_transmissoes_recentes.exame_sifilis_realizado_valido,
            dados_transmissoes_recentes.exame_sifilis_hiv_realizado_valido,
            dados_transmissoes_recentes.possui_registro_aborto,
            dados_transmissoes_recentes.possui_registro_parto,
            dados_transmissoes_recentes.criacao_data,
            dados_transmissoes_recentes.municipio_id_sus
           FROM dados_transmissoes_recentes
        ), data_registro_producao AS (
         SELECT une_as_bases.municipio_id_sus,
            impulso_previne_dados_nominais.equipe_ine(une_as_bases.municipio_id_sus::text, une_as_bases.equipe_ine) AS equipe_ine,
            max(GREATEST(une_as_bases.consulta_prenatal_ultima_data)) AS dt_registro_producao_mais_recente,
            min(LEAST(une_as_bases.consulta_prenatal_ultima_data)) AS dt_registro_producao_mais_antigo
           FROM une_as_bases
          GROUP BY une_as_bases.municipio_id_sus, (impulso_previne_dados_nominais.equipe_ine(une_as_bases.municipio_id_sus::text, une_as_bases.equipe_ine))
        ), tabela_aux AS (
         SELECT tb1.chave_gestacao AS chave_id_gestacao,
            tb1.municipio_id_sus,
            impulso_previne_dados_nominais.equipe_ine(tb1.municipio_id_sus::text, tb1.equipe_ine) AS equipe_ine,
            impulso_previne_dados_nominais.equipe_ine(tb1.municipio_id_sus::text, tb1.equipe_nome) AS equipe_nome,
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
            tb1.acs_nome,
            CURRENT_DATE AS atualizacao_data,
            tb1.criacao_data::date AS criacao_data
           FROM une_as_bases tb1
          WHERE tb1.possui_registro_aborto = 'Não'::text
        )
 SELECT tabela_aux.chave_id_gestacao,
    tabela_aux.municipio_id_sus,
    tabela_aux.equipe_ine,
    tabela_aux.equipe_nome,
    tabela_aux.cidadao_nome,
    tabela_aux.cidadao_cpf_dt_nascimento,
    tabela_aux.gestacao_data_dpp,
    tabela_aux.gestacao_quadrimestre,
    tabela_aux.gestacao_idade_gestacional_atual,
    tabela_aux.gestacao_idade_gestacional_primeiro_atendimento,
    tabela_aux.consulta_prenatal_ultima_data,
    tabela_aux.consultas_pre_natal_validas,
    tabela_aux.id_atendimento_odontologico,
    tabela_aux.id_exame_hiv_sifilis,
    tabela_aux.id_status_usuario,
    tabela_aux.id_registro_parto,
    tabela_aux.id_registro_aborto,
    tabela_aux.acs_nome,
    tabela_aux.atualizacao_data,
    tabela_aux.criacao_data,
    concat(tb2.nome, ' - ', tb2.uf_sigla) AS municipio_uf,
    drp.dt_registro_producao_mais_recente
   FROM tabela_aux
     LEFT JOIN data_registro_producao drp ON drp.municipio_id_sus::text = tabela_aux.municipio_id_sus::text AND drp.equipe_ine = tabela_aux.equipe_ine
     LEFT JOIN listas_de_codigos.municipios tb2 ON tabela_aux.municipio_id_sus::bpchar = tb2.id_sus
WITH DATA;

-- View indexes:
CREATE INDEX painel_gestantes_lista_nominal_chave_id_gestacao_idx ON impulso_previne_dados_nominais.painel_gestantes_lista_nominal USING btree (chave_id_gestacao);
