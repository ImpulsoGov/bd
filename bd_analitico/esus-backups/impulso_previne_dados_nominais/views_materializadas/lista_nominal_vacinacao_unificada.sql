-- impulso_previne_dados_nominais.lista_nominal_vacinacao_unificada source

CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.lista_nominal_vacinacao_unificada
TABLESPACE pg_default
AS WITH sumarizacao_criancas AS (
         SELECT
            DISTINCT 
            h_1.municipio_id_sus,
            h_1.chave_cidadao,
            h_1.cidadao_cpf,
            h_1.cidadao_cns,
            h_1.cidadao_nome,
            h_1.dt_nascimento,
            h_1.cidadao_nome_responsavel,
            h_1.cidadao_cns_responsavel,
            h_1.cidadao_cpf_responsavel,
            h_1.cidadao_idade_meses_atual,
            h_1.estabelecimento_cnes_atendimento,
            h_1.estabelecimento_cnes_cadastro,
            h_1.estabelecimento_nome_atendimento,
            h_1.estabelecimento_nome_cadastro,
            h_1.equipe_ine_atendimento,
            h_1.equipe_ine_cadastro,
            h_1.equipe_ine_aplicacao_vacina,
            h_1.equipe_nome_atendimento,
            h_1.equipe_nome_cadastro,
            h_1.equipe_nome_aplicacao_vacina,
            h_1.acs_nome_cadastro,
            h_1.acs_nome_visita,
            h_1.profissional_nome_atendimento,
            h_1.profissional_nome_aplicacao_vacina,
            h_1.data_ultimo_cadastro_individual,
            h_1.data_ultimo_atendimento_individual,
            h_1.data_ultima_vista_domiciliar,
            h_1.criacao_data,
                CASE
                    WHEN date_part('month'::text, date(h_1.dt_nascimento + '1 year'::interval)) >= 1::double precision AND date_part('month'::text, date(h_1.dt_nascimento + '1 year'::interval)) <= 4::double precision THEN concat(date_part('year'::text, date(h_1.dt_nascimento + '1 year'::interval)), '-01-01')::date
                    WHEN date_part('month'::text, date(h_1.dt_nascimento + '1 year'::interval)) >= 5::double precision AND date_part('month'::text, date(h_1.dt_nascimento + '1 year'::interval)) <= 8::double precision THEN concat(date_part('year'::text, date(h_1.dt_nascimento + '1 year'::interval)), '-05-01')::date
                    WHEN date_part('month'::text, date(h_1.dt_nascimento + '1 year'::interval)) >= 9::double precision AND date_part('month'::text, date(h_1.dt_nascimento + '1 year'::interval)) <= 12::double precision THEN concat(date_part('year'::text, date(h_1.dt_nascimento + '1 year'::interval)), '-09-01')::date
                    ELSE NULL::date
                END AS inicio_quadri_completa_1_ano,
                CASE
                    WHEN h_1.cidadao_idade_meses_atual < 2 THEN 'vacinacao_nao_iniciada'::text
                    WHEN h_1.cidadao_idade_meses_atual >= 2 AND h_1.cidadao_idade_meses_atual <= 12 THEN 'vacinacao_em_andamento'::text
                    WHEN h_1.cidadao_idade_meses_atual > 12 THEN 'periodo_vacinacao_encerrado'::text
                    ELSE NULL::text
                END AS status_idade
           FROM impulso_previne_dados_nominais.eventos_vacinacao h_1
        ), quantidade_vacinas_polio_registradas AS (
         SELECT st.chave_cidadao,
            count(DISTINCT st.co_seq_fat_vacinacao_vacina) AS qtde_vacinas_polio_registradas
           FROM impulso_previne_dados_nominais.eventos_vacinacao st
          WHERE st.codigo_vacina::text = '22'::text
          GROUP BY st.chave_cidadao
        ), primeira_dose_polio AS (
         WITH base AS (
                 SELECT st.chave_cidadao,
                    st.dt_nascimento,
                    st.data_registro_vacina AS data_1dose_polio,
                    st.co_seq_fat_vacinacao_vacina,
                    st.dose_vacina,
                    row_number() OVER (PARTITION BY st.chave_cidadao ORDER BY st.data_registro_vacina, st.co_seq_fat_vacinacao_vacina) AS ordem_aplicacao
                   FROM impulso_previne_dados_nominais.eventos_vacinacao st
                  WHERE st.dose_vacina::text = '1ª DOSE'::text AND st.codigo_vacina::text = '22'::text
                )
         SELECT base.chave_cidadao,
            base.dt_nascimento,
            base.data_1dose_polio,
            base.co_seq_fat_vacinacao_vacina,
            base.dose_vacina,
            base.ordem_aplicacao
           FROM base
          WHERE base.ordem_aplicacao = 1
        ), segunda_dose_polio AS (
         WITH base AS (
                 SELECT st.chave_cidadao,
                    st.dt_nascimento,
                    st.data_registro_vacina AS data_2dose_polio,
                    st.co_seq_fat_vacinacao,
                    st.dose_vacina,
                    row_number() OVER (PARTITION BY st.chave_cidadao ORDER BY st.data_registro_vacina, st.co_seq_fat_vacinacao) AS ordem_aplicacao
                   FROM impulso_previne_dados_nominais.eventos_vacinacao st
                  WHERE st.dose_vacina::text = '2ª DOSE'::text AND st.codigo_vacina::text = '22'::text
                )
         SELECT base.chave_cidadao,
            base.dt_nascimento,
            base.data_2dose_polio,
            base.co_seq_fat_vacinacao,
            base.dose_vacina,
            base.ordem_aplicacao
           FROM base
          WHERE base.ordem_aplicacao = 1
        ), terceira_dose_polio AS (
         WITH base AS (
                 SELECT st.chave_cidadao,
                    st.dt_nascimento,
                    st.data_registro_vacina AS data_3dose_polio,
                    st.co_seq_fat_vacinacao,
                    st.dose_vacina,
                    row_number() OVER (PARTITION BY st.chave_cidadao ORDER BY st.data_registro_vacina, st.co_seq_fat_vacinacao) AS ordem_aplicacao
                   FROM impulso_previne_dados_nominais.eventos_vacinacao st
                  WHERE st.dose_vacina::text = '3ª DOSE'::text AND st.codigo_vacina::text = '22'::text
                )
         SELECT base.chave_cidadao,
            base.dt_nascimento,
            base.data_3dose_polio,
            base.co_seq_fat_vacinacao,
            base.dose_vacina,
            base.ordem_aplicacao
           FROM base
          WHERE base.ordem_aplicacao = 1
        ), sumarizacao_polio AS (
         SELECT h_1.chave_cidadao,
            h_1.dt_nascimento,
            polio1.data_1dose_polio,
            polio2.data_2dose_polio,
            polio3.data_3dose_polio,
            q.qtde_vacinas_polio_registradas,
                CASE
                    WHEN polio1.data_1dose_polio IS NOT NULL THEN 1
                    ELSE 0
                END +
                CASE
                    WHEN polio2.data_2dose_polio IS NOT NULL THEN 1
                    ELSE 0
                END +
                CASE
                    WHEN polio3.data_3dose_polio IS NOT NULL THEN 1
                    ELSE 0
                END AS quantidade_polio_validas,
            date_part('month'::text, age(polio1.data_1dose_polio::timestamp with time zone, h_1.dt_nascimento::timestamp with time zone)) AS idade_meses_1dose_polio,
            date_part('month'::text, age(polio2.data_2dose_polio::timestamp with time zone, h_1.dt_nascimento::timestamp with time zone)) AS idade_meses_2dose_polio,
            date_part('month'::text, age(polio3.data_3dose_polio::timestamp with time zone, h_1.dt_nascimento::timestamp with time zone)) AS idade_meses_3dose_polio,
            date(h_1.dt_nascimento + '2 mons'::interval) AS prazo_1dose_polio,
            date(h_1.dt_nascimento + '8 mons'::interval) AS prazo_limite_1dose_polio,
                CASE
                    WHEN polio1.data_1dose_polio IS NULL THEN date(h_1.dt_nascimento + '4 mons'::interval)
                    ELSE date(polio1.data_1dose_polio + '2 mons'::interval)
                END AS prazo_2dose_polio,
                CASE
                    WHEN polio1.data_1dose_polio IS NOT NULL AND polio2.data_2dose_polio IS NULL THEN date(polio1.data_1dose_polio + '4 mons'::interval)
                    WHEN polio2.data_2dose_polio IS NOT NULL THEN date(polio2.data_2dose_polio + '2 mons'::interval)
                    ELSE date(h_1.dt_nascimento + '6 mons'::interval)
                END AS prazo_3dose_polio
           FROM impulso_previne_dados_nominais.eventos_vacinacao h_1
             LEFT JOIN primeira_dose_polio polio1 ON h_1.chave_cidadao::text = polio1.chave_cidadao::text
             LEFT JOIN segunda_dose_polio polio2 ON h_1.chave_cidadao::text = polio2.chave_cidadao::text
             LEFT JOIN terceira_dose_polio polio3 ON h_1.chave_cidadao::text = polio3.chave_cidadao::text
             LEFT JOIN quantidade_vacinas_polio_registradas q ON q.chave_cidadao::text = h_1.chave_cidadao::text
          GROUP BY h_1.chave_cidadao, h_1.dt_nascimento, polio1.data_1dose_polio, polio2.data_2dose_polio, polio3.data_3dose_polio, q.qtde_vacinas_polio_registradas, (
                CASE
                    WHEN polio1.data_1dose_polio IS NOT NULL THEN 1
                    ELSE 0
                END +
                CASE
                    WHEN polio2.data_2dose_polio IS NOT NULL THEN 1
                    ELSE 0
                END +
                CASE
                    WHEN polio3.data_3dose_polio IS NOT NULL THEN 1
                    ELSE 0
                END), (date_part('month'::text, age(polio1.data_1dose_polio::timestamp with time zone, h_1.dt_nascimento::timestamp with time zone))), (date_part('month'::text, age(polio2.data_2dose_polio::timestamp with time zone, h_1.dt_nascimento::timestamp with time zone))), (date_part('month'::text, age(polio3.data_3dose_polio::timestamp with time zone, h_1.dt_nascimento::timestamp with time zone)))
        ), quantidade_vacinas_penta_registradas AS (
         SELECT st.chave_cidadao,
            count(DISTINCT st.co_seq_fat_vacinacao_vacina) AS qtde_vacinas_penta_registradas
           FROM impulso_previne_dados_nominais.eventos_vacinacao st
          WHERE st.codigo_vacina::text = '42'::text
          GROUP BY st.chave_cidadao
        ), primeira_dose_penta AS (
         WITH base AS (
                 SELECT st.chave_cidadao,
                    st.dt_nascimento,
                    st.data_registro_vacina AS data_1dose_penta,
                    st.co_seq_fat_vacinacao_vacina,
                    st.dose_vacina,
                    row_number() OVER (PARTITION BY st.chave_cidadao ORDER BY st.data_registro_vacina, st.co_seq_fat_vacinacao_vacina) AS ordem_aplicacao
                   FROM impulso_previne_dados_nominais.eventos_vacinacao st
                  WHERE st.dose_vacina::text = '1ª DOSE'::text AND st.codigo_vacina::text = '42'::text
                )
         SELECT base.chave_cidadao,
            base.dt_nascimento,
            base.data_1dose_penta,
            base.co_seq_fat_vacinacao_vacina,
            base.dose_vacina,
            base.ordem_aplicacao
           FROM base
          WHERE base.ordem_aplicacao = 1
        ), segunda_dose_penta AS (
         WITH base AS (
                 SELECT st.chave_cidadao,
                    st.dt_nascimento,
                    st.data_registro_vacina AS data_2dose_penta,
                    st.co_seq_fat_vacinacao,
                    st.dose_vacina,
                    row_number() OVER (PARTITION BY st.chave_cidadao ORDER BY st.data_registro_vacina, st.co_seq_fat_vacinacao) AS ordem_aplicacao
                   FROM impulso_previne_dados_nominais.eventos_vacinacao st
                  WHERE st.dose_vacina::text = '2ª DOSE'::text AND st.codigo_vacina::text = '42'::text
                )
         SELECT base.chave_cidadao,
            base.dt_nascimento,
            base.data_2dose_penta,
            base.co_seq_fat_vacinacao,
            base.dose_vacina,
            base.ordem_aplicacao
           FROM base
          WHERE base.ordem_aplicacao = 1
        ), terceira_dose_penta AS (
         WITH base AS (
                 SELECT st.chave_cidadao,
                    st.dt_nascimento,
                    st.data_registro_vacina AS data_3dose_penta,
                    st.co_seq_fat_vacinacao,
                    st.dose_vacina,
                    row_number() OVER (PARTITION BY st.chave_cidadao ORDER BY st.data_registro_vacina, st.co_seq_fat_vacinacao) AS ordem_aplicacao
                   FROM impulso_previne_dados_nominais.eventos_vacinacao st
                  WHERE st.dose_vacina::text = '3ª DOSE'::text AND st.codigo_vacina::text = '42'::text
                )
         SELECT base.chave_cidadao,
            base.dt_nascimento,
            base.data_3dose_penta,
            base.co_seq_fat_vacinacao,
            base.dose_vacina,
            base.ordem_aplicacao
           FROM base
          WHERE base.ordem_aplicacao = 1
        ), sumarizacao_penta AS (
         SELECT h_1.chave_cidadao,
            h_1.dt_nascimento,
            penta1.data_1dose_penta,
            penta2.data_2dose_penta,
            penta3.data_3dose_penta,
            q.qtde_vacinas_penta_registradas,
                CASE
                    WHEN penta1.data_1dose_penta IS NOT NULL THEN 1
                    ELSE 0
                END +
                CASE
                    WHEN penta2.data_2dose_penta IS NOT NULL THEN 1
                    ELSE 0
                END +
                CASE
                    WHEN penta3.data_3dose_penta IS NOT NULL THEN 1
                    ELSE 0
                END AS quantidade_penta_validas,
            date_part('month'::text, age(penta1.data_1dose_penta::timestamp with time zone, h_1.dt_nascimento::timestamp with time zone)) AS idade_meses_1dose_penta,
            date_part('month'::text, age(penta2.data_2dose_penta::timestamp with time zone, h_1.dt_nascimento::timestamp with time zone)) AS idade_meses_2dose_penta,
            date_part('month'::text, age(penta3.data_3dose_penta::timestamp with time zone, h_1.dt_nascimento::timestamp with time zone)) AS idade_meses_3dose_penta,
            date(h_1.dt_nascimento + '2 mons'::interval) AS prazo_1dose_penta,
            date(h_1.dt_nascimento + '8 mons'::interval) AS prazo_limite_1dose_penta,
                CASE
                    WHEN penta1.data_1dose_penta IS NULL THEN date(h_1.dt_nascimento + '4 mons'::interval)
                    ELSE date(penta1.data_1dose_penta + '2 mons'::interval)
                END AS prazo_2dose_penta,
                CASE
                    WHEN penta1.data_1dose_penta IS NOT NULL AND penta2.data_2dose_penta IS NULL THEN date(penta1.data_1dose_penta + '4 mons'::interval)
                    WHEN penta2.data_2dose_penta IS NOT NULL THEN date(penta2.data_2dose_penta + '2 mons'::interval)
                    ELSE date(h_1.dt_nascimento + '6 mons'::interval)
                END AS prazo_3dose_penta
           FROM impulso_previne_dados_nominais.eventos_vacinacao h_1
             LEFT JOIN primeira_dose_penta penta1 ON h_1.chave_cidadao::text = penta1.chave_cidadao::text
             LEFT JOIN segunda_dose_penta penta2 ON h_1.chave_cidadao::text = penta2.chave_cidadao::text
             LEFT JOIN terceira_dose_penta penta3 ON h_1.chave_cidadao::text = penta3.chave_cidadao::text
             LEFT JOIN quantidade_vacinas_penta_registradas q ON q.chave_cidadao::text = h_1.chave_cidadao::text
          GROUP BY h_1.chave_cidadao, h_1.dt_nascimento, penta1.data_1dose_penta, penta2.data_2dose_penta, penta3.data_3dose_penta, q.qtde_vacinas_penta_registradas, (
                CASE
                    WHEN penta1.data_1dose_penta IS NOT NULL THEN 1
                    ELSE 0
                END +
                CASE
                    WHEN penta2.data_2dose_penta IS NOT NULL THEN 1
                    ELSE 0
                END +
                CASE
                    WHEN penta3.data_3dose_penta IS NOT NULL THEN 1
                    ELSE 0
                END), (date_part('month'::text, age(penta1.data_1dose_penta::timestamp with time zone, h_1.dt_nascimento::timestamp with time zone))), (date_part('month'::text, age(penta2.data_2dose_penta::timestamp with time zone, h_1.dt_nascimento::timestamp with time zone))), (date_part('month'::text, age(penta3.data_3dose_penta::timestamp with time zone, h_1.dt_nascimento::timestamp with time zone)))
        )
 SELECT
        CASE
            WHEN date_part('month'::text, CURRENT_DATE) >= 1::double precision AND date_part('month'::text, CURRENT_DATE) <= 4::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '.Q1')
            WHEN date_part('month'::text, CURRENT_DATE) >= 5::double precision AND date_part('month'::text, CURRENT_DATE) <= 8::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '.Q2')
            WHEN date_part('month'::text, CURRENT_DATE) >= 9::double precision AND date_part('month'::text, CURRENT_DATE) <= 12::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '.Q3')
            ELSE NULL::text
        END AS quadrimestre_atual,
    h.municipio_id_sus,
    h.chave_cidadao,
    h.cidadao_nome,
    h.cidadao_cpf,
    h.cidadao_cns,
    h.dt_nascimento,
    h.cidadao_idade_meses_atual,
    h.status_idade,
    p.codigo AS quadrimestre_completa_1_ano,
    polio.data_1dose_polio,
    polio.data_2dose_polio,
    polio.data_3dose_polio,
    polio.qtde_vacinas_polio_registradas,
    polio.quantidade_polio_validas,
    polio.idade_meses_1dose_polio,
    polio.idade_meses_2dose_polio,
    polio.idade_meses_3dose_polio,
    polio.prazo_1dose_polio,
    polio.prazo_limite_1dose_polio,
    polio.prazo_2dose_polio,
    polio.prazo_3dose_polio,
    penta.data_1dose_penta,
    penta.data_2dose_penta,
    penta.data_3dose_penta,
    penta.qtde_vacinas_penta_registradas,
    penta.quantidade_penta_validas,
    penta.idade_meses_1dose_penta,
    penta.idade_meses_2dose_penta,
    penta.idade_meses_3dose_penta,
    penta.prazo_1dose_penta,
    penta.prazo_limite_1dose_penta,
    penta.prazo_2dose_penta,
    penta.prazo_3dose_penta,
    h.cidadao_nome_responsavel,
    h.cidadao_cns_responsavel,
    h.cidadao_cpf_responsavel,
    h.estabelecimento_cnes_atendimento,
    h.estabelecimento_cnes_cadastro,
    h.estabelecimento_nome_atendimento,
    h.estabelecimento_nome_cadastro,
    h.equipe_ine_atendimento,
    h.equipe_ine_cadastro,
    h.equipe_ine_aplicacao_vacina,
    h.equipe_nome_atendimento,
    h.equipe_nome_cadastro,
    h.equipe_nome_aplicacao_vacina,
    h.acs_nome_cadastro,
    h.acs_nome_visita,
    h.profissional_nome_atendimento,
    h.profissional_nome_aplicacao_vacina,
    h.data_ultimo_cadastro_individual,
    h.data_ultimo_atendimento_individual,
    h.data_ultima_vista_domiciliar,
    h.criacao_data,
    now() AS atualizacao_data
   FROM sumarizacao_criancas h
     LEFT JOIN sumarizacao_polio polio ON polio.chave_cidadao::text = h.chave_cidadao::text
     LEFT JOIN sumarizacao_penta penta ON penta.chave_cidadao::text = h.chave_cidadao::text
     LEFT JOIN listas_de_codigos.periodos p ON p.data_inicio = h.inicio_quadri_completa_1_ano AND p.tipo::text = 'Quadrimestral'::text
WITH DATA;