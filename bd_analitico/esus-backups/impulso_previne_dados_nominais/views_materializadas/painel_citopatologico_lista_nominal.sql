
CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.painel_citopatologico_lista_nominal
TABLESPACE pg_default
AS WITH dados_anonimizados_demo_vicosa AS (
         SELECT '100111'::character varying AS municipio_id_sus,
            res.quadrimestre_atual,
            upper(nomes.nome_ficticio) AS paciente_nome,
            res.cidadao_cpf,
            res.cidadao_cns,
            res.paciente_idade_atual,
            res.dt_nascimento,
            res.dt_ultimo_exame,
            res.realizou_exame_ultimos_36_meses,
            res.data_projetada_proximo_exame,
            res.status_exame,
            res.data_limite_a_realizar_proximo_exame,
            res.cnes_estabelecimento_exame,
            res.nome_estabelecimento_exame,
            res.ine_equipe_exame,
            res.nome_equipe_exame,
            res.nome_profissional_exame,
            res.dt_ultimo_cadastro,
            res.estabelecimento_nome_cadastro,
            res.estabelecimento_cnes_cadastro,
            res.equipe_ine_cadastro,
            res.equipe_nome_cadastro,
            upper(nomes2.nome_ficticio) AS acs_nome_cadastro,
            res.dt_ultimo_atendimento,
            res.estabelecimento_nome_ultimo_atendimento,
            res.estabelecimento_cnes_ultimo_atendimento,
            res.equipe_ine_ultimo_atendimento,
            res.equipe_nome_ultimo_atendimento,
            res.acs_nome_ultimo_atendimento,
            upper(nomes2.nome_ficticio) AS acs_nome_visita,
            res.criacao_data
           FROM ( SELECT tb1_1.municipio_id_sus,
                    tb1_1.quadrimestre_atual,
                    tb1_1.paciente_nome,
                    row_number() OVER (PARTITION BY 0::integer) AS seq,
                    concat(impulso_previne_dados_nominais.random_between(100000000, 999999999)::text, impulso_previne_dados_nominais.random_between(10, 99)::text) AS cidadao_cpf,
                    concat('7', impulso_previne_dados_nominais.random_between(100000000, 999999999)::text, impulso_previne_dados_nominais.random_between(10000, 99999)::text) AS cidadao_cns,
                    tb1_1.paciente_idade_atual,
                    tb1_1.dt_nascimento,
                    tb1_1.dt_ultimo_exame,
                    tb1_1.realizou_exame_ultimos_36_meses,
                    tb1_1.data_projetada_proximo_exame,
                    tb1_1.status_exame,
                    tb1_1.data_limite_a_realizar_proximo_exame,
                    tb1_1.cnes_estabelecimento_exame,
                    tb1_1.nome_estabelecimento_exame,
                    tb1_1.ine_equipe_exame,
                    tb1_1.nome_equipe_exame,
                    tb1_1.nome_profissional_exame,
                    tb1_1.dt_ultimo_cadastro,
                    tb1_1.estabelecimento_nome_cadastro,
                    tb1_1.estabelecimento_cnes_cadastro,
                    tb1_1.equipe_ine_cadastro,
                    tb1_1.equipe_nome_cadastro,
                    tb1_1.acs_nome_cadastro,
                    tb1_1.dt_ultimo_atendimento,
                    tb1_1.estabelecimento_nome_ultimo_atendimento,
                    tb1_1.estabelecimento_cnes_ultimo_atendimento,
                    tb1_1.equipe_ine_ultimo_atendimento,
                    tb1_1.equipe_nome_ultimo_atendimento,
                    tb1_1.acs_nome_ultimo_atendimento,
                    tb1_1.acs_nome_visita,
                    tb1_1.criacao_data
                   FROM dados_nominais_mg_vicosa.lista_nominal_citopatologico tb1_1) res
             JOIN configuracoes.nomes_ficticios_citopatologico nomes ON res.seq = nomes.seq
             JOIN configuracoes.nomes_ficticios_gestantes nomes2 ON res.seq = nomes2.seq
        ), dados_anonimizados_impulsolandia AS (
         SELECT '111111'::character varying AS municipio_id_sus,
            res.quadrimestre_atual,
            upper(nomes.nome_ficticio) AS paciente_nome,
            res.cidadao_cpf,
            res.cidadao_cns,
            res.paciente_idade_atual,
            res.dt_nascimento,
            res.dt_ultimo_exame,
            res.realizou_exame_ultimos_36_meses,
            res.data_projetada_proximo_exame,
            res.status_exame,
            res.data_limite_a_realizar_proximo_exame,
            res.cnes_estabelecimento_exame,
            res.nome_estabelecimento_exame,
            res.ine_equipe_exame,
            res.nome_equipe_exame,
            res.nome_profissional_exame,
            res.dt_ultimo_cadastro,
            res.estabelecimento_nome_cadastro,
            res.estabelecimento_cnes_cadastro,
            res.equipe_ine_cadastro,
            res.equipe_nome_cadastro,
            upper(nomes2.nome_ficticio) AS acs_nome_cadastro,
            res.dt_ultimo_atendimento,
            res.estabelecimento_nome_ultimo_atendimento,
            res.estabelecimento_cnes_ultimo_atendimento,
            res.equipe_ine_ultimo_atendimento,
            res.equipe_nome_ultimo_atendimento,
            res.acs_nome_ultimo_atendimento,
            upper(nomes2.nome_ficticio) AS acs_nome_visita,
            res.criacao_data
           FROM ( SELECT tb1_1.municipio_id_sus,
                    tb1_1.quadrimestre_atual,
                    tb1_1.paciente_nome,
                    row_number() OVER (PARTITION BY 0::integer) AS seq,
                    concat(impulso_previne_dados_nominais.random_between(100000000, 999999999)::text, impulso_previne_dados_nominais.random_between(10, 99)::text) AS cidadao_cpf,
                    concat('7', impulso_previne_dados_nominais.random_between(100000000, 999999999)::text, impulso_previne_dados_nominais.random_between(10000, 99999)::text) AS cidadao_cns,
                    tb1_1.paciente_idade_atual,
                    tb1_1.dt_nascimento,
                    tb1_1.dt_ultimo_exame,
                    tb1_1.realizou_exame_ultimos_36_meses,
                    tb1_1.data_projetada_proximo_exame,
                    tb1_1.status_exame,
                    tb1_1.data_limite_a_realizar_proximo_exame,
                    tb1_1.cnes_estabelecimento_exame,
                    tb1_1.nome_estabelecimento_exame,
                    tb1_1.ine_equipe_exame,
                    tb1_1.nome_equipe_exame,
                    tb1_1.nome_profissional_exame,
                    tb1_1.dt_ultimo_cadastro,
                    tb1_1.estabelecimento_nome_cadastro,
                    tb1_1.estabelecimento_cnes_cadastro,
                    tb1_1.equipe_ine_cadastro,
                    tb1_1.equipe_nome_cadastro,
                    tb1_1.acs_nome_cadastro,
                    tb1_1.dt_ultimo_atendimento,
                    tb1_1.estabelecimento_nome_ultimo_atendimento,
                    tb1_1.estabelecimento_cnes_ultimo_atendimento,
                    tb1_1.equipe_ine_ultimo_atendimento,
                    tb1_1.equipe_nome_ultimo_atendimento,
                    tb1_1.acs_nome_ultimo_atendimento,
                    tb1_1.acs_nome_visita,
                    tb1_1.criacao_data
                   FROM dados_nominais_mg_vicosa.lista_nominal_citopatologico tb1_1) res
             JOIN configuracoes.nomes_ficticios_citopatologico nomes ON res.seq = nomes.seq
             JOIN configuracoes.nomes_ficticios_gestantes nomes2 ON res.seq = nomes2.seq
        ), dados_transmissoes_recentes AS (
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
            tb1_1.ine_equipe_exame,
            tb1_1.nome_equipe_exame,
            tb1_1.nome_profissional_exame,
            tb1_1.dt_ultimo_cadastro,
            tb1_1.estabelecimento_nome_cadastro,
            tb1_1.estabelecimento_cnes_cadastro,
            tb1_1.equipe_ine_cadastro,
            tb1_1.equipe_nome_cadastro,
            tb1_1.acs_nome_cadastro,
            tb1_1.dt_ultimo_atendimento,
            tb1_1.estabelecimento_nome_ultimo_atendimento,
            tb1_1.estabelecimento_cnes_ultimo_atendimento,
            tb1_1.equipe_ine_ultimo_atendimento,
            tb1_1.equipe_nome_ultimo_atendimento,
            tb1_1.acs_nome_ultimo_atendimento,
            tb1_1.acs_nome_visita,
            tb1_1.criacao_data
           FROM impulso_previne_dados_nominais.lista_nominal_citopatologico_unificada tb1_1
        ), une_as_bases AS (
         SELECT dados_anonimizados_demo_vicosa.municipio_id_sus,
            dados_anonimizados_demo_vicosa.quadrimestre_atual,
            dados_anonimizados_demo_vicosa.paciente_nome,
            dados_anonimizados_demo_vicosa.cidadao_cpf,
            dados_anonimizados_demo_vicosa.cidadao_cns,
            dados_anonimizados_demo_vicosa.paciente_idade_atual,
            dados_anonimizados_demo_vicosa.dt_nascimento,
            dados_anonimizados_demo_vicosa.dt_ultimo_exame,
            dados_anonimizados_demo_vicosa.realizou_exame_ultimos_36_meses,
            dados_anonimizados_demo_vicosa.data_projetada_proximo_exame,
            dados_anonimizados_demo_vicosa.status_exame,
            dados_anonimizados_demo_vicosa.data_limite_a_realizar_proximo_exame,
            dados_anonimizados_demo_vicosa.cnes_estabelecimento_exame,
            dados_anonimizados_demo_vicosa.nome_estabelecimento_exame,
            dados_anonimizados_demo_vicosa.ine_equipe_exame,
            dados_anonimizados_demo_vicosa.nome_equipe_exame,
            dados_anonimizados_demo_vicosa.nome_profissional_exame,
            dados_anonimizados_demo_vicosa.dt_ultimo_cadastro,
            dados_anonimizados_demo_vicosa.estabelecimento_nome_cadastro,
            dados_anonimizados_demo_vicosa.estabelecimento_cnes_cadastro,
            dados_anonimizados_demo_vicosa.equipe_ine_cadastro,
            dados_anonimizados_demo_vicosa.equipe_nome_cadastro,
            dados_anonimizados_demo_vicosa.acs_nome_cadastro,
            dados_anonimizados_demo_vicosa.dt_ultimo_atendimento,
            dados_anonimizados_demo_vicosa.estabelecimento_nome_ultimo_atendimento,
            dados_anonimizados_demo_vicosa.estabelecimento_cnes_ultimo_atendimento,
            dados_anonimizados_demo_vicosa.equipe_ine_ultimo_atendimento,
            dados_anonimizados_demo_vicosa.equipe_nome_ultimo_atendimento,
            dados_anonimizados_demo_vicosa.acs_nome_ultimo_atendimento,
            dados_anonimizados_demo_vicosa.acs_nome_visita,
            dados_anonimizados_demo_vicosa.criacao_data
           FROM dados_anonimizados_demo_vicosa
        UNION ALL
         SELECT dados_anonimizados_impulsolandia.municipio_id_sus,
            dados_anonimizados_impulsolandia.quadrimestre_atual,
            dados_anonimizados_impulsolandia.paciente_nome,
            dados_anonimizados_impulsolandia.cidadao_cpf,
            dados_anonimizados_impulsolandia.cidadao_cns,
            dados_anonimizados_impulsolandia.paciente_idade_atual,
            dados_anonimizados_impulsolandia.dt_nascimento,
            dados_anonimizados_impulsolandia.dt_ultimo_exame,
            dados_anonimizados_impulsolandia.realizou_exame_ultimos_36_meses,
            dados_anonimizados_impulsolandia.data_projetada_proximo_exame,
            dados_anonimizados_impulsolandia.status_exame,
            dados_anonimizados_impulsolandia.data_limite_a_realizar_proximo_exame,
            dados_anonimizados_impulsolandia.cnes_estabelecimento_exame,
            dados_anonimizados_impulsolandia.nome_estabelecimento_exame,
            dados_anonimizados_impulsolandia.ine_equipe_exame,
            dados_anonimizados_impulsolandia.nome_equipe_exame,
            dados_anonimizados_impulsolandia.nome_profissional_exame,
            dados_anonimizados_impulsolandia.dt_ultimo_cadastro,
            dados_anonimizados_impulsolandia.estabelecimento_nome_cadastro,
            dados_anonimizados_impulsolandia.estabelecimento_cnes_cadastro,
            dados_anonimizados_impulsolandia.equipe_ine_cadastro,
            dados_anonimizados_impulsolandia.equipe_nome_cadastro,
            dados_anonimizados_impulsolandia.acs_nome_cadastro,
            dados_anonimizados_impulsolandia.dt_ultimo_atendimento,
            dados_anonimizados_impulsolandia.estabelecimento_nome_ultimo_atendimento,
            dados_anonimizados_impulsolandia.estabelecimento_cnes_ultimo_atendimento,
            dados_anonimizados_impulsolandia.equipe_ine_ultimo_atendimento,
            dados_anonimizados_impulsolandia.equipe_nome_ultimo_atendimento,
            dados_anonimizados_impulsolandia.acs_nome_ultimo_atendimento,
            dados_anonimizados_impulsolandia.acs_nome_visita,
            dados_anonimizados_impulsolandia.criacao_data
           FROM dados_anonimizados_impulsolandia
        UNION ALL
         SELECT dados_transmissoes_recentes.municipio_id_sus,
            dados_transmissoes_recentes.quadrimestre_atual,
            dados_transmissoes_recentes.paciente_nome,
            dados_transmissoes_recentes.cidadao_cpf,
            dados_transmissoes_recentes.cidadao_cns,
            dados_transmissoes_recentes.paciente_idade_atual,
            dados_transmissoes_recentes.dt_nascimento,
            dados_transmissoes_recentes.dt_ultimo_exame,
            dados_transmissoes_recentes.realizou_exame_ultimos_36_meses,
            dados_transmissoes_recentes.data_projetada_proximo_exame,
            dados_transmissoes_recentes.status_exame,
            dados_transmissoes_recentes.data_limite_a_realizar_proximo_exame,
            dados_transmissoes_recentes.cnes_estabelecimento_exame,
            dados_transmissoes_recentes.nome_estabelecimento_exame,
            dados_transmissoes_recentes.ine_equipe_exame,
            dados_transmissoes_recentes.nome_equipe_exame,
            dados_transmissoes_recentes.nome_profissional_exame,
            dados_transmissoes_recentes.dt_ultimo_cadastro,
            dados_transmissoes_recentes.estabelecimento_nome_cadastro,
            dados_transmissoes_recentes.estabelecimento_cnes_cadastro,
            dados_transmissoes_recentes.equipe_ine_cadastro,
            dados_transmissoes_recentes.equipe_nome_cadastro,
            dados_transmissoes_recentes.acs_nome_cadastro,
            dados_transmissoes_recentes.dt_ultimo_atendimento,
            dados_transmissoes_recentes.estabelecimento_nome_ultimo_atendimento,
            dados_transmissoes_recentes.estabelecimento_cnes_ultimo_atendimento,
            dados_transmissoes_recentes.equipe_ine_ultimo_atendimento,
            dados_transmissoes_recentes.equipe_nome_ultimo_atendimento,
            dados_transmissoes_recentes.acs_nome_ultimo_atendimento,
            dados_transmissoes_recentes.acs_nome_visita,
            dados_transmissoes_recentes.criacao_data
           FROM dados_transmissoes_recentes
        )
, data_registro_producao AS (
    SELECT 
        municipio_id_sus,
        impulso_previne_dados_nominais.equipe_ine(municipio_id_sus::text, COALESCE(equipe_ine_cadastro, equipe_ine_ultimo_atendimento)::text) AS ine_master,
        MAX(GREATEST(dt_ultimo_exame,dt_ultimo_atendimento,dt_ultimo_cadastro)) AS dt_registro_producao_mais_recente,
        MIN(LEAST(dt_ultimo_exame,dt_ultimo_atendimento,dt_ultimo_cadastro)) AS dt_registro_producao_mais_antigo
    FROM une_as_bases
    GROUP BY 1, 2
)
, tabela_aux AS (
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
    COALESCE(tb1.acs_nome_visita, tb1.acs_nome_cadastro) AS acs_nome,
    COALESCE(tb1.estabelecimento_cnes_cadastro, tb1.estabelecimento_cnes_ultimo_atendimento) AS estabelecimento_cnes,
    COALESCE(tb1.estabelecimento_nome_cadastro, tb1.estabelecimento_nome_ultimo_atendimento) AS estabelecimento_nome,
    COALESCE(tb1.equipe_ine_cadastro, tb1.equipe_ine_ultimo_atendimento) AS equipe_ine,
    impulso_previne_dados_nominais.equipe_ine(tb1.municipio_id_sus::text, COALESCE(tb1.equipe_ine_cadastro, tb1.equipe_ine_ultimo_atendimento)::text) AS ine_master,
    impulso_previne_dados_nominais.equipe_ine(tb1.municipio_id_sus::text, COALESCE(tb1.equipe_nome_cadastro, tb1.equipe_nome_ultimo_atendimento)::text) AS equipe_nome,
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
    CURRENT_TIMESTAMP AS atualizacao_data
   FROM une_as_bases tb1
     LEFT JOIN listas_de_codigos.municipios tb2 ON tb1.municipio_id_sus::bpchar = tb2.id_sus
) SELECT
    tabela_aux.*,
    drp.dt_registro_producao_mais_recente
FROM tabela_aux
LEFT JOIN data_registro_producao drp 
    ON drp.municipio_id_sus = tabela_aux.municipio_id_sus
    AND drp.ine_master = tabela_aux.ine_master
WITH DATA;