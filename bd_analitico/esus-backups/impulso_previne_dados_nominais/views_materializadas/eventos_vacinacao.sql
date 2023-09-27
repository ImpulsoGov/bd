
CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.eventos_vacinacao
TABLESPACE pg_default
AS SELECT unir_tabelas_lista_nominal_vacinacao.municipio_id_sus,
    unir_tabelas_lista_nominal_vacinacao.chave_cidadao,
    unir_tabelas_lista_nominal_vacinacao.cidadao_nome,
    unir_tabelas_lista_nominal_vacinacao.cidadao_cpf,
    unir_tabelas_lista_nominal_vacinacao.cidadao_cns,
    unir_tabelas_lista_nominal_vacinacao.cidadao_sexo,
    unir_tabelas_lista_nominal_vacinacao.dt_nascimento,
    unir_tabelas_lista_nominal_vacinacao.cidadao_nome_responsavel,
    unir_tabelas_lista_nominal_vacinacao.cidadao_cns_responsavel,
    unir_tabelas_lista_nominal_vacinacao.cidadao_cpf_responsavel,
    unir_tabelas_lista_nominal_vacinacao.cidadao_idade_meses_atual,
    unir_tabelas_lista_nominal_vacinacao.cidadao_idade_meses_inicio_quadri,
    unir_tabelas_lista_nominal_vacinacao.cidadao_idade_meses_fim_quadri,
    unir_tabelas_lista_nominal_vacinacao.se_faleceu,
    unir_tabelas_lista_nominal_vacinacao.co_seq_fat_vacinacao,
    unir_tabelas_lista_nominal_vacinacao.co_seq_fat_vacinacao_vacina,
    unir_tabelas_lista_nominal_vacinacao.tipo_ficha,
    unir_tabelas_lista_nominal_vacinacao.codigo_vacina,
    unir_tabelas_lista_nominal_vacinacao.nome_vacina,
    unir_tabelas_lista_nominal_vacinacao.dose_vacina,
    unir_tabelas_lista_nominal_vacinacao.data_registro_vacina,
    unir_tabelas_lista_nominal_vacinacao.estabelecimento_cnes_aplicacao_vacina,
    unir_tabelas_lista_nominal_vacinacao.estabelecimento_nome_aplicacao_vacina,
    unir_tabelas_lista_nominal_vacinacao.equipe_ine_aplicacao_vacina,
    unir_tabelas_lista_nominal_vacinacao.equipe_nome_aplicacao_vacina,
    unir_tabelas_lista_nominal_vacinacao.profissional_nome_aplicacao_vacina,
    unir_tabelas_lista_nominal_vacinacao.data_ultimo_cadastro_individual,
    unir_tabelas_lista_nominal_vacinacao.estabelecimento_cnes_cadastro,
    unir_tabelas_lista_nominal_vacinacao.estabelecimento_nome_cadastro,
    unir_tabelas_lista_nominal_vacinacao.equipe_ine_cadastro,
    unir_tabelas_lista_nominal_vacinacao.equipe_nome_cadastro,
    unir_tabelas_lista_nominal_vacinacao.acs_nome_cadastro,
    unir_tabelas_lista_nominal_vacinacao.estabelecimento_cnes_atendimento,
    unir_tabelas_lista_nominal_vacinacao.estabelecimento_nome_atendimento,
    unir_tabelas_lista_nominal_vacinacao.equipe_ine_atendimento,
    unir_tabelas_lista_nominal_vacinacao.equipe_nome_atendimento,
    unir_tabelas_lista_nominal_vacinacao.data_ultimo_atendimento_individual,
    unir_tabelas_lista_nominal_vacinacao.data_ultima_vista_domiciliar,
    unir_tabelas_lista_nominal_vacinacao.acs_nome_visita,
    unir_tabelas_lista_nominal_vacinacao.criacao_data
   FROM impulso_previne_dados_nominais.unir_tabelas_lista_nominal_vacinacao() unir_tabelas_lista_nominal_vacinacao(municipio_id_sus, chave_cidadao, cidadao_nome, cidadao_cpf, cidadao_cns, cidadao_sexo, dt_nascimento, cidadao_nome_responsavel, cidadao_cns_responsavel, cidadao_cpf_responsavel, cidadao_idade_meses_atual, cidadao_idade_meses_inicio_quadri, cidadao_idade_meses_fim_quadri, se_faleceu, co_seq_fat_vacinacao, co_seq_fat_vacinacao_vacina, tipo_ficha, codigo_vacina, nome_vacina, dose_vacina, data_registro_vacina, estabelecimento_cnes_aplicacao_vacina, estabelecimento_nome_aplicacao_vacina, equipe_ine_aplicacao_vacina, equipe_nome_aplicacao_vacina, profissional_nome_aplicacao_vacina, data_ultimo_cadastro_individual, estabelecimento_cnes_cadastro, estabelecimento_nome_cadastro, equipe_ine_cadastro, equipe_nome_cadastro, acs_nome_cadastro, estabelecimento_cnes_atendimento, estabelecimento_nome_atendimento, equipe_ine_atendimento, equipe_nome_atendimento, data_ultimo_atendimento_individual, data_ultima_vista_domiciliar, acs_nome_visita, criacao_data)
WITH DATA;