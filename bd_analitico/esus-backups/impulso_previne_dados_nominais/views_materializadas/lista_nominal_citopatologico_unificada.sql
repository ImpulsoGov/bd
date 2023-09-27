
CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.lista_nominal_citopatologico_unificada
TABLESPACE pg_default
AS SELECT consulta_tabelas_lista_nominal_citopatologico.municipio_id_sus,
    consulta_tabelas_lista_nominal_citopatologico.quadrimestre_atual,
    consulta_tabelas_lista_nominal_citopatologico.chave_mulher,
    consulta_tabelas_lista_nominal_citopatologico.paciente_nome,
    consulta_tabelas_lista_nominal_citopatologico.cidadao_cns,
    consulta_tabelas_lista_nominal_citopatologico.cidadao_cpf,
    consulta_tabelas_lista_nominal_citopatologico.paciente_idade_atual,
    consulta_tabelas_lista_nominal_citopatologico.dt_nascimento,
    consulta_tabelas_lista_nominal_citopatologico.dt_ultimo_exame,
    consulta_tabelas_lista_nominal_citopatologico.realizou_exame_ultimos_36_meses,
    consulta_tabelas_lista_nominal_citopatologico.data_projetada_proximo_exame,
    consulta_tabelas_lista_nominal_citopatologico.status_exame,
    consulta_tabelas_lista_nominal_citopatologico.data_limite_a_realizar_proximo_exame,
    consulta_tabelas_lista_nominal_citopatologico.cnes_estabelecimento_exame,
    consulta_tabelas_lista_nominal_citopatologico.nome_estabelecimento_exame,
    consulta_tabelas_lista_nominal_citopatologico.ine_equipe_exame,
    consulta_tabelas_lista_nominal_citopatologico.nome_equipe_exame,
    consulta_tabelas_lista_nominal_citopatologico.nome_profissional_exame,
    consulta_tabelas_lista_nominal_citopatologico.dt_ultimo_cadastro,
    consulta_tabelas_lista_nominal_citopatologico.estabelecimento_nome_cadastro,
    consulta_tabelas_lista_nominal_citopatologico.estabelecimento_cnes_cadastro,
    consulta_tabelas_lista_nominal_citopatologico.equipe_ine_cadastro,
    consulta_tabelas_lista_nominal_citopatologico.equipe_nome_cadastro,
    consulta_tabelas_lista_nominal_citopatologico.acs_nome_cadastro,
    consulta_tabelas_lista_nominal_citopatologico.dt_ultimo_atendimento,
    consulta_tabelas_lista_nominal_citopatologico.estabelecimento_nome_ultimo_atendimento,
    consulta_tabelas_lista_nominal_citopatologico.estabelecimento_cnes_ultimo_atendimento,
    consulta_tabelas_lista_nominal_citopatologico.equipe_ine_ultimo_atendimento,
    consulta_tabelas_lista_nominal_citopatologico.equipe_nome_ultimo_atendimento,
    consulta_tabelas_lista_nominal_citopatologico.acs_nome_ultimo_atendimento,
    consulta_tabelas_lista_nominal_citopatologico.acs_nome_visita,
    consulta_tabelas_lista_nominal_citopatologico.criacao_data,
    consulta_tabelas_lista_nominal_citopatologico.atualizacao_data
   FROM impulso_previne_dados_nominais.consulta_tabelas_lista_nominal_citopatologico() consulta_tabelas_lista_nominal_citopatologico(municipio_id_sus, quadrimestre_atual, chave_mulher, paciente_nome, cidadao_cns, cidadao_cpf, paciente_idade_atual, dt_nascimento, dt_ultimo_exame, realizou_exame_ultimos_36_meses, data_projetada_proximo_exame, status_exame, data_limite_a_realizar_proximo_exame, cnes_estabelecimento_exame, nome_estabelecimento_exame, ine_equipe_exame, nome_equipe_exame, nome_profissional_exame, dt_ultimo_cadastro, estabelecimento_nome_cadastro, estabelecimento_cnes_cadastro, equipe_ine_cadastro, equipe_nome_cadastro, acs_nome_cadastro, dt_ultimo_atendimento, estabelecimento_nome_ultimo_atendimento, estabelecimento_cnes_ultimo_atendimento, equipe_ine_ultimo_atendimento, equipe_nome_ultimo_atendimento, acs_nome_ultimo_atendimento, acs_nome_visita, criacao_data, atualizacao_data)
WITH DATA;