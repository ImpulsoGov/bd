
CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.lista_nominal_hipertensos_unificada
TABLESPACE pg_default
AS SELECT consulta_tabelas_lista_nominal_hipertensos.municipio_id_sus,
    consulta_tabelas_lista_nominal_hipertensos.quadrimestre_atual,
    consulta_tabelas_lista_nominal_hipertensos.realizou_afericao_ultimos_6_meses,
    consulta_tabelas_lista_nominal_hipertensos.dt_afericao_pressao_mais_recente,
    consulta_tabelas_lista_nominal_hipertensos.realizou_consulta_ultimos_6_meses,
    consulta_tabelas_lista_nominal_hipertensos.dt_consulta_mais_recente,
    consulta_tabelas_lista_nominal_hipertensos.co_seq_fat_cidadao_pec,
    consulta_tabelas_lista_nominal_hipertensos.cidadao_cpf,
    consulta_tabelas_lista_nominal_hipertensos.cidadao_cns,
    consulta_tabelas_lista_nominal_hipertensos.cidadao_nome,
    consulta_tabelas_lista_nominal_hipertensos.cidadao_nome_social,
    consulta_tabelas_lista_nominal_hipertensos.cidadao_sexo,
    consulta_tabelas_lista_nominal_hipertensos.dt_nascimento,
    consulta_tabelas_lista_nominal_hipertensos.estabelecimento_cnes_atendimento,
    consulta_tabelas_lista_nominal_hipertensos.estabelecimento_cnes_cadastro,
    consulta_tabelas_lista_nominal_hipertensos.estabelecimento_nome_atendimento,
    consulta_tabelas_lista_nominal_hipertensos.estabelecimento_nome_cadastro,
    consulta_tabelas_lista_nominal_hipertensos.equipe_ine_atendimento,
    consulta_tabelas_lista_nominal_hipertensos.equipe_ine_cadastro,
    consulta_tabelas_lista_nominal_hipertensos.equipe_nome_atendimento,
    consulta_tabelas_lista_nominal_hipertensos.equipe_nome_cadastro,
    consulta_tabelas_lista_nominal_hipertensos.acs_nome_cadastro,
    consulta_tabelas_lista_nominal_hipertensos.acs_nome_visita,
    consulta_tabelas_lista_nominal_hipertensos.possui_hipertensao_autorreferida,
    consulta_tabelas_lista_nominal_hipertensos.possui_hipertensao_diagnosticada,
    consulta_tabelas_lista_nominal_hipertensos.data_ultimo_cadastro,
    consulta_tabelas_lista_nominal_hipertensos.dt_ultima_consulta,
    consulta_tabelas_lista_nominal_hipertensos.se_faleceu,
    consulta_tabelas_lista_nominal_hipertensos.se_mudou,
    consulta_tabelas_lista_nominal_hipertensos.criacao_data,
    consulta_tabelas_lista_nominal_hipertensos.atualizacao_data
   FROM impulso_previne_dados_nominais.consulta_tabelas_lista_nominal_hipertensos() consulta_tabelas_lista_nominal_hipertensos(municipio_id_sus, quadrimestre_atual, realizou_afericao_ultimos_6_meses, dt_afericao_pressao_mais_recente, realizou_consulta_ultimos_6_meses, dt_consulta_mais_recente, co_seq_fat_cidadao_pec, cidadao_cpf, cidadao_cns, cidadao_nome, cidadao_nome_social, cidadao_sexo, dt_nascimento, estabelecimento_cnes_atendimento, estabelecimento_cnes_cadastro, estabelecimento_nome_atendimento, estabelecimento_nome_cadastro, equipe_ine_atendimento, equipe_ine_cadastro, equipe_nome_atendimento, equipe_nome_cadastro, acs_nome_cadastro, acs_nome_visita, possui_hipertensao_autorreferida, possui_hipertensao_diagnosticada, data_ultimo_cadastro, dt_ultima_consulta, se_faleceu, se_mudou, criacao_data, atualizacao_data)
WITH DATA;