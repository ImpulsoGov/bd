-- impulso_previne_dados_nominais.lista_nominal_diabeticos_unificada source

CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.lista_nominal_diabeticos_unificada
TABLESPACE pg_default
AS SELECT consulta_tabelas_lista_nominal_diabeticos.municipio_id_sus,
    consulta_tabelas_lista_nominal_diabeticos.quadrimestre_atual,
    consulta_tabelas_lista_nominal_diabeticos.realizou_solicitacao_hemoglobina_ultimos_6_meses,
    consulta_tabelas_lista_nominal_diabeticos.dt_solicitacao_hemoglobina_glicada_mais_recente,
    consulta_tabelas_lista_nominal_diabeticos.realizou_consulta_ultimos_6_meses,
    consulta_tabelas_lista_nominal_diabeticos.dt_consulta_mais_recente,
    consulta_tabelas_lista_nominal_diabeticos.co_seq_fat_cidadao_pec,
    consulta_tabelas_lista_nominal_diabeticos.cidadao_cpf,
    consulta_tabelas_lista_nominal_diabeticos.cidadao_cns,
    consulta_tabelas_lista_nominal_diabeticos.cidadao_nome,
    consulta_tabelas_lista_nominal_diabeticos.cidadao_nome_social,
    consulta_tabelas_lista_nominal_diabeticos.cidadao_sexo,
    consulta_tabelas_lista_nominal_diabeticos.dt_nascimento,
    consulta_tabelas_lista_nominal_diabeticos.estabelecimento_cnes_atendimento,
    consulta_tabelas_lista_nominal_diabeticos.estabelecimento_cnes_cadastro,
    consulta_tabelas_lista_nominal_diabeticos.estabelecimento_nome_atendimento,
    consulta_tabelas_lista_nominal_diabeticos.estabelecimento_nome_cadastro,
    consulta_tabelas_lista_nominal_diabeticos.equipe_ine_atendimento,
    consulta_tabelas_lista_nominal_diabeticos.equipe_ine_cadastro,
    consulta_tabelas_lista_nominal_diabeticos.equipe_ine_procedimento,
    consulta_tabelas_lista_nominal_diabeticos.equipe_nome_atendimento,
    consulta_tabelas_lista_nominal_diabeticos.equipe_nome_cadastro,
    consulta_tabelas_lista_nominal_diabeticos.equipe_nome_procedimento,
    consulta_tabelas_lista_nominal_diabeticos.acs_nome_cadastro,
    consulta_tabelas_lista_nominal_diabeticos.acs_nome_visita,
    consulta_tabelas_lista_nominal_diabeticos.profissional_nome_atendimento,
    consulta_tabelas_lista_nominal_diabeticos.profissional_nome_procedimento,
    consulta_tabelas_lista_nominal_diabeticos.possui_diabetes_autoreferida,
    consulta_tabelas_lista_nominal_diabeticos.possui_diabetes_diagnosticada,
    consulta_tabelas_lista_nominal_diabeticos.data_ultimo_cadastro,
    consulta_tabelas_lista_nominal_diabeticos.dt_ultima_consulta,
    consulta_tabelas_lista_nominal_diabeticos.se_faleceu,
    consulta_tabelas_lista_nominal_diabeticos.se_mudou,
    consulta_tabelas_lista_nominal_diabeticos.criacao_data,
    now() AS atualizacao_data
   FROM impulso_previne_dados_nominais.consulta_tabelas_lista_nominal_diabeticos() consulta_tabelas_lista_nominal_diabeticos(
   municipio_id_sus, 
   quadrimestre_atual, 
   realizou_solicitacao_hemoglobina_ultimos_6_meses,
   dt_solicitacao_hemoglobina_glicada_mais_recente, 
   realizou_consulta_ultimos_6_meses, 
   dt_consulta_mais_recente, 
   co_seq_fat_cidadao_pec, 
   cidadao_cpf, 
   cidadao_cns, 
   cidadao_nome, 
   cidadao_nome_social, 
   cidadao_sexo, 
   dt_nascimento, 
   estabelecimento_cnes_atendimento, 
   estabelecimento_cnes_cadastro, 
   estabelecimento_nome_atendimento, 
   estabelecimento_nome_cadastro, 
   equipe_ine_atendimento, 
   equipe_ine_cadastro, 
   equipe_ine_procedimento, 
   equipe_nome_atendimento, 
   equipe_nome_cadastro, 
   equipe_nome_procedimento,
   acs_nome_cadastro, 
   acs_nome_visita,
   profissional_nome_atendimento,
   profissional_nome_procedimento,
   possui_diabetes_autoreferida, 
   possui_diabetes_diagnosticada,
   data_ultimo_cadastro, 
   dt_ultima_consulta, 
   se_faleceu, 
   se_mudou, 
   criacao_data, 
   atualizacao_data)
   WITH DATA;
