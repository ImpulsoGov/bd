CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.eventos_pre_natal
TABLESPACE pg_default
AS SELECT unir_tabelas_lista_nominal_gestantes.municipio_id_sus,
    unir_tabelas_lista_nominal_gestantes.id_registro,
    unir_tabelas_lista_nominal_gestantes.tipo_registro,
    unir_tabelas_lista_nominal_gestantes.data_registro,
    unir_tabelas_lista_nominal_gestantes.chave_gestante,
    unir_tabelas_lista_nominal_gestantes.gestante_nome,
    unir_tabelas_lista_nominal_gestantes.gestante_data_de_nascimento,
    unir_tabelas_lista_nominal_gestantes.gestante_documento_cpf,
    unir_tabelas_lista_nominal_gestantes.gestante_documento_cns,
    unir_tabelas_lista_nominal_gestantes.gestante_telefone,
    unir_tabelas_lista_nominal_gestantes.data_dum,
    unir_tabelas_lista_nominal_gestantes.idade_gestacional_atendimento,
    unir_tabelas_lista_nominal_gestantes.profissional_cns_atendimento,
    unir_tabelas_lista_nominal_gestantes.profissional_nome_atendimento,
    unir_tabelas_lista_nominal_gestantes.estabelecimento_cnes_atendimento,
    unir_tabelas_lista_nominal_gestantes.estabelecimento_nome_atendimento,
    unir_tabelas_lista_nominal_gestantes.equipe_ine_atendimento,
    unir_tabelas_lista_nominal_gestantes.equipe_nome_atendimento,
    unir_tabelas_lista_nominal_gestantes.data_ultimo_cadastro_individual,
    unir_tabelas_lista_nominal_gestantes.estabelecimento_cnes_cad_indivual,
    unir_tabelas_lista_nominal_gestantes.estabelecimento_nome_cad_individual,
    unir_tabelas_lista_nominal_gestantes.equipe_ine_cad_individual,
    unir_tabelas_lista_nominal_gestantes.equipe_nome_cad_individual,
    unir_tabelas_lista_nominal_gestantes.data_ultima_visita_acs,
    unir_tabelas_lista_nominal_gestantes.acs_visita_domiciliar,
    unir_tabelas_lista_nominal_gestantes.acs_cad_dom_familia,
    unir_tabelas_lista_nominal_gestantes.acs_cad_individual,
    unir_tabelas_lista_nominal_gestantes.criacao_data,
    unir_tabelas_lista_nominal_gestantes.atualizacao_data
   FROM impulso_previne_dados_nominais.unir_tabelas_lista_nominal_gestantes() unir_tabelas_lista_nominal_gestantes(municipio_id_sus, id_registro, tipo_registro, data_registro, chave_gestante, gestante_nome, gestante_data_de_nascimento, gestante_documento_cpf, gestante_documento_cns, gestante_telefone, data_dum, idade_gestacional_atendimento, profissional_cns_atendimento, profissional_nome_atendimento, estabelecimento_cnes_atendimento, estabelecimento_nome_atendimento, equipe_ine_atendimento, equipe_nome_atendimento, data_ultimo_cadastro_individual, estabelecimento_cnes_cad_indivual, estabelecimento_nome_cad_individual, equipe_ine_cad_individual, equipe_nome_cad_individual, data_ultima_visita_acs, acs_visita_domiciliar, acs_cad_dom_familia, acs_cad_individual, criacao_data, atualizacao_data)
WITH DATA;

-- View indexes:
CREATE INDEX eventos_pre_natal_id_registro_idx ON impulso_previne_dados_nominais.eventos_pre_natal USING btree (id_registro, chave_gestante);
CREATE INDEX eventos_pre_natal_tipo_registro_idx ON impulso_previne_dados_nominais.eventos_pre_natal USING btree (tipo_registro, data_registro);