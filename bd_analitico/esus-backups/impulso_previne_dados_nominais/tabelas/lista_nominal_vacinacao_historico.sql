-- impulso_previne_dados_nominais.lista_nominal_vacinacao_historico definition

-- Drop table

-- DROP TABLE impulso_previne_dados_nominais.lista_nominal_vacinacao_historico;

CREATE TABLE impulso_previne_dados_nominais.lista_nominal_vacinacao_historico (
	municipio_id_sus varchar(6) NULL,
	chave_cidadao varchar(500) NULL,
	cidadao_nome varchar(500) NULL,
	cidadao_cpf varchar(20) NULL,
	cidadao_cns varchar(20) NULL,
	cidadao_sexo varchar(20) NULL,
	dt_nascimento date NULL,
	cidadao_nome_responsavel varchar(500) NULL,
	cidadao_cns_responsavel varchar(20) NULL,
	cidadao_cpf_responsavel varchar(20) NULL,
	cidadao_idade_meses_atual int4 NULL,
	cidadao_idade_meses_inicio_quadri int4 NULL,
	cidadao_idade_meses_fim_quadri int4 NULL,
	se_faleceu int4 NULL,
	co_seq_fat_vacinacao varchar(30) NULL,
	co_seq_fat_vacinacao_vacina varchar(30) NULL,
	tipo_ficha varchar(100) NULL,
	codigo_vacina varchar(10) NULL,
	nome_vacina varchar(100) NULL,
	dose_vacina varchar(100) NULL,
	data_registro_vacina date NULL,
	estabelecimento_cnes_aplicacao_vacina varchar(30) NULL,
	estabelecimento_nome_aplicacao_vacina varchar(500) NULL,
	equipe_ine_aplicacao_vacina varchar(20) NULL,
	equipe_nome_aplicacao_vacina varchar(500) NULL,
	profissional_nome_aplicacao_vacina varchar(500) NULL,
	data_ultimo_cadastro_individual date NULL,
	estabelecimento_cnes_cadastro varchar(30) NULL,
	estabelecimento_nome_cadastro varchar(500) NULL,
	equipe_ine_cadastro varchar(20) NULL,
	equipe_nome_cadastro varchar(255) NULL,
	acs_nome_cadastro varchar(255) NULL,
	estabelecimento_cnes_atendimento varchar(20) NULL,
	estabelecimento_nome_atendimento varchar(500) NULL,
	equipe_ine_atendimento varchar(20) NULL,
	equipe_nome_atendimento varchar(255) NULL,
	data_ultimo_atendimento_individual date NULL,
	data_ultima_vista_domiciliar date NULL,
	acs_nome_visita varchar(255) NULL,
	criacao_data timestamptz NULL,
	atualizacao_data timestamptz NULL DEFAULT CURRENT_TIMESTAMP
);