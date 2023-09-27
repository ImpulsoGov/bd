-- impulso_previne_dados_nominais.lista_nominal_citopatologico_historico definition

-- Drop table

-- DROP TABLE impulso_previne_dados_nominais.lista_nominal_citopatologico_historico;

CREATE TABLE impulso_previne_dados_nominais.lista_nominal_citopatologico_historico (
	municipio_id_sus varchar(6) NULL,
	quadrimestre_atual text NULL,
	chave_mulher varchar(500) NULL,
	paciente_nome varchar(500) NULL,
	cidadao_cns varchar(30) NULL,
	cidadao_cpf varchar(500) NULL,
	paciente_idade_atual int8 NULL,
	dt_nascimento date NULL,
	dt_ultimo_exame date NULL,
	realizou_exame_ultimos_36_meses bool NULL,
	data_projetada_proximo_exame date NULL,
	status_exame varchar(50) NULL,
	data_limite_a_realizar_proximo_exame date NULL,
	cnes_estabelecimento_exame varchar(30) NULL,
	nome_estabelecimento_exame varchar(500) NULL,
	ine_equipe_exame varchar(30) NULL,
	nome_equipe_exame varchar(500) NULL,
	nome_profissional_exame varchar(500) NULL,
	dt_ultimo_cadastro date NULL,
	estabelecimento_nome_cadastro varchar(500) NULL,
	estabelecimento_cnes_cadastro varchar(30) NULL,
	equipe_ine_cadastro varchar(30) NULL,
	equipe_nome_cadastro varchar(500) NULL,
	acs_nome_cadastro varchar(500) NULL,
	dt_ultimo_atendimento date NULL,
	estabelecimento_nome_ultimo_atendimento varchar(500) NULL,
	estabelecimento_cnes_ultimo_atendimento varchar(30) NULL,
	equipe_ine_ultimo_atendimento varchar(30) NULL,
	equipe_nome_ultimo_atendimento varchar(500) NULL,
	acs_nome_ultimo_atendimento varchar(500) NULL,
	acs_nome_visita varchar(500) NULL,
	criacao_data timestamptz NULL,
	atualizacao_data timestamptz NULL DEFAULT CURRENT_TIMESTAMP
);