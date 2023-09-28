-- impulso_previne.crm_dados_incricoes_contatos definition

-- Drop table

-- DROP TABLE impulso_previne.crm_dados_incricoes_contatos;

CREATE TABLE impulso_previne.crm_dados_incricoes_contatos (
	id_inscricao int8 NULL,
	email varchar(200) NULL,
	telefone varchar(200) NULL,
	nome varchar(200) NULL,
	cargo varchar(200) NULL,
	estado varchar(200) NULL,
	municipio varchar(200) NULL,
	data_registro date NULL,
	origem_inscricao varchar(200) NULL,
	data_inscricao_consultoria date NULL,
	data_inscricao_webinar date NULL,
	data_inscricao_manual_ip date NULL,
	data_inscricao_conteudo date NULL,
	data_inscricao_conasems date NULL
);