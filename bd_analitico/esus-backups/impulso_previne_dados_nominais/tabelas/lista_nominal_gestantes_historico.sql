-- impulso_previne_dados_nominais.lista_nominal_gestantes_historico definition

-- Drop table

-- DROP TABLE impulso_previne_dados_nominais.lista_nominal_gestantes_historico;

CREATE TABLE impulso_previne_dados_nominais.lista_nominal_gestantes_historico (
	municipio_id_sus varchar(6) NULL,
	chave_gestacao varchar(500) NOT NULL,
	ordem_gestacao varchar(30) NULL,
	chave_gestante varchar(500) NOT NULL,
	gestante_telefone varchar(100) NULL,
	gestante_nome varchar(500) NULL,
	gestante_data_de_nascimento date NULL,
	estabelecimento_cnes varchar(10) NULL,
	estabelecimento_nome varchar(500) NULL,
	equipe_ine varchar(10) NULL,
	equipe_nome varchar(500) NULL,
	acs_nome varchar(500) NULL,
	acs_data_ultima_visita date NULL,
	gestacao_data_dum date NULL,
	gestacao_data_dpp date NULL,
	gestacao_dpp_dias_para int4 NULL,
	gestacao_quadrimestre varchar(10) NULL,
	gestacao_idade_gestacional_primeiro_atendimento int4 NULL,
	consulta_prenatal_primeira_data date NULL,
	consulta_prenatal_ultima_data date NULL,
	consulta_prenatal_ultima_dias_desde int4 NULL,
	data_fim_primeira_gestacao date NULL,
	tipo_encerramento_primeira_gestacao varchar(50) NULL,
	gestante_documento_cpf varchar(15) NULL,
	gestante_documento_cns varchar(20) NULL,
	gestacao_idade_gestacional_atual int4 NULL,
	sinalizacao_erro_registro varchar(500) NULL,
	gestacao_qtde_dums varchar(100) NULL,
	consultas_prenatal_total int4 NULL,
	consultas_pre_natal_validas int4 NULL,
	atendimento_odontologico_realizado bool NULL,
	exame_hiv_realizado bool NULL,
	exame_sifilis_realizado bool NULL,
	possui_registro_aborto varchar(6) NULL,
	possui_registro_parto varchar(6) NULL,
	exame_sifilis_hiv_realizado bool NULL,
	periodo_semana_transmissao int8 NULL,
	periodo_data_transmissao date NULL,
	criacao_data timestamptz NULL,
	atualizacao_data timestamptz NULL DEFAULT CURRENT_TIMESTAMP
);