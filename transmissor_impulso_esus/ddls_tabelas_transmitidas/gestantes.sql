"""DDL de criação da tabela que recebe os dados da transmissão da lista de gestantes"""

--DROP TABLE IF EXISTS dados_ip_impulsolandia.lista_nominal_gestantes;
CREATE TABLE dados_ip_impulsolandia.lista_nominal_gestantes (
	municipio_id_sus varchar(6) NULL DEFAULT 111111,
	id_registro varchar(10) NOT NULL,
	tipo_registro varchar(50) NOT NULL,
	data_registro date NULL,
	chave_gestante varchar(500) NULL,
	gestante_nome varchar(500) NULL,
	gestante_data_de_nascimento date NULL,
	gestante_documento_cpf varchar(20) NULL,
	gestante_documento_cns varchar(20) NULL,
	gestante_telefone varchar(20) NULL,
	data_dum date NULL,
	idade_gestacional_atendimento int4 NULL,
	profissional_cns_atendimento varchar(15) NULL,
	profissional_nome_atendimento varchar(255) NULL,
	estabelecimento_cnes_atendimento varchar(20) NULL,
	estabelecimento_nome_atendimento varchar(500) NULL,
	equipe_ine_atendimento varchar(20) NULL,
	equipe_nome_atendimento varchar(500) NULL,
	data_ultimo_cadastro_individual date NULL,
	estabelecimento_cnes_cad_indivual varchar(20) NULL,
	estabelecimento_nome_cad_individual varchar(500) NULL,
	equipe_ine_cad_individual varchar(20) NULL,
	equipe_nome_cad_individual varchar(500) NULL,
	data_ultima_visita_acs date NULL,
	acs_visita_domiciliar varchar(255) NULL,
	acs_cad_dom_familia varchar(255) NULL,
	acs_cad_individual varchar(255) NULL,
	criacao_data timestamptz NULL,
	atualizacao_data timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT registros_atendimentos_pre_natal_01_pk PRIMARY KEY (id_registro, tipo_registro)
);
CREATE INDEX registros_atendimentos_pre_natal_01_idx ON dados_ip_impulsolandia.lista_nominal_gestantes USING btree (municipio_id_sus, id_registro, tipo_registro);
