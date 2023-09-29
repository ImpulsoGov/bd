-- impulso_previne_dados_nominais.lista_nominal_gestantes_duplicadas_por_erro_grafia definition

-- Drop table

-- DROP TABLE impulso_previne_dados_nominais.lista_nominal_gestantes_duplicadas_por_erro_grafia;

CREATE TABLE impulso_previne_dados_nominais.lista_nominal_gestantes_duplicadas_por_erro_grafia (
	municipio_id_sus varchar NULL,
	gestante_nome varchar NULL,
	gestante_data_de_nascimento date NULL,
	gestante_documento_cpf varchar NULL,
	gestante_documento_cns varchar NULL,
	periodo_data_transmissao date NULL,
	gestante_dum date NULL,
	gestante_dpp date NULL,
	equipe_ine text NULL,
	equipe_nome text NULL,
	estabelecimento_cnes text NULL,
	estabelecimento_nome text NULL,
	acs_nome text NULL,
	atualizacao_data timestamptz NULL DEFAULT now()
);