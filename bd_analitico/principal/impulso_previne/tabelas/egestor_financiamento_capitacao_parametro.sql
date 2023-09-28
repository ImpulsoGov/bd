-- impulso_previne.egestor_financiamento_capitacao_parametro definition

-- Drop table

-- DROP TABLE impulso_previne.egestor_financiamento_capitacao_parametro;

CREATE TABLE impulso_previne.egestor_financiamento_capitacao_parametro (
	uf varchar(2) NULL,
	municipio_nome varchar(90) NULL,
	municipio_id_sus int4 NULL,
	competencia_financeira varchar(8) NULL,
	cadastro_esf_eap int4 NULL,
	cadastro_potencial int4 NULL,
	cadastro_populacao_vulneravel int4 NULL,
	cadastro_populacao_nao_vulneravel int4 NULL
);