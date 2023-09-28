-- impulso_previne.usuarios_ip definition

-- Drop table

-- DROP TABLE impulso_previne.usuarios_ip;

CREATE TABLE impulso_previne.usuarios_ip (
	id uuid NOT NULL,
	municipio varchar NOT NULL,
	cargo varchar NOT NULL,
	telefone varchar NOT NULL,
	id_usuario uuid NOT NULL,
	criacao_data timestamp NOT NULL,
	atualizacao_data timestamp NOT NULL,
	whatsapp bool NOT NULL,
	equipe varchar NULL,
	CONSTRAINT usuarios_ip_pk PRIMARY KEY (id)
);