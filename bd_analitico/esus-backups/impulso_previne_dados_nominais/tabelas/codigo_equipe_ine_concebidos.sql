-- impulso_previne_dados_nominais.codigo_equipe_ine_concebidos definition

-- Drop table

-- DROP TABLE impulso_previne_dados_nominais.codigo_equipe_ine_concebidos;

CREATE TABLE impulso_previne_dados_nominais.codigo_equipe_ine_concebidos (
	municipio_uf varchar(500) NOT NULL,
	municipio_id_sus varchar(10) NOT NULL,
	equipe_ine varchar(10) NOT NULL,
	equipe_super_ine varchar(10) NOT NULL,
	CONSTRAINT codigo_equipe_ine_concebidos_pk PRIMARY KEY (equipe_ine)
);