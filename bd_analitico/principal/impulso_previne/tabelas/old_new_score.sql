-- impulso_previne.old_new_score definition

-- Drop table

-- DROP TABLE impulso_previne.old_new_score;

CREATE TABLE impulso_previne.old_new_score (
	municipio_id_ibge varchar NULL,
	municipio_nome varchar(50) NULL,
	indicador_nome varchar(50) NULL,
	periodo_codigo varchar(50) NULL,
	new_score int4 NULL,
	old_score int4 NULL
);