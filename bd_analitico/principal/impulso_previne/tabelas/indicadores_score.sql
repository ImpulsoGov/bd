-- impulso_previne.indicadores_score definition

-- Drop table

-- DROP TABLE impulso_previne.indicadores_score;

CREATE TABLE impulso_previne.indicadores_score (
	id uuid NOT NULL,
	municipio_id_sus varchar(30) NOT NULL,
	periodo_id varchar(7) NOT NULL,
	indicadores_score_premissas_id uuid NOT NULL,
	var_diff_numerador_para_meta int4 NOT NULL,
	var_diff_resultado_para_meta int4 NOT NULL,
	var_quantidade_usuarios_total_meta numeric NOT NULL,
	var_identificacao_publico_alvo numeric NOT NULL,
	nota int4 NOT NULL,
	score int4 NOT NULL,
	criacao_data timestamptz NOT NULL,
	atualizacao_data timestamptz NOT NULL,
	CONSTRAINT indicadores_score_pk PRIMARY KEY (id)
);