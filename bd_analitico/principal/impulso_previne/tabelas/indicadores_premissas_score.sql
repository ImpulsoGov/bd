-- impulso_previne.indicadores_premissas_score definition

-- Drop table

-- DROP TABLE impulso_previne.indicadores_premissas_score;

CREATE TABLE impulso_previne.indicadores_premissas_score (
	id uuid NOT NULL,
	indicador_regras_id uuid NULL,
	versao varchar(5) NOT NULL,
	versao_inicio date NOT NULL,
	versao_fim date NULL,
	criacao_data timestamptz NOT NULL,
	atualizacao_data timestamptz NOT NULL,
	validade_resultado float4 NULL,
	acoes_por_usuario float4 NULL,
	CONSTRAINT indicadores_premissas_score_pk PRIMARY KEY (id)
);


-- impulso_previne.indicadores_premissas_score foreign keys

ALTER TABLE impulso_previne.indicadores_premissas_score ADD CONSTRAINT indicadores_premissas_regras_id FOREIGN KEY (indicador_regras_id) REFERENCES previne_brasil.indicadores_regras(id);