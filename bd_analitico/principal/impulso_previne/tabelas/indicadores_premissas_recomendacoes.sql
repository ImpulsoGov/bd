-- impulso_previne.indicadores_premissas_recomendacoes definition

-- Drop table

-- DROP TABLE impulso_previne.indicadores_premissas_recomendacoes;

CREATE TABLE impulso_previne.indicadores_premissas_recomendacoes (
	id uuid NOT NULL,
	indicador_regras_id uuid NULL,
	versao varchar(5) NOT NULL,
	versao_inicio date NOT NULL,
	versao_fim date NOT NULL,
	recomendacao text NOT NULL,
	criacao_data timestamptz NOT NULL,
	atualizacao_data timestamptz NOT NULL,
	indicadores_parametros_id uuid NOT NULL,
	CONSTRAINT indicadores_premissas_recomendacoes_pk PRIMARY KEY (id)
);