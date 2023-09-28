-- impulso_previne.validacao_premissas_recomendacoes definition

-- Drop table

-- DROP TABLE impulso_previne.validacao_premissas_recomendacoes;

CREATE TABLE impulso_previne.validacao_premissas_recomendacoes (
	id uuid NOT NULL,
	versao varchar(5) NOT NULL,
	versao_inicio date NOT NULL,
	versao_fim date NULL,
	recomendacao text NOT NULL,
	criacao_data timestamptz NOT NULL,
	atualizacao_data timestamptz NOT NULL,
	validacao_nome varchar(100) NULL,
	CONSTRAINT indicadores_premissas_recomendacoes_pk_1 PRIMARY KEY (id)
);