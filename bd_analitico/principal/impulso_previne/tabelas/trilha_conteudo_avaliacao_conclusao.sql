-- impulso_previne.trilha_conteudo_avaliacao_conclusao definition

-- Drop table

-- DROP TABLE impulso_previne.trilha_conteudo_avaliacao_conclusao;

CREATE TABLE impulso_previne.trilha_conteudo_avaliacao_conclusao (
	id uuid NOT NULL,
	concluido bool NULL,
	avaliacao int4 NULL,
	usuario_id uuid NOT NULL,
	criacao_data timestamp NOT NULL,
	atualizacao_data timestamp NOT NULL,
	codigo_conteudo varchar NOT NULL,
	CONSTRAINT trilha_conteudo_avaliacao_conclusao_pk PRIMARY KEY (id)
);