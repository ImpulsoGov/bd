-- configuracoes.capturas_erros_etl definition

-- Drop table

-- DROP TABLE configuracoes.capturas_erros_etl;

CREATE TABLE configuracoes.capturas_erros_etl (
	id uuid NOT NULL DEFAULT uuid_generate_v7(),
	operacao_id uuid NOT NULL,
	periodo_id uuid NOT NULL,
	unidade_geografica_id uuid NOT NULL,
	erro_mensagem text NOT NULL,
	erro_traceback text NOT NULL,
	criacao_data timestamp NULL DEFAULT CURRENT_TIMESTAMP,
	atualizacao_data timestamp NULL DEFAULT CURRENT_TIMESTAMP
);