-- impulso_previne.alertas_erros_etl_notificados definition

-- Drop table

-- DROP TABLE impulso_previne.alertas_erros_etl_notificados;

CREATE TABLE impulso_previne.alertas_erros_etl_notificados (
	etl_nome text NOT NULL,
	periodo_codigo varchar(15) NOT NULL,
	erro_mensagem text NOT NULL,
	erro_traceback text NOT NULL,
	atualizacao_data timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	criacao_data timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
);