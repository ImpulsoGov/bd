-- impulso_previne.alertas_dados_agendamentos_etl_notificados definition

-- Drop table

-- DROP TABLE impulso_previne.alertas_dados_agendamentos_etl_notificados;

CREATE TABLE impulso_previne.alertas_dados_agendamentos_etl_notificados (
	etl_nome text NOT NULL,
	tabela_destino text NOT NULL,
	periodo_codigo varchar(15) NOT NULL,
	atualizacao_retroativa bool NOT NULL
);