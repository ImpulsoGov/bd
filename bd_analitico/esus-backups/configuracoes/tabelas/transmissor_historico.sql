-- configuracoes.transmissor_historico definition

-- Drop table

-- DROP TABLE configuracoes.transmissor_historico;

CREATE TABLE configuracoes.transmissor_historico (
	execucao_data_hora timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
	municipio_id_sus varchar NULL,
	mensagem text NULL,
	registros int8 NULL,
	projuto_nome text NULL,
	tabela_nome text NULL,
	erro_contexto text NULL,
	erro_transmitido bool NULL
);