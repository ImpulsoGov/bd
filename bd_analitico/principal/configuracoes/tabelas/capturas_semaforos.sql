-- configuracoes.capturas_semaforos definition

-- Drop table

-- DROP TABLE configuracoes.capturas_semaforos;

CREATE TABLE configuracoes.capturas_semaforos (
	periodo_id uuid NOT NULL,
	unidade_geografica_id uuid NOT NULL,
	tabela_destino text NOT NULL,
	data_inicio timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
	cliente_nome text NULL,
	cliente_ipv4 varchar(15) NULL
);
CREATE UNIQUE INDEX capturas_semaforos_periodo_id_idx ON configuracoes.capturas_semaforos USING btree (periodo_id, unidade_geografica_id, tabela_destino);