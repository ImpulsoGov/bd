-- configuracoes.historico_notificacoes_realizadas definition

-- Drop table

-- DROP TABLE configuracoes.historico_notificacoes_realizadas;

CREATE TABLE configuracoes.historico_notificacoes_realizadas (
	id uuid NOT NULL DEFAULT gen_random_uuid(),
	notificacao_data_hora timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
	municipio_id_sus varchar(6) NOT NULL,
	lista_nominal text NOT NULL,
	ultima_transmissao timestamptz NOT NULL,
	transmissao_atrasao_dias text NOT NULL,
	notificacao_tipo varchar(50) NOT NULL,
	criacao_data timestamptz NULL,
	atualizacao_data timestamptz NULL DEFAULT CURRENT_TIMESTAMP,
	CONSTRAINT historico_notificacoes_realizadas_pkey PRIMARY KEY (notificacao_data_hora, municipio_id_sus, lista_nominal, notificacao_tipo)
);