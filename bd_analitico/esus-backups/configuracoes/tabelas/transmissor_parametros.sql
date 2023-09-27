-- configuracoes.transmissor_parametros definition

-- Drop table

-- DROP TABLE configuracoes.transmissor_parametros;

CREATE TABLE configuracoes.transmissor_parametros (
	id uuid NOT NULL DEFAULT gen_random_uuid(),
	projuto_nome text NOT NULL,
	view_codigo text NOT NULL,
	view_versao int4 NOT NULL,
	view_versao_descricao text NOT NULL,
	tabela_campos text NOT NULL,
	tabela_nome text NOT NULL,
	parametro_ativo bool NOT NULL,
	view_versao_data date NULL DEFAULT CURRENT_DATE,
	CONSTRAINT transmissor_parametros_pkey PRIMARY KEY (id)
);