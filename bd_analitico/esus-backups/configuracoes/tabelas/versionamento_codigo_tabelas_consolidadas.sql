-- configuracoes.versionamento_codigo_tabelas_consolidadas definition

-- Drop table

-- DROP TABLE configuracoes.versionamento_codigo_tabelas_consolidadas;

CREATE TABLE configuracoes.versionamento_codigo_tabelas_consolidadas (
	id uuid NULL DEFAULT gen_random_uuid(),
	codigo text NULL,
	versao_ordem int4 NULL,
	versao_descricao text NULL,
	versao_data date NULL,
	tabela_destino varchar(500) NULL,
	parametro_ativo bool NULL,
	painel_nome text NULL,
	criacao_data date NULL DEFAULT CURRENT_DATE
);