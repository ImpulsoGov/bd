-- impulso_previne."_usuarios_acessos_ga4" definition

-- Drop table

-- DROP TABLE impulso_previne."_usuarios_acessos_ga4";

CREATE TABLE impulso_previne."_usuarios_acessos_ga4" (
	id uuid NOT NULL DEFAULT gen_random_uuid(),
	periodo_data_hora varchar NOT NULL,
	usuario_id text NULL,
	usuario_municipio text NULL,
	usuario_equipe_ine varchar(10) NULL,
	usuario_cargo text NULL,
	cidade_acesso text NULL,
	pagina_path text NULL,
	pagina_referrer text NULL,
	usuarios_ativos int4 NULL,
	novos_usuarios int4 NULL,
	periodo_data_primeira_sessao varchar NULL,
	eventos int4 NULL,
	sessoes_engajadas int4 NULL,
	taxa_engajamento float4 NULL,
	visualizacoes int4 NULL,
	sessao_duracao int8 NULL,
	sessao_duracao_media float8 NULL,
	dau_per_mau float8 NULL,
	dau_per_wau float8 NULL,
	data_inicio_relatorio date NULL DEFAULT '2022-11-01'::date,
	criacao_data timestamptz NULL,
	atualizacao_data timestamptz NULL DEFAULT CURRENT_TIMESTAMP
);