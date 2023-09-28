-- impulso_previne.usuarios_acessos_ga4 definition

-- Drop table

-- DROP TABLE impulso_previne.usuarios_acessos_ga4;

CREATE TABLE impulso_previne.usuarios_acessos_ga4 (
	id uuid NOT NULL DEFAULT gen_random_uuid(),
	periodo_data_hora varchar NOT NULL,
	usuario_id text NULL,
	usuario_id_alternativo text NULL,
	cidade_acesso text NULL,
	pagina_path text NULL,
	pagina_url text NULL,
	usuarios_ativos int4 NULL,
	eventos int4 NULL,
	sessoes int4 NULL,
	sessoes_engajadas int4 NULL,
	taxa_engajamento float4 NULL,
	visualizacoes int4 NULL,
	sessao_duracao float8 NULL,
	sessao_duracao_media float8 NULL,
	data_inicio_relatorio date NULL DEFAULT '2022-11-01'::date,
	criacao_data timestamptz NULL,
	atualizacao_data timestamptz NULL DEFAULT CURRENT_TIMESTAMP
);