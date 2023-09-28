-- impulso_previne.usuarios_cliques_ga4 definition

-- Drop table

-- DROP TABLE impulso_previne.usuarios_cliques_ga4;

CREATE TABLE impulso_previne.usuarios_cliques_ga4 (
	periodo_data_hora text NULL,
	usuario_id text NULL,
	usuario_id_alternativo text NULL,
	pagina_path text NULL,
	pagina_origem_url text NULL,
	clique_nome text NULL,
	pagina_clique_element text NULL,
	cidade_acesso text NULL,
	eventos int8 NULL,
	criacao_data text NULL,
	atualizacao_data text NULL,
	id uuid NULL DEFAULT gen_random_uuid()
);