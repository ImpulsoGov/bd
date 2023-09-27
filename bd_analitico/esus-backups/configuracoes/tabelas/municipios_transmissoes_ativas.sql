-- configuracoes.municipios_transmissoes_ativas definition

-- Drop table

-- DROP TABLE configuracoes.municipios_transmissoes_ativas;

CREATE TABLE configuracoes.municipios_transmissoes_ativas (
	table_name text NULL,
	table_schema text NULL,
	municipio_id_sus bpchar(6) NULL,
	primeira_transmissao date NULL
);