-- impulso_previne.usuarios definition

-- Drop table

-- DROP TABLE impulso_previne.usuarios;

CREATE TABLE impulso_previne.usuarios (
	id uuid NOT NULL,
	nome_usuario varchar NOT NULL,
	hash_senha varchar NULL,
	mail varchar NOT NULL,
	cpf varchar NOT NULL,
	criacao_data date NOT NULL,
	atualizacao_data date NOT NULL,
	perfil_ativo bool NULL,
	CONSTRAINT usuarios_pkey PRIMARY KEY (id),
	CONSTRAINT usuarios_un UNIQUE (mail, cpf)
);