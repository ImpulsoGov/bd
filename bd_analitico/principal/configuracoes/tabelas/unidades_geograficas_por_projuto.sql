-- configuracoes.unidades_geograficas_por_projuto definition

-- Drop table

-- DROP TABLE configuracoes.unidades_geograficas_por_projuto;

CREATE TABLE configuracoes.unidades_geograficas_por_projuto (
	unidade_geografica_id uuid NOT NULL,
	projuto text NOT NULL,
	ativa bool NOT NULL DEFAULT true,
	CONSTRAINT unidades_geograficas_por_projuto_pk PRIMARY KEY (unidade_geografica_id, projuto)
);
CREATE INDEX unidades_geograficas_por_projuto_projuto_idx ON configuracoes.unidades_geograficas_por_projuto USING btree (projuto, unidade_geografica_id);


-- configuracoes.unidades_geograficas_por_projuto foreign keys

ALTER TABLE configuracoes.unidades_geograficas_por_projuto ADD CONSTRAINT unidades_geograficas_por_projuto_fk FOREIGN KEY (unidade_geografica_id) REFERENCES listas_de_codigos.unidades_geograficas(id);