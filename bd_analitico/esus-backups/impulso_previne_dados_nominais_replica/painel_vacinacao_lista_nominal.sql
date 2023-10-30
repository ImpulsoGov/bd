CREATE TABLE impulso_previne_dados_nominais.painel_vacinacao_lista_nominal (
	municipio_id_sus varchar(6) NULL,
	municipio_uf varchar(100) NULL,
	cidadao_nome varchar(500) NULL,
	cidadao_nome_responsavel varchar(500) NULL,
	cidadao_cpf_dt_nascimento varchar(30) NULL,
	cidadao_idade_meses int4 NULL,
	quadrimestre_completa_1_ano varchar(30) NULL,
	id_faixa_etaria int4 NULL,
	data_ou_prazo_1dose_polio date NULL,
	data_ou_prazo_2dose_polio date NULL,
	data_ou_prazo_3dose_polio date NULL,
	id_status_polio int4 NULL,
	data_ou_prazo_1dose_penta date NULL,
	data_ou_prazo_2dose_penta date NULL,
	data_ou_prazo_3dose_penta date NULL,
	id_status_penta int4 NULL,
	acs_nome varchar(500) NULL,
	equipe_ine varchar(20) NULL,
	equipe_nome varchar(500) NULL,
	criacao_data date NULL
	atualizacao_data date NULL,
	dt_registro_producao_mais_recente date NULL
);

CREATE INDEX painel_vacinacao_lista_nominal_municipio_id_sus_idx_01 ON impulso_previne_dados_nominais.painel_vacinacao_lista_nominal USING hash (municipio_uf);
CREATE INDEX painel_vacinacao_lista_nominal_municipio_uf_idx_02 ON impulso_previne_dados_nominais.painel_vacinacao_lista_nominal USING btree (municipio_uf, equipe_ine);