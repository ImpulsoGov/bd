"""DDL de criação da tabela que recebe os dados da transmissão da lista de diabéticos"""

--DROP TABLE IF EXISTS dados_ip_impulsolandia.lista_nominal_diabeticos;
CREATE TABLE dados_ip_impulsolandia.lista_nominal_diabeticos (
	municipio_id_sus varchar(6) NULL DEFAULT 111111,
	quadrimestre_atual text NULL,
	realizou_solicitacao_hemoglobina_ultimos_6_meses bool NULL,
	dt_solicitacao_hemoglobina_glicada_mais_recente varchar(11) NULL,
	realizou_consulta_ultimos_6_meses bool NULL,
	dt_consulta_mais_recente date NULL,
	co_seq_fat_cidadao_pec int8 NULL,
	cidadao_cpf varchar(11) NULL,
	cidadao_cns varchar(15) NULL,
	cidadao_nome varchar(500) NULL,
	cidadao_nome_social varchar(500) NULL,
	cidadao_sexo varchar(20) NULL,
	dt_nascimento date NULL,
	estabelecimento_cnes_atendimento text NULL,
	estabelecimento_cnes_cadastro text NULL,
	estabelecimento_nome_atendimento text NULL,
	estabelecimento_nome_cadastro text NULL,
	equipe_ine_atendimento text NULL,
	equipe_ine_cadastro text NULL,
	equipe_nome_atendimento text NULL,
	equipe_nome_cadastro text NULL,
	acs_nome_cadastro text NULL,
	acs_nome_visita text NULL,
	possui_diabetes_autoreferida bool NULL,
	possui_diabetes_diagnosticada bool NULL,
	data_ultimo_cadastro date NULL,
	dt_ultima_consulta date NULL,
	se_faleceu int4 NULL,
	se_mudou int4 NULL,
	criacao_data timestamptz NULL,
	atualizacao_data timestamptz NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX lista_nominal_diabeticos_criacao_data_idx ON dados_ip_impulsolandia.lista_nominal_diabeticos USING btree (criacao_data);
CREATE INDEX lista_nominal_diabeticos_municipio_id_sus_idx ON dados_ip_impulsolandia.lista_nominal_diabeticos USING btree (municipio_id_sus);
