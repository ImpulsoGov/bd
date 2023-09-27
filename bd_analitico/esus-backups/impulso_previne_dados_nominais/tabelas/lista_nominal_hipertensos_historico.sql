-- impulso_previne_dados_nominais.lista_nominal_hipertensos_historico definition

-- Drop table

-- DROP TABLE impulso_previne_dados_nominais.lista_nominal_hipertensos_historico;

CREATE TABLE impulso_previne_dados_nominais.lista_nominal_hipertensos_historico (
	municipio_id_sus varchar(6) NULL,
	quadrimestre_atual text NULL,
	realizou_afericao_ultimos_6_meses bool NULL,
	dt_afericao_pressao_mais_recente varchar(11) NULL,
	realizou_consulta_ultimos_6_meses bool NULL,
	dt_consulta_mais_recente date NULL,
	co_seq_fat_cidadao_pec int8 NULL,
	cidadao_cpf varchar(11) NULL,
	cidadao_cns varchar(15) NULL,
	cidadao_nome varchar(500) NULL,
	cidadao_nome_social varchar(500) NULL,
	cidadao_sexo text NULL,
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
	possui_hipertensao_autorreferida bool NULL,
	possui_hipertensao_diagnosticada bool NULL,
	data_ultimo_cadastro date NULL,
	dt_ultima_consulta date NULL,
	se_faleceu int4 NULL,
	se_mudou int4 NULL,
	periodo_data_transmissao date NULL,
	atualizacao_data timestamptz NULL DEFAULT CURRENT_DATE
);