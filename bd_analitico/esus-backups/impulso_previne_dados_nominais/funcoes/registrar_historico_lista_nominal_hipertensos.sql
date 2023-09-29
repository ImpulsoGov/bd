CREATE OR REPLACE PROCEDURE impulso_previne_dados_nominais.registrar_historico_lista_nominal_hipertensos()
 LANGUAGE plpgsql
AS $procedure$
	DECLARE
    	r record;
  	BEGIN
        FOR r IN (
		SELECT table_schema 
		FROM configuracoes.municipios_transmissoes_ativas
		where table_name = 'lista_nominal_hipertensos'
	    ) 
        loop
        	EXECUTE format(
        	'INSERT INTO impulso_previne_dados_nominais.lista_nominal_hipertensos_historico (municipio_id_sus, quadrimestre_atual, realizou_afericao_ultimos_6_meses, dt_afericao_pressao_mais_recente, realizou_consulta_ultimos_6_meses, dt_consulta_mais_recente, co_seq_fat_cidadao_pec, cidadao_cpf, cidadao_cns, cidadao_nome, cidadao_nome_social, cidadao_sexo, dt_nascimento, estabelecimento_cnes_atendimento, estabelecimento_cnes_cadastro, estabelecimento_nome_atendimento, estabelecimento_nome_cadastro, equipe_ine_atendimento, equipe_ine_cadastro, equipe_nome_atendimento, equipe_nome_cadastro, acs_nome_cadastro, acs_nome_visita, possui_hipertensao_autorreferida, possui_hipertensao_diagnosticada, data_ultimo_cadastro, dt_ultima_consulta, se_faleceu, se_mudou,periodo_data_transmissao)
        	(
			with municipios_ultimas_transmissoes as (
				SELECT 	
				tb2.municipio_id_sus,
				max(tb2.periodo_data_transmissao) as  ultima_transmissao
				FROM impulso_previne_dados_nominais.lista_nominal_hipertensos_historico tb2 
				group by tb2.municipio_id_sus)	
			SELECT tb1.municipio_id_sus, quadrimestre_atual, realizou_afericao_ultimos_6_meses, dt_afericao_pressao_mais_recente, realizou_consulta_ultimos_6_meses, dt_consulta_mais_recente, co_seq_fat_cidadao_pec, cidadao_cpf, cidadao_cns, cidadao_nome, cidadao_nome_social, cidadao_sexo, dt_nascimento, estabelecimento_cnes_atendimento, estabelecimento_cnes_cadastro, estabelecimento_nome_atendimento, estabelecimento_nome_cadastro, equipe_ine_atendimento, equipe_ine_cadastro, equipe_nome_atendimento, equipe_nome_cadastro, acs_nome_cadastro, acs_nome_visita, possui_hipertensao_autorreferida, possui_hipertensao_diagnosticada, data_ultimo_cadastro, dt_ultima_consulta, se_faleceu, se_mudou, criacao_data 
       		FROM %s.lista_nominal_hipertensos tb1
          	 left join municipios_ultimas_transmissoes tb2 on tb1.municipio_id_sus = tb2.municipio_id_sus
			WHERE tb1.criacao_data::date > ( case when tb2.ultima_transmissao::date is null then current_date - ''1 day''::interval else tb2.ultima_transmissao::date end)
				)',r.table_schema);
        END loop;
        END;
$procedure$
;
