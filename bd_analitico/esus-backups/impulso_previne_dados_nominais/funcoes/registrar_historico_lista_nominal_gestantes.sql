CREATE OR REPLACE PROCEDURE impulso_previne_dados_nominais.registrar_historico_lista_nominal_gestantes()
 LANGUAGE plpgsql
AS $procedure$
	DECLARE
    	r record;
  	BEGIN
        FOR r IN (
		SELECT table_schema 
		FROM configuracoes.municipios_transmissoes_ativas
		where table_name = 'lista_nominal_gestantes'
	    ) 
        loop
        	EXECUTE format(
        	'INSERT INTO impulso_previne_dados_nominais.lista_nominal_gestantes_historico (municipio_id_sus, chave_gestacao, ordem_gestacao, chave_gestante, gestante_telefone, gestante_nome, 
																							gestante_data_de_nascimento, estabelecimento_cnes, estabelecimento_nome, equipe_ine, equipe_nome, 
																							acs_nome, acs_data_ultima_visita, gestacao_data_dum, gestacao_data_dpp, gestacao_dpp_dias_para, 
																							gestacao_quadrimestre, gestacao_idade_gestacional_primeiro_atendimento, consulta_prenatal_primeira_data, 
																							consulta_prenatal_ultima_data, consulta_prenatal_ultima_dias_desde, data_fim_primeira_gestacao, tipo_encerramento_primeira_gestacao,
																							gestante_documento_cpf, gestante_documento_cns, gestacao_idade_gestacional_atual, sinalizacao_erro_registro, gestacao_qtde_dums, 
																							consultas_prenatal_total, consultas_pre_natal_validas, atendimento_odontologico_realizado, exame_hiv_realizado, 
																							exame_sifilis_realizado, possui_registro_aborto, possui_registro_parto, exame_sifilis_hiv_realizado, periodo_semana_transmissao, 
																							periodo_data_transmissao)

			(
			with municipios_ultimas_transmissoes as (
				SELECT 	
				tb2.municipio_id_sus,
				max(tb2.periodo_data_transmissao) as  ultima_transmissao
				FROM impulso_previne_dados_nominais.lista_nominal_gestantes_historico tb2 
				group by tb2.municipio_id_sus)	
			SELECT tb1.municipio_id_sus, chave_gestacao, ordem_gestacao, chave_gestante, gestante_telefone, gestante_nome, 
					gestante_data_de_nascimento, estabelecimento_cnes, estabelecimento_nome, equipe_ine, equipe_nome, 
					acs_nome, acs_data_ultima_visita, gestacao_data_dum, gestacao_data_dpp, gestacao_dpp_dias_para, 
					gestacao_quadrimestre, gestacao_idade_gestacional_primeiro_atendimento, consulta_prenatal_primeira_data, 
					consulta_prenatal_ultima_data, consulta_prenatal_ultima_dias_desde, data_fim_primeira_gestacao, tipo_encerramento_primeira_gestacao,
					gestante_documento_cpf, gestante_documento_cns, gestacao_idade_gestacional_atual, sinalizacao_erro_registro, gestacao_qtde_dums, 
					consultas_prenatal_total, consultas_pre_natal_validas, atendimento_odontologico_realizado, exame_hiv_realizado, 
					exame_sifilis_realizado, possui_registro_aborto, possui_registro_parto, exame_sifilis_hiv_realizado,
				extract(week from cast(criacao_data as date)),criacao_data 
				FROM %s.lista_nominal_gestantes tb1
				left join municipios_ultimas_transmissoes tb2 on tb1.municipio_id_sus = tb2.municipio_id_sus
				WHERE tb1.criacao_data::date > ( case when tb2.ultima_transmissao::date is null then tb1.criacao_data - ''1 day''::interval else tb2.ultima_transmissao::date end)
			)',r.table_schema);
        END loop;
        END;
$procedure$
;
