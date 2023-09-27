CREATE OR REPLACE PROCEDURE impulso_previne_dados_nominais.registrar_historico_lista_nominal_citopatologico()
 LANGUAGE plpgsql
AS $procedure$
	DECLARE
    	r record;
  	BEGIN
        FOR r IN (
		SELECT table_schema 
		FROM configuracoes.municipios_transmissoes_ativas
		where table_name = 'lista_nominal_citopatologico'
		) 
        loop
        	EXECUTE format(
        	'INSERT INTO impulso_previne_dados_nominais.lista_nominal_citopatologico_historico (municipio_id_sus, quadrimestre_atual, chave_mulher, paciente_nome, cidadao_cns, cidadao_cpf, paciente_idade_atual, dt_nascimento, dt_ultimo_exame, realizou_exame_ultimos_36_meses, data_projetada_proximo_exame, status_exame, data_limite_a_realizar_proximo_exame, cnes_estabelecimento_exame, nome_estabelecimento_exame, ine_equipe_exame, nome_equipe_exame, nome_profissional_exame, dt_ultimo_cadastro, estabelecimento_nome_cadastro, estabelecimento_cnes_cadastro, equipe_ine_cadastro, equipe_nome_cadastro, acs_nome_cadastro, dt_ultimo_atendimento, estabelecimento_nome_ultimo_atendimento, estabelecimento_cnes_ultimo_atendimento, equipe_ine_ultimo_atendimento, equipe_nome_ultimo_atendimento, acs_nome_ultimo_atendimento, acs_nome_visita, criacao_data, atualizacao_data)
			(
			with municipios_ultimas_transmissoes as (
				SELECT 	
				tb2.municipio_id_sus,
				max(tb2.criacao_data) as  ultima_transmissao
				FROM impulso_previne_dados_nominais.lista_nominal_citopatologico_historico tb2 
				group by tb2.municipio_id_sus
			)	
			SELECT tb1.municipio_id_sus, quadrimestre_atual, chave_mulher, paciente_nome, cidadao_cns, cidadao_cpf, paciente_idade_atual, dt_nascimento, dt_ultimo_exame, realizou_exame_ultimos_36_meses, data_projetada_proximo_exame, status_exame, data_limite_a_realizar_proximo_exame, cnes_estabelecimento_exame, nome_estabelecimento_exame, ine_equipe_exame, nome_equipe_exame, nome_profissional_exame, dt_ultimo_cadastro, estabelecimento_nome_cadastro, estabelecimento_cnes_cadastro, equipe_ine_cadastro, equipe_nome_cadastro, acs_nome_cadastro, dt_ultimo_atendimento, estabelecimento_nome_ultimo_atendimento, estabelecimento_cnes_ultimo_atendimento, equipe_ine_ultimo_atendimento, equipe_nome_ultimo_atendimento, acs_nome_ultimo_atendimento, acs_nome_visita, criacao_data, atualizacao_data
       		FROM %s.lista_nominal_citopatologico tb1
          	 left join municipios_ultimas_transmissoes tb2 on tb1.municipio_id_sus = tb2.municipio_id_sus
			WHERE tb1.criacao_data::date > ( case when tb2.ultima_transmissao::date is null then current_date - ''1 day''::interval else tb2.ultima_transmissao::date end)
				)',r.table_schema);
        END loop;
        END;
$procedure$
;
