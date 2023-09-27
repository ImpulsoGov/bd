CREATE OR REPLACE FUNCTION impulso_previne_dados_nominais.consulta_tabelas_lista_nominal_citopatologico()
 RETURNS TABLE(municipio_id_sus character varying, quadrimestre_atual text, chave_mulher character varying, paciente_nome character varying, cidadao_cns character varying, cidadao_cpf character varying, paciente_idade_atual bigint, dt_nascimento date, dt_ultimo_exame date, realizou_exame_ultimos_36_meses boolean, data_projetada_proximo_exame date, status_exame character varying, data_limite_a_realizar_proximo_exame date, cnes_estabelecimento_exame character varying, nome_estabelecimento_exame character varying, ine_equipe_exame character varying, nome_equipe_exame character varying, nome_profissional_exame character varying, dt_ultimo_cadastro date, estabelecimento_nome_cadastro character varying, estabelecimento_cnes_cadastro character varying, equipe_ine_cadastro character varying, equipe_nome_cadastro character varying, acs_nome_cadastro character varying, dt_ultimo_atendimento date, estabelecimento_nome_ultimo_atendimento character varying, estabelecimento_cnes_ultimo_atendimento character varying, equipe_ine_ultimo_atendimento character varying, equipe_nome_ultimo_atendimento character varying, acs_nome_ultimo_atendimento character varying, acs_nome_visita character varying, criacao_data timestamp with time zone, atualizacao_data timestamp with time zone)
 LANGUAGE plpgsql
AS $function$
DECLARE
    r record;
begin
    FOR r IN 
    	SELECT table_schema, table_name 
    	FROM configuracoes.municipios_transmissoes_ativas
		where table_name = 'lista_nominal_citopatologico'
	LOOP
        RETURN QUERY EXECUTE format('
		SELECT 
		municipio_id_sus, 
		quadrimestre_atual, 
		chave_mulher, 
		paciente_nome, 
		cidadao_cns, 
		cidadao_cpf, 
		paciente_idade_atual, 
		dt_nascimento, 
		dt_ultimo_exame, 
		realizou_exame_ultimos_36_meses, 
		data_projetada_proximo_exame, 
		status_exame, 
		data_limite_a_realizar_proximo_exame, 
		cnes_estabelecimento_exame, 
		nome_estabelecimento_exame, 
		ine_equipe_exame, 
		nome_equipe_exame, 
		nome_profissional_exame, 
		dt_ultimo_cadastro, 
		estabelecimento_nome_cadastro, 
		estabelecimento_cnes_cadastro, 
		equipe_ine_cadastro, equipe_nome_cadastro, 
		acs_nome_cadastro, dt_ultimo_atendimento, 
		estabelecimento_nome_ultimo_atendimento, 
		estabelecimento_cnes_ultimo_atendimento, 
		equipe_ine_ultimo_atendimento, 
		equipe_nome_ultimo_atendimento, 
		acs_nome_ultimo_atendimento, 
		acs_nome_visita, 
		criacao_data, 
		atualizacao_data
		FROM %s.%s ',r.table_schema,r.table_name);
    END LOOP;
END;
$function$
;
