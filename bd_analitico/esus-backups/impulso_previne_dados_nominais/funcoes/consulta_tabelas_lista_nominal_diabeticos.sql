CREATE OR REPLACE FUNCTION impulso_previne_dados_nominais.consulta_tabelas_lista_nominal_diabeticos()
 RETURNS TABLE(municipio_id_sus character varying, quadrimestre_atual text, realizou_solicitacao_hemoglobina_ultimos_6_meses boolean, dt_solicitacao_hemoglobina_glicada_mais_recente character varying, realizou_consulta_ultimos_6_meses boolean, dt_consulta_mais_recente date, co_seq_fat_cidadao_pec text, cidadao_cpf character varying, cidadao_cns character varying, cidadao_nome character varying, cidadao_nome_social character varying, cidadao_sexo text, dt_nascimento date, estabelecimento_cnes_atendimento text, estabelecimento_cnes_cadastro text, estabelecimento_nome_atendimento text, estabelecimento_nome_cadastro text, equipe_ine_atendimento text, equipe_ine_cadastro text, equipe_ine_procedimento text, equipe_nome_atendimento text, equipe_nome_cadastro text, equipe_nome_procedimento text, acs_nome_cadastro text, acs_nome_visita text, profissional_nome_atendimento text, profissional_nome_procedimento text, possui_diabetes_autoreferida boolean, possui_diabetes_diagnosticada boolean, data_ultimo_cadastro date, dt_ultima_consulta date, se_faleceu integer, se_mudou integer, criacao_data timestamp with time zone, atualizacao_data timestamp with time zone)
 LANGUAGE plpgsql
AS $function$
DECLARE
    r record;
begin
    FOR r IN 
    	SELECT table_schema, table_name 
    	FROM configuracoes.municipios_transmissoes_ativas
		where table_name = 'lista_nominal_diabeticos'
	LOOP
        RETURN QUERY EXECUTE format('
		SELECT 
		municipio_id_sus, 
		quadrimestre_atual, 
		realizou_solicitacao_hemoglobina_ultimos_6_meses, 
		dt_solicitacao_hemoglobina_glicada_mais_recente, 
		realizou_consulta_ultimos_6_meses, 
		dt_consulta_mais_recente, 
		co_seq_fat_cidadao_pec::text, 
		cidadao_cpf, 
		cidadao_cns, 
		cidadao_nome, 
		cidadao_nome_social, 
		cidadao_sexo::text, 
		dt_nascimento::date, 
		estabelecimento_cnes_atendimento, 
		estabelecimento_cnes_cadastro, 
		estabelecimento_nome_atendimento, 
		estabelecimento_nome_cadastro, 
		equipe_ine_atendimento, 
		equipe_ine_cadastro, 
		equipe_ine_procedimento,
		equipe_nome_atendimento, 
		equipe_nome_cadastro, 
		equipe_nome_procedimento,
		acs_nome_cadastro, 
		acs_nome_visita, 
		profissional_nome_atendimento,
		profissional_nome_procedimento,
		possui_diabetes_autoreferida, 
		possui_diabetes_diagnosticada, 
		data_ultimo_cadastro, 
		dt_ultima_consulta, 
		se_faleceu,
		se_mudou, 
		criacao_data, 
		atualizacao_data
		FROM %s.%s ',r.table_schema,r.table_name);
    END LOOP;
END;
$function$
;
