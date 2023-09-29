CREATE OR REPLACE FUNCTION impulso_previne_dados_nominais.consulta_tabelas()
 RETURNS TABLE(municipio_id_sus character varying, id_registro character varying, tipo_registro character varying, data_registro text, chave_gestante character varying, gestante_nome character varying, gestante_data_de_nascimento date, gestante_documento_cpf character varying, gestante_documento_cns character varying, gestante_telefone character varying, data_dum date, idade_gestacional_atendimento integer, profissional_cns_atendimento character varying, profissional_nome_atendimento character varying, estabelecimento_cnes_atendimento character varying, estabelecimento_nome_atendimento character varying, equipe_ine_atendimento character varying, equipe_nome_atendimento character varying, data_ultimo_cadastro_individual date, estabelecimento_cnes_cad_indivual character varying, estabelecimento_nome_cad_individual character varying, equipe_ine_cad_individual character varying, equipe_nome_cad_individual character varying, data_ultima_visita_acs date, acs_visita_domiciliar character varying, acs_cad_dom_familia character varying, acs_cad_individual character varying, criacao_data timestamp with time zone, atualizacao_data timestamp with time zone)
 LANGUAGE plpgsql
AS $function$
DECLARE
    r record;
begin
    FOR r IN 
    	SELECT table_schema, table_name 
    	FROM configuracoes.municipios_transmissoes_ativas
		where table_name = 'lista_nominal_gestantes'
	LOOP
        RETURN QUERY EXECUTE format('SELECT *
							FROM %s.%s ',r.table_schema,r.table_name);
    END LOOP;
END;
$function$
;