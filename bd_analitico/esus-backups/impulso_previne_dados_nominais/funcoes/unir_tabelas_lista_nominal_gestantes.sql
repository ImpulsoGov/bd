CREATE OR REPLACE FUNCTION impulso_previne_dados_nominais.unir_tabelas_lista_nominal_gestantes()
 RETURNS TABLE(municipio_id_sus character varying, id_registro character varying, tipo_registro character varying, data_registro date, chave_gestante character varying, gestante_nome character varying, gestante_data_de_nascimento date, gestante_documento_cpf character varying, gestante_documento_cns character varying, gestante_telefone character varying, data_dum date, idade_gestacional_atendimento integer, profissional_cns_atendimento character varying, profissional_nome_atendimento character varying, estabelecimento_cnes_atendimento character varying, estabelecimento_nome_atendimento character varying, equipe_ine_atendimento character varying, equipe_nome_atendimento character varying, data_ultimo_cadastro_individual date, estabelecimento_cnes_cad_indivual character varying, estabelecimento_nome_cad_individual character varying, equipe_ine_cad_individual character varying, equipe_nome_cad_individual character varying, data_ultima_visita_acs date, acs_visita_domiciliar character varying, acs_cad_dom_familia character varying, acs_cad_individual character varying, criacao_data timestamp with time zone, atualizacao_data timestamp with time zone)
 LANGUAGE plpgsql
AS $function$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema like 'dados_nominais%' AND
              table_name = 'lista_nominal_gestantes'
    ) loop
	    BEGIN
        RETURN QUERY EXECUTE FORMAT('SELECT municipio_id_sus, id_registro, tipo_registro, data_registro, chave_gestante, gestante_nome, gestante_data_de_nascimento, 
										gestante_documento_cpf, gestante_documento_cns, gestante_telefone, data_dum, idade_gestacional_atendimento, 
										profissional_cns_atendimento, profissional_nome_atendimento, estabelecimento_cnes_atendimento, estabelecimento_nome_atendimento, 
										equipe_ine_atendimento, equipe_nome_atendimento, data_ultimo_cadastro_individual, estabelecimento_cnes_cad_indivual, 
										estabelecimento_nome_cad_individual, equipe_ine_cad_individual, equipe_nome_cad_individual, data_ultima_visita_acs, 
										acs_visita_domiciliar, acs_cad_dom_familia, acs_cad_individual, criacao_data, atualizacao_data 
									FROM %I.%I', r.table_schema, r.table_name);
		EXCEPTION
            WHEN OTHERS THEN
                -- Log the error and continue processing other tables
                RAISE NOTICE 'Erro ao processar a tabela %I.%I: %', r.table_schema, r.table_name, SQLERRM;
        END;
    END LOOP;
    RETURN;
END;
$function$
;
