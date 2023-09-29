CREATE OR REPLACE FUNCTION impulso_previne_dados_nominais.unir_tabelas_lista_nominal_vacinacao()
 RETURNS TABLE(municipio_id_sus character varying, chave_cidadao character varying, cidadao_nome character varying, cidadao_cpf character varying, cidadao_cns character varying, cidadao_sexo character varying, dt_nascimento date, cidadao_nome_responsavel character varying, cidadao_cns_responsavel character varying, cidadao_cpf_responsavel character varying, cidadao_idade_meses_atual integer, cidadao_idade_meses_inicio_quadri integer, cidadao_idade_meses_fim_quadri integer, se_faleceu integer, co_seq_fat_vacinacao character varying, co_seq_fat_vacinacao_vacina character varying, tipo_ficha character varying, codigo_vacina character varying, nome_vacina character varying, dose_vacina character varying, data_registro_vacina date, estabelecimento_cnes_aplicacao_vacina character varying, estabelecimento_nome_aplicacao_vacina character varying, equipe_ine_aplicacao_vacina character varying, equipe_nome_aplicacao_vacina character varying, profissional_nome_aplicacao_vacina character varying, data_ultimo_cadastro_individual date, estabelecimento_cnes_cadastro character varying, estabelecimento_nome_cadastro character varying, equipe_ine_cadastro character varying, equipe_nome_cadastro character varying, acs_nome_cadastro character varying, estabelecimento_cnes_atendimento character varying, estabelecimento_nome_atendimento character varying, equipe_ine_atendimento character varying, equipe_nome_atendimento character varying, data_ultimo_atendimento_individual date, data_ultima_vista_domiciliar date, acs_nome_visita character varying, criacao_data timestamp with time zone)
 LANGUAGE plpgsql
AS $function$
DECLARE
    r RECORD;
BEGIN
    FOR r IN (
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema like 'dados_nominais%' AND
              table_name = 'lista_nominal_vacinacao'
    ) loop
	    BEGIN
        RETURN QUERY EXECUTE FORMAT('SELECT municipio_id_sus,chave_cidadao,cidadao_nome,cidadao_cpf,cidadao_cns,cidadao_sexo,dt_nascimento,cidadao_nome_responsavel,cidadao_cns_responsavel,cidadao_cpf_responsavel,
										cidadao_idade_meses_atual,cidadao_idade_meses_inicio_quadri,cidadao_idade_meses_fim_quadri,se_faleceu,co_seq_fat_vacinacao,co_seq_fat_vacinacao_vacina,
										tipo_ficha,codigo_vacina,nome_vacina,dose_vacina,data_registro_vacina,estabelecimento_cnes_aplicacao_vacina,estabelecimento_nome_aplicacao_vacina,
										equipe_ine_aplicacao_vacina,equipe_nome_aplicacao_vacina,profissional_nome_aplicacao_vacina,data_ultimo_cadastro_individual,estabelecimento_cnes_cadastro,
										estabelecimento_nome_cadastro,equipe_ine_cadastro,equipe_nome_cadastro, acs_nome_cadastro,estabelecimento_cnes_atendimento,estabelecimento_nome_atendimento,
										equipe_ine_atendimento,equipe_nome_atendimento,data_ultimo_atendimento_individual,data_ultima_vista_domiciliar,acs_nome_visita,criacao_data
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
