CREATE OR REPLACE PROCEDURE impulso_previne_dados_nominais.sincronizar_dados_nominais_bd_producao()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    tabela text;
begin
    FOR tabela IN 
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = '_impulso_previne_dados_nominais_bd_producao'
        and table_name in ('painel_cadastros_gestantes_duplicadas'
							'painel_citopatologico_lista_nominal'
							'painel_enfermeiras_lista_nominal_diabeticos'
							'painel_enfermeiras_lista_nominal_hipertensos'
							'painel_gestantes_lista_nominal'
							'painel_vacinacao_lista_nominal')        
    LOOP
	    SET ROLE postgres;
	   	EXECUTE format('DELETE FROM "_impulso_previne_dados_nominais_bd_producao".%I',tabela);
	    EXECUTE format(
	        'INSERT INTO _impulso_previne_dados_nominais_bd_producao.%I SELECT * FROM impulso_previne_dados_nominais_replica.%I;',
	        tabela,
	        tabela
	    );
	    RESET ROLE;
    END LOOP;
END;
$procedure$
;
