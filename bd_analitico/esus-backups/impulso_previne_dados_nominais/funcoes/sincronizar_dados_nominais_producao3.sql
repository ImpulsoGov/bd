CREATE OR REPLACE PROCEDURE impulso_previne_dados_nominais.sincronizar_dados_nominais_producao3()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    tabela text;
begin
	DROP SCHEMA IF EXISTS _impulso_previne_dados_nominais_producao CASCADE;
	CREATE SCHEMA _impulso_previne_dados_nominais_producao;
	IMPORT FOREIGN SCHEMA impulso_previne_dados_nominais 
	limit to (	painel_enfermeiras_lista_nominal_diabeticos,
				painel_enfermeiras_lista_nominal_gestantes,
				painel_coordenadores_lista_nominal_gestantes,
				painel_enfermeiras_lista_nominal_hipertensos,
				painel_citopatologico_lista_nominal)
	FROM SERVER impulso_previne_producao INTO _impulso_previne_dados_nominais_producao;
    FOR tabela IN 
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = '_impulso_previne_dados_nominais_producao'
    LOOP
	    SET ROLE postgres;
	   	EXECUTE format('DELETE FROM "_impulso_previne_dados_nominais_producao".%I',tabela);
	    EXECUTE format(
	        'INSERT INTO _impulso_previne_dados_nominais_producao.%I SELECT * FROM impulso_previne_dados_nominais.%I;',
	        tabela,
	        tabela
	    );
	    RESET ROLE;
    END LOOP;
END;
$procedure$
;
