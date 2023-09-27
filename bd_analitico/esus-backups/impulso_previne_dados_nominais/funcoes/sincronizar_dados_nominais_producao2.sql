CREATE OR REPLACE PROCEDURE impulso_previne_dados_nominais.sincronizar_dados_nominais_producao2()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    tabela text;
begin
	DROP SCHEMA IF EXISTS _impulso_previne_dados_nominais_producao CASCADE;
	CREATE SCHEMA _impulso_previne_dados_nominais_producao;
	IMPORT FOREIGN SCHEMA impulso_previne_dados_nominais 
	limit to (
				painel_diabeticos,
				painel_diabeticos_lista_nominal,
				painel_gestantes,
				painel_gestantes_lista_nominal,
				painel_gestantes_historico_atendimentos,
				painel_hipertensos,
				painel_hipertensos_lista_nominal,
				painel_citopatologico_lista_nominal)
	FROM SERVER impulso_previne_producao INTO _impulso_previne_dados_nominais_producao;
	--CALL configuracoes.atualizar_tabela_municipios_transmissoes_ativas();
	--CALL impulso_previne_dados_nominais.registrar_historico_dados_nominais();
	--CALL impulso_previne_dados_nominais.atualizar_views_impulso_previne_dados_nominais();
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
