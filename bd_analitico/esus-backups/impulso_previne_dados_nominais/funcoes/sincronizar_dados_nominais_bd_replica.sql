CREATE OR REPLACE PROCEDURE impulso_previne_dados_nominais.sincronizar_dados_nominais_bd_replica()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    tabela text;
begin
	CALL configuracoes.atualizar_tabela_municipios_transmissoes_ativas();
	call impulso_previne_dados_nominais.deduplicacao_cadastros();
	CALL impulso_previne_dados_nominais.atualizar_views_impulso_previne_dados_nominais();
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
	   	EXECUTE format(
        'TRUNCATE TABLE impulso_previne_dados_nominais_replica.%I;',tabela);
	    EXECUTE format(
	        'INSERT INTO impulso_previne_dados_nominais_replica.%I SELECT * FROM impulso_previne_dados_nominais.%I;',
	        tabela,
	        tabela
	    );
	    RESET ROLE;
    END LOOP;
END;
$procedure$
;
