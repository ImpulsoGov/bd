CREATE OR REPLACE PROCEDURE impulso_previne_dados_nominais.sincronizar_relatorio_transmissao()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    tabela text;
begin
	CALL impulso_previne_dados_nominais.atualizar_views_impulso_previne_dados_nominais();
	CALL impulso_previne_dados_nominais.atualizar_views_relatorio_transmissao();
    FOR tabela IN 
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = '_impulso_previne_dados_nominais'
    LOOP
	    SET ROLE postgres;
	   	EXECUTE format('DELETE FROM "_impulso_previne_dados_nominais".%I',tabela);
	    EXECUTE format(
	        'INSERT INTO _impulso_previne_dados_nominais.%I SELECT * FROM impulso_previne_dados_nominais.%I;',
	        tabela,
	        tabela
	    );
	    RESET ROLE;
    END LOOP;
END;
$procedure$
;
