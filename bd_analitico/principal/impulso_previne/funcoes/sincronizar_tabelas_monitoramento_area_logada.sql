CREATE OR REPLACE PROCEDURE impulso_previne.sincronizar_tabelas_monitoramento_area_logada()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    tabela text;
begin
    SET ROLE postgres;
  	call impulso_previne.sincronizar_tabelas_suporte_usuarios();
   	call impulso_previne.atualizar_views_area_logada();
    FOR tabela IN 
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = '_impulso_previne_analises'
    LOOP
	   	EXECUTE format('DELETE FROM "_impulso_previne_analises".%I',tabela);
	    EXECUTE format(
	        'INSERT INTO _impulso_previne_analises.%I SELECT * FROM impulso_previne.%I;',
	        tabela,
	        tabela
	    );
    END LOOP;
   	RESET ROLE;
END;
$procedure$
;
