CREATE OR REPLACE PROCEDURE impulso_previne.sincronizar_tabela_caracterizacao_municipal_resumo()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    tabela text;
BEGIN
    FOR tabela IN 
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = '_impulso_previne_producao'
        and table_name like any (array['caracterizacao_municipal_resumo%'])
    LOOP
	    SET ROLE postgres;
	   refresh materialized view impulso_previne.caracterizacao_municipal_resumo;
	   	EXECUTE format('DELETE FROM "_impulso_previne_producao".%I',tabela);
	    EXECUTE format(
	        'INSERT INTO _impulso_previne_producao.%I SELECT 
				* FROM impulso_previne.%I;',
				tabela,
	        tabela
	    );
	    RESET ROLE;
    END LOOP;
END;
$procedure$
;
