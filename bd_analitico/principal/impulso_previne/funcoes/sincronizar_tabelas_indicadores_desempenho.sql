CREATE OR REPLACE PROCEDURE impulso_previne.sincronizar_tabelas_indicadores_desempenho()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    tabela text;
BEGIN
    FOR tabela IN 
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = '_impulso_previne_producao'
        and table_name like 'indicadores_desempenho%'
    LOOP
	    SET ROLE postgres;
	   call impulso_previne.atualizar_views_indicadores_desempenho();
	   	EXECUTE format('DELETE FROM "_impulso_previne_producao".%I',tabela);
	    EXECUTE format(
	        'INSERT INTO _impulso_previne_producao.%I SELECT * FROM impulso_previne.%I;',
	        tabela,
	        tabela
	    );
	    RESET ROLE;
    END LOOP;
END;
$procedure$
;