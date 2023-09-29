CREATE OR REPLACE PROCEDURE impulso_previne.sincronizar_tabelas_capitacao_ponderada_validacao()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    tabela text;
BEGIN
    FOR tabela IN 
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = '_impulso_previne_producao'
        and table_name like 'capitacao_ponderada_validacao%'
    LOOP
	    SET ROLE postgres;
	   call impulso_previne.atualizar_views_capitacao_ponderada_validacao();
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