CREATE OR REPLACE PROCEDURE impulso_previne.sincronizar_tabelas_insights()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    tabela text;
BEGIN
    FOR tabela IN 
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = '_impulso_previne_producao'
        and table_name in ('indicadores_municipios_equipes_homologadas',
        					'insigths_financiamento_desempenho_isf','insigths_indicadores_equipes_validas_meta')
    LOOP
	    SET ROLE postgres;
	   call impulso_previne.atualizar_views_insights();
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
