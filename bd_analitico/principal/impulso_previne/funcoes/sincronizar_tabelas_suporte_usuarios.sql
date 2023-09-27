CREATE OR REPLACE PROCEDURE impulso_previne.sincronizar_tabelas_suporte_usuarios()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    tabela text;
begin
		DROP SCHEMA IF EXISTS _suporte_producao CASCADE;
		create schema _suporte_producao;
		IMPORT FOREIGN SCHEMA suporte LIMIT TO (usuarios,usuarios_ip)
		FROM SERVER api INTO _suporte_producao;
		IMPORT FOREIGN SCHEMA impulso_previne LIMIT TO (trilha_conteudo_avaliacao_conclusao,nps)
		FROM SERVER api INTO _suporte_producao;
    FOR tabela IN 
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = '_suporte_producao'
        and table_name in ('usuarios',
        					'usuarios_ip','trilha_conteudo_avaliacao_conclusao','nps')
    LOOP
	    SET ROLE postgres;
	   	EXECUTE format('DELETE FROM "impulso_previne".%I',tabela);
	    EXECUTE format(
	        'INSERT INTO impulso_previne.%I SELECT * FROM _suporte_producao.%I;',
	        tabela,
	        tabela
	    );
	    RESET ROLE;
    END LOOP;
END;
$procedure$
;
