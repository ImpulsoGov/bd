CREATE OR REPLACE PROCEDURE configuracoes.remover_gatilho_gravacao_historico(tabela text)
 LANGUAGE plpgsql
AS $procedure$
	BEGIN
	    execute 'drop trigger if exists gravar_historico on ' || tabela || ';';
	END;
$procedure$
;
