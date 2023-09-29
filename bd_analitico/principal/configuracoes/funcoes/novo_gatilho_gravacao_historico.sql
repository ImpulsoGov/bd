CREATE OR REPLACE PROCEDURE configuracoes.novo_gatilho_gravacao_historico(tabela text)
 LANGUAGE plpgsql
AS $procedure$
	BEGIN
	    execute 
			'CREATE TRIGGER gravar_historico AFTER INSERT on '
	    	|| tabela ||
	    	' FOR EACH ROW EXECUTE PROCEDURE dados_publicos.gravar_historico();'
		;
	END;
$procedure$
;
