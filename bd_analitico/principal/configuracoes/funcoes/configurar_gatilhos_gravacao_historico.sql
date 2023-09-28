CREATE OR REPLACE FUNCTION configuracoes.configurar_gatilhos_gravacao_historico()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
	-- Para cada operação de captura/ETL descrita na tabela `configuracoes.capturas_operacoes`,
	-- se esta for a única operação para uma dada tabela de destino,  garante que a tabela de destino tenha um gatilho
	-- que grave o histórico de inserções
    BEGIN
        IF (TG_OP = 'INSERT') then 
        	if ((
        		select count(distinct op.id)
        		from configuracoes.capturas_operacoes op
        		where op.tabela_destino = NEW.tabela_destino
        	) = 1) then 
            	execute 
            		'CREATE TRIGGER gravar_historico AFTER INSERT on '
            		|| NEW.tabela_destino
            		|| ' FOR EACH ROW EXECUTE PROCEDURE dados_publicos.gravar_historico();'
            	;
            else
            	drop trigger if exists gravar_historico on NEW.tabela_destino;
            end if;
            RETURN NEW;
        elsif (tg_op = 'DELETE') then
        	if ((
        		select count(distinct op.id)
        		from configuracoes.capturas_operacoes op
        		where op.tabela_destino = OLD.tabela_destino
        	) = 1) then 
            	execute 
            		'CREATE TRIGGER gravar_historico AFTER INSERT on '
            		|| NEW.tabela_destino
            		|| ' FOR EACH ROW EXECUTE PROCEDURE dados_publicos.gravar_historico();'
            	;
            else
            	drop trigger if exists gravar_historico on OLD.tabela_destino;
            end if;
            RETURN OLD;
        END IF;
        RETURN NULL; -- result is ignored since this is an AFTER trigger
    END;
$function$
;
