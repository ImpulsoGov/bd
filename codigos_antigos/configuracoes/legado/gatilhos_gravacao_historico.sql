CREATE OR REPLACE FUNCTION dados_publicos.gravar_historico()
RETURNS TRIGGER 
LANGUAGE plpgsql
AS $function$
    DECLARE
        captura_id uuid;
    BEGIN
        IF (TG_OP = 'INSERT') THEN
            INSERT INTO configuracoes.capturas_historico (
                id,
                operacao_id,
                periodo_id,
                unidade_geografica_id,
                parametros
            )
            SELECT DISTINCT ON (periodo_id, unidade_geografica_id)
                captura_id AS id,
				operacao.id,
				NEW.periodo_id,
				NEW.unidade_geografica_id,
				operacao.parametros
			FROM configuracoes.capturas_operacoes operacao
			WHERE operacao.tabela_destino = TG_TABLE_NAME AND operacao.ativa;
            RETURN NEW;
        END IF;
        RETURN NULL; -- result IS ignored since this IS an AFTER trigger
    END;
$function$
;


CREATE OR REPLACE FUNCTION
    configuracoes.configurar_gatilhos_gravacao_historico()
RETURNS TRIGGER AS $gatilho$
	-- Para cada operação de captura/ETL descrita na tabela `configuracoes.capturas_operacoes`,
	-- se esta for a única operação para uma dada tabela de destino,  garante que a tabela de destino tenha um gatilho
	-- que grave o histórico de inserções
    BEGIN
        IF (TG_OP = 'INSERT') THEN 
        	if ((
        		SELECT count(DISTINCT op.id)
        		FROM configuracoes.capturas_operacoes op
        		WHERE op.tabela_destino = NEW.tabela_destino
        	) = 1) THEN 
            	execute 
            		'CREATE TRIGGER gravar_historico AFTER INSERT ON '
            		|| NEW.tabela_destino
            		|| ' FOR EACH ROW EXECUTE PROCEDURE dados_publicos.gravar_historico();'
            	;
            ELSE
            	DROP trigger IF EXISTS gravar_historico ON NEW.tabela_destino;
            END if;
            RETURN NEW;
        ELSIF (tg_op = 'DELETE') THEN
        	IF ((
        		SELECT count(DISTINCT op.id)
        		FROM configuracoes.capturas_operacoes op
        		WHERE op.tabela_destino = OLD.tabela_destino
        	) = 1) THEN 
            	EXECUTE 
            		'CREATE TRIGGER gravar_historico AFTER INSERT ON '
            		|| NEW.tabela_destino
            		|| ' FOR EACH ROW EXECUTE PROCEDURE dados_publicos.gravar_historico();'
            	;
            ELSE
            	DROP TRIGGER IF EXISTS gravar_historico ON OLD.tabela_destino;
            END if;
            RETURN OLD;
        END IF;
        RETURN NULL; -- result IS ignored since this IS an AFTER trigger
    END;
$gatilho$ LANGUAGE plpgsql;

DROP TRIGGER configurar_gatilhos_gravacao_historico 
ON configuracoes.capturas_operacoes;

CREATE TRIGGER configurar_gatilhos_gravacao_historico
AFTER INSERT OR DELETE ON configuracoes.capturas_operacoes 
    FOR EACH ROW EXECUTE PROCEDURE configuracoes.configurar_gatilhos_gravacao_historico();