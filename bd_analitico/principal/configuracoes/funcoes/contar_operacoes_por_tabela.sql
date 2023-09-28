CREATE OR REPLACE FUNCTION configuracoes.contar_operacoes_por_tabela(tabela text)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
declare
	temp RECORD;
	BEGIN
		select
			count(distinct op.id) as num_operacoes
		into temp  
    	from configuracoes.capturas_operacoes op
    	where op.tabela_destino = tabela;
        return temp.num_operacoes;
	END;
$function$
;
