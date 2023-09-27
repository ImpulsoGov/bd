CREATE OR REPLACE PROCEDURE impulso_previne_dados_nominais.atualizar_views_impulso_previne_dados_nominais()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    view_materializada text;
begin
	FOR view_materializada IN (
        SELECT matviewname
        FROM pg_matviews
        WHERE schemaname in ('impulso_previne_dados_nominais') and
        (matviewname like 'painel%' or matviewname like 'lista_nominal%' or matviewname like 'api_futuro_%' or matviewname like 'eventos_pre_natal' or matviewname like 'eventos_vacinacao')
        order by matviewname asc
    ) 
	loop
	   	EXECUTE format(
            'REFRESH MATERIALIZED VIEW impulso_previne_dados_nominais.%s;',
            quote_ident(view_materializada)
           );
	end loop;
END;
$procedure$
;
