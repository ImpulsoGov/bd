CREATE OR REPLACE PROCEDURE configuracoes.atualizar_views_painel_monitoramento_transmissoes()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    view_materializada text;
begin
	FOR view_materializada IN (
        SELECT matviewname
        FROM pg_matviews
        WHERE schemaname in ('configuracoes')   and 
		matviewname like 'monitoramento_transmissoes_%'
        order by matviewname asc
    ) 
	loop
		CALL configuracoes.atualizar_tabela_municipios_transmissoes_ativas();
	   	EXECUTE format(
            'REFRESH MATERIALIZED VIEW configuracoes.%s;',
            quote_ident(view_materializada)
           );
	end loop;
END;
$procedure$
;
