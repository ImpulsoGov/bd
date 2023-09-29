CREATE OR REPLACE PROCEDURE impulso_previne_dados_nominais.atualizar_views_relatorio_transmissao()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    view_materializada text;
BEGIN
    FOR view_materializada IN (
        SELECT matviewname
        FROM pg_matviews
        WHERE schemaname = 'impulso_previne_dados_nominais'
        and matviewname like 'relatorio_transmissao%'
    ) LOOP
        EXECUTE format(
            'REFRESH MATERIALIZED VIEW impulso_previne_dados_nominais.%s;',
            quote_ident(view_materializada)
        );
    END LOOP;
END;
$procedure$
;
