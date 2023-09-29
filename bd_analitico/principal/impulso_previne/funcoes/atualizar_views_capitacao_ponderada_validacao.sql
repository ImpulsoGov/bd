CREATE OR REPLACE PROCEDURE impulso_previne.atualizar_views_capitacao_ponderada_validacao()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    view_materializada text;
BEGIN
    FOR view_materializada IN (
        SELECT matviewname
        FROM pg_matviews
        WHERE schemaname = 'impulso_previne'
        and matviewname like 'capitacao_ponderada_validacao%'
    ) LOOP
        EXECUTE format(
            'REFRESH MATERIALIZED VIEW impulso_previne.%s;',
            quote_ident(view_materializada)
        );
    END LOOP;
END;
$procedure$
;
