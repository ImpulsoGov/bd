CREATE OR REPLACE PROCEDURE impulso_previne.atualizar_views_insights()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    view_materializada text;
BEGIN
    FOR view_materializada IN (
        SELECT matviewname
        FROM pg_matviews
        WHERE schemaname = 'impulso_previne'
        and matviewname in ('indicadores_municipios_equipes_homologadas',
        					'insigths_financiamento_desempenho_isf','insigths_indicadores_equipes_validas_meta')
    ) LOOP
        EXECUTE format(
            'REFRESH MATERIALIZED VIEW impulso_previne.%s;',
            quote_ident(view_materializada)
        );
    END LOOP;
END;
$procedure$
;
