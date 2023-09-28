-- impulso_previne.monitoramento_area_logada_usuarios_ativos_por_municipio source

CREATE MATERIALIZED VIEW impulso_previne.monitoramento_area_logada_usuarios_ativos_por_municipio
TABLESPACE pg_default
AS WITH rel_usuarios_cadastrados AS (
         SELECT ui.municipio,
            ui.cargo,
            count(DISTINCT ui.id_usuario) AS usuarios_cadastrados,
            count(DISTINCT ui.equipe) FILTER (WHERE ui.cargo::text <> 'Coordenação APS'::text) AS equipes_cadastradas
           FROM impulso_previne.usuarios_ip ui
          WHERE ui.cargo::text <> 'Impulser'::text
          GROUP BY ui.municipio, ui.cargo
        ), rel_usuarios_ativos_por_dia AS (
         SELECT DISTINCT "substring"(uag.periodo_data_hora::text, 1, 8)::date AS periodo_data,
            uag.usuario_municipio,
            uag.usuario_cargo,
            uag.usuario_id,
            uag.pagina_path,
                CASE
                    WHEN uag.usuario_equipe_ine::text <> '0'::text THEN uag.usuario_equipe_ine
                    ELSE NULL::character varying
                END AS usuario_equipe_ine
           FROM impulso_previne.monitoramento_area_logada uag
          WHERE uag.usuario_cargo <> 'Impulser'::text AND uag.usuario_id <> '(not set)'::text AND uag.usuarios_ativos > 0 AND (uag.cidade_acesso <> ALL (ARRAY['Sao Paulo'::text, 'Santo Andre'::text, 'Rio de Janeiro'::text, 'Brasilia'::text, 'Praia Grande'::text, 'Ribeirao Preto'::text, 'Santos'::text, 'Sao Bernardo do Campo'::text, 'Sao Caetano do Sul'::text, 'Santos'::text, 'Sao Roque'::text]))
        )
 SELECT
        CASE
            WHEN tb2.periodo_data IS NULL THEN CURRENT_DATE
            ELSE tb2.periodo_data
        END AS periodo_data,
    tb1.municipio,
    tb1.cargo,
    tb1.usuarios_cadastrados,
    tb1.equipes_cadastradas,
    tb2.usuario_id,
    tb2.usuario_equipe_ine,
    tb2.pagina_path
   FROM rel_usuarios_cadastrados tb1
     LEFT JOIN rel_usuarios_ativos_por_dia tb2 ON tb1.municipio::text = tb2.usuario_municipio AND tb1.cargo::text = tb2.usuario_cargo
WITH DATA;

-- View indexes:
CREATE INDEX monitoramento_area_logada_usuarios_ativos_por_municipio_periodo ON impulso_previne.monitoramento_area_logada_usuarios_ativos_por_municipio USING btree (periodo_data, municipio, cargo);