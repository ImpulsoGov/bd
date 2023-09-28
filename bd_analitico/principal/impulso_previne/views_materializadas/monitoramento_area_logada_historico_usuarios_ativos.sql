-- impulso_previne.monitoramento_area_logada_historico_usuarios_ativos source

CREATE MATERIALIZED VIEW impulso_previne.monitoramento_area_logada_historico_usuarios_ativos
TABLESPACE pg_default
AS SELECT res.periodo_data,
    res.usuario_municipio,
        CASE
            WHEN res.usuario_cargo = ANY (ARRAY['Coordenação APS'::text, 'Coordenadora PS'::text, 'Coordenação da APS'::text]) THEN 'Coordenação APS'::text
            WHEN res.usuario_cargo = 'Coordenação de Equipe'::text THEN 'Coordenação de Equipe'::text
            ELSE NULL::text
        END AS usuario_cargo,
    res.pagina_path,
        CASE
            WHEN res.usuarios_ativos <> 0 THEN res.usuario_id
            ELSE NULL::text
        END AS usuario_id
   FROM ( SELECT "substring"(tb1.periodo_data_hora::text, 1, 8)::date AS periodo_data,
            tb1.usuario_id,
            tb1.usuario_municipio,
            tb1.usuario_cargo,
            tb1.pagina_path,
            count(DISTINCT tb1.usuario_id) FILTER (WHERE tb1.usuarios_ativos > 0) AS usuarios_ativos
           FROM impulso_previne.monitoramento_area_logada tb1
          WHERE tb1.usuario_id <> '(not set)'::text AND tb1.usuario_cargo <> 'Impulser'::text AND (tb1.cidade_acesso <> ALL (ARRAY['Sao Paulo'::text, 'Santo Andre'::text, 'Rio de Janeiro'::text, 'Brasilia'::text, 'Praia Grande'::text, 'Ribeirao Preto'::text, 'Santos'::text, 'Sao Bernardo do Campo'::text, 'Sao Caetano do Sul'::text, 'Santos'::text, 'Sao Roque'::text]))
          GROUP BY tb1.periodo_data_hora, tb1.usuario_municipio, tb1.usuario_cargo, tb1.usuario_id, tb1.pagina_path) res
  ORDER BY res.periodo_data
WITH DATA;

-- View indexes:
CREATE INDEX monitoramento_area_logada_historico_usuarios_ativos_periodo_dat ON impulso_previne.monitoramento_area_logada_historico_usuarios_ativos USING btree (periodo_data, usuario_municipio, usuario_cargo, pagina_path);