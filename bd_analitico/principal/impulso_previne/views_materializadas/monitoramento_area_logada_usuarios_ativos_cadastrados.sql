-- impulso_previne.monitoramento_area_logada_usuarios_ativos_cadastrados source

CREATE MATERIALIZED VIEW impulso_previne.monitoramento_area_logada_usuarios_ativos_cadastrados
TABLESPACE pg_default
AS SELECT tb1.municipio AS usuario_municipio,
    tb1.cargo AS usuario_cargo,
    count(DISTINCT tb1.id_usuario) AS usuarios_cadastrados,
    ( SELECT count(DISTINCT tb2_1.usuario_id) AS count
           FROM impulso_previne.monitoramento_area_logada tb2_1
          WHERE tb2_1.usuario_municipio = tb1.municipio::text AND tb2_1.usuario_cargo = tb1.cargo::text AND tb2_1.usuarios_ativos > 0 AND tb2_1.usuario_id <> '(not set)'::text AND tb2_1.usuario_cargo <> 'Impulser'::text AND (tb2_1.cidade_acesso <> ALL (ARRAY['Sao Paulo'::text, 'Santo Andre'::text, 'Rio de Janeiro'::text, 'Brasilia'::text, 'Praia Grande'::text, 'Ribeirao Preto'::text, 'Santos'::text, 'Sao Bernardo do Campo'::text, 'Sao Caetano do Sul'::text, 'Santos'::text, 'Sao Roque'::text]))) AS usuarios_ativos,
    count(DISTINCT tb1.equipe) FILTER (WHERE tb1.cargo::text <> ALL (ARRAY['Impulser'::character varying::text, 'Coordenação APS'::character varying::text])) AS equipes_cadastrados,
    ( SELECT count(DISTINCT tb2_1.usuario_equipe_ine) AS count
           FROM impulso_previne.monitoramento_area_logada tb2_1
          WHERE tb2_1.usuario_municipio = tb1.municipio::text AND tb2_1.usuario_cargo = tb1.cargo::text AND tb2_1.usuarios_ativos > 0 AND tb2_1.usuario_equipe_ine::text <> '(not set)'::text AND (tb2_1.usuario_cargo <> ALL (ARRAY['Impulser'::text, 'Coordenação APS'::text])) AND (tb2_1.cidade_acesso <> ALL (ARRAY['Sao Paulo'::text, 'Santo Andre'::text, 'Rio de Janeiro'::text, 'Brasilia'::text, 'Praia Grande'::text, 'Ribeirao Preto'::text, 'Santos'::text, 'Sao Bernardo do Campo'::text, 'Sao Caetano do Sul'::text, 'Santos'::text, 'Sao Roque'::text]))) AS equipes_ativas
   FROM impulso_previne.usuarios_ip tb1
     LEFT JOIN impulso_previne.monitoramento_area_logada tb2 ON tb1.id_usuario::text = tb2.usuario_id
  WHERE tb1.cargo::text <> 'Impulser'::text
  GROUP BY tb1.municipio, tb1.cargo
WITH DATA;

-- View indexes:
CREATE INDEX monitoramento_area_logada_usuarios_ativos_cadastrados_usuario_m ON impulso_previne.monitoramento_area_logada_usuarios_ativos_cadastrados USING btree (usuario_municipio, usuario_cargo);