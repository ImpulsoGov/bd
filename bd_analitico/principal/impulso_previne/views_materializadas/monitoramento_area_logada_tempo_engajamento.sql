-- impulso_previne.monitoramento_area_logada_tempo_engajamento source

CREATE MATERIALIZED VIEW impulso_previne.monitoramento_area_logada_tempo_engajamento
TABLESPACE pg_default
AS SELECT res.periodo_data,
    res.usuario_municipio,
    res.usuario_cargo,
    res.pagina_path,
    avg(res.sessao_duracao) AS sessao_duracao_media
   FROM ( SELECT "substring"(tb1.periodo_data_hora::text, 1, 8)::date AS periodo_data,
            tb1.usuario_municipio,
            tb1.usuario_cargo,
            tb1.pagina_path,
            tb1.sessao_duracao
           FROM impulso_previne.monitoramento_area_logada tb1
          WHERE tb1.usuario_id <> '(not set)'::text AND tb1.usuario_cargo <> 'Impulser'::text AND (tb1.cidade_acesso <> ALL (ARRAY['Sao Paulo'::text, 'Santo Andre'::text, 'Rio de Janeiro'::text, 'Brasilia'::text, 'Praia Grande'::text, 'Ribeirao Preto'::text, 'Santos'::text, 'Sao Bernardo do Campo'::text, 'Sao Caetano do Sul'::text, 'Santos'::text, 'Sao Roque'::text])) AND tb1.sessao_duracao > 0) res
  GROUP BY res.periodo_data, res.usuario_municipio, res.usuario_cargo, res.pagina_path
WITH DATA;

-- View indexes:
CREATE INDEX monitoramento_area_logada_tempo_engajamento_periodo_data_idx ON impulso_previne.monitoramento_area_logada_tempo_engajamento USING btree (periodo_data, usuario_municipio, usuario_cargo, pagina_path);