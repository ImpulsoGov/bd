-- impulso_previne.area_logada_nps source

CREATE MATERIALIZED VIEW impulso_previne.area_logada_nps
TABLESPACE pg_default
AS SELECT n.usuario_id,
    n.avaliacao,
    n.criacao_data::date AS data_avaliacao,
    date_trunc('WEEK'::text, n.criacao_data::date::timestamp with time zone)::date AS semana_avaliacao,
    date_trunc('MONTH'::text, n.criacao_data::date::timestamp with time zone)::date AS mes_avaliacao,
        CASE
            WHEN n.avaliacao = 5 THEN 'Promotor'::text
            WHEN n.avaliacao = 4 THEN 'Neutro'::text
            WHEN n.avaliacao = ANY (ARRAY[1, 2, 3]) THEN 'Detrator'::text
            ELSE NULL::text
        END AS categoria_nps,
    ui.municipio,
    ui.cargo
   FROM impulso_previne.nps n
     JOIN impulso_previne.usuarios_ip ui ON n.usuario_id = ui.id_usuario
  WHERE ui.cargo::text <> 'Impulser'::text
WITH DATA;

-- View indexes:
CREATE INDEX area_logada_nps_usuario_id_idx ON impulso_previne.area_logada_nps USING btree (usuario_id, avaliacao);