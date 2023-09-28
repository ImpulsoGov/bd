-- impulso_previne.capitacao_ponderada_contagem_equipes source

CREATE MATERIALIZED VIEW impulso_previne.capitacao_ponderada_contagem_equipes
TABLESPACE pg_default
AS SELECT res.periodo_codigo,
    concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
    res.municipio_id_sus,
    res.equipe_status_tipo,
    res.equipe_status,
    COALESCE(NULLIF(res.equipe_total::double precision, 0::double precision), 0::double precision) AS equipe_total,
    CURRENT_TIMESTAMP AS criacao_data,
    CURRENT_TIMESTAMP AS atualizacao_data
   FROM ( SELECT tb1.periodo_codigo,
            tb1.municipio_id_sus,
            'Cadastradas'::text AS equipe_status,
            'Cadastrados'::text AS equipe_status_tipo,
            count(DISTINCT tb1.equipe_id_ine) AS equipe_total
           FROM dados_publicos.sisab_cadastros_municipios_equipe_todas tb1
          GROUP BY tb1.periodo_codigo, tb1.municipio_id_sus
        UNION ALL
         SELECT tb1.periodo_codigo,
            tb1.municipio_id_sus,
            'Cadastradas e homologadas'::text AS equipe_status,
            'Homologados'::text AS equipe_status_tipo,
            count(DISTINCT tb1.equipe_id_ine) AS equipe_total
           FROM dados_publicos.sisab_cadastros_municipios_equipe_homologadas tb1
          GROUP BY tb1.periodo_codigo, tb1.municipio_id_sus
        UNION ALL
         SELECT tb1.periodo_codigo,
            tb1.municipio_id_sus,
            'Cadastradas e não homologadas'::text AS equipe_status,
            'Homologados'::text AS equipe_status_tipo,
            count(DISTINCT tb1.equipe_id_ine) AS equipe_total
           FROM dados_publicos.sisab_cadastros_municipios_equipe_todas tb1
          WHERE NOT (tb1.equipe_id_ine::text IN ( SELECT sisab_cadastros_municipios_equipe_homologadas.equipe_id_ine
                   FROM dados_publicos.sisab_cadastros_municipios_equipe_homologadas
                  WHERE tb1.municipio_id_sus::text = sisab_cadastros_municipios_equipe_homologadas.municipio_id_sus::text AND tb1.periodo_codigo::text = sisab_cadastros_municipios_equipe_homologadas.periodo_codigo::text AND tb1.criterio_pontuacao = sisab_cadastros_municipios_equipe_homologadas.criterio_pontuacao AND tb1.cnes_id::text = sisab_cadastros_municipios_equipe_homologadas.cnes_id::text))
          GROUP BY tb1.periodo_codigo, tb1.municipio_id_sus
        UNION ALL
         SELECT tb1.periodo_codigo,
            tb1.municipio_id_sus,
            'Homologadas e válidas'::text AS equipe_status,
            'Válidos'::text AS equipe_status_tipo,
            count(DISTINCT tb1.equipe_id_ine) AS equipe_total
           FROM dados_publicos.sisab_cadastros_municipios_equipe_validas tb1
          GROUP BY tb1.periodo_codigo, tb1.municipio_id_sus
        UNION ALL
         SELECT tb1.periodo_codigo,
            tb1.municipio_id_sus,
            'Homologadas e não válidas'::text AS equipe_status,
            'Válidos'::text AS equipe_status_tipo,
            count(DISTINCT tb1.equipe_id_ine) AS equipe_total
           FROM dados_publicos.sisab_cadastros_municipios_equipe_homologadas tb1
          WHERE NOT (tb1.equipe_id_ine::text IN ( SELECT sisab_cadastros_municipios_equipe_validas.equipe_id_ine
                   FROM dados_publicos.sisab_cadastros_municipios_equipe_validas
                  WHERE tb1.municipio_id_sus::text = sisab_cadastros_municipios_equipe_validas.municipio_id_sus::text AND tb1.periodo_codigo::text = sisab_cadastros_municipios_equipe_validas.periodo_codigo::text AND tb1.criterio_pontuacao = sisab_cadastros_municipios_equipe_validas.criterio_pontuacao AND tb1.cnes_id::text = sisab_cadastros_municipios_equipe_validas.cnes_id::text))
          GROUP BY tb1.periodo_codigo, tb1.municipio_id_sus) res
     LEFT JOIN listas_de_codigos.municipios m ON res.municipio_id_sus::bpchar = m.id_sus
  WHERE (res.periodo_codigo::text IN ( SELECT pe.periodo_codigo
           FROM dados_publicos.sisab_cadastros_municipios_equipe_todas pe
          WHERE pe.periodo_codigo::text >= '2022.M5'::text
          ORDER BY pe.periodo_codigo DESC
         LIMIT 1))
WITH DATA;

-- View indexes:
CREATE INDEX capitacao_ponderada_contagem_equipes_municipio_uf_idx ON impulso_previne.capitacao_ponderada_contagem_equipes USING btree (municipio_uf, equipe_status_tipo, equipe_status);
CREATE INDEX capitacao_ponderada_contagem_equipes_periodo_codigo_idx ON impulso_previne.capitacao_ponderada_contagem_equipes USING btree (periodo_codigo, municipio_id_sus, equipe_status);