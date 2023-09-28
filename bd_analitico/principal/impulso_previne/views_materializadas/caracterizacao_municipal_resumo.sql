-- impulso_previne.caracterizacao_municipal_resumo source

CREATE MATERIALIZED VIEW impulso_previne.caracterizacao_municipal_resumo
TABLESPACE pg_default
AS WITH rel_cadastros_equipes_todas AS (
         SELECT tb1_1.periodo_codigo,
            tb1_1.municipio_id_sus,
            count(DISTINCT tb1_1.equipe_id_ine) AS equipe_total
           FROM dados_publicos.sisab_cadastros_municipios_equipe_todas tb1_1
          WHERE (tb1_1.periodo_codigo::text IN ( SELECT pe.periodo_codigo
                   FROM dados_publicos.sisab_cadastros_municipios_equipe_todas pe
                  WHERE pe.periodo_codigo::text >= '2022.M5'::text
                  ORDER BY pe.periodo_codigo DESC
                 LIMIT 1))
          GROUP BY tb1_1.municipio_id_sus, tb1_1.periodo_codigo
        ), rel_cadastros_equipes_validas AS (
         SELECT tb2_1.periodo_codigo,
            tb2_1.municipio_id_sus,
            sum(tb2_1.quantidade) FILTER (WHERE tb2_1.criterio_pontuacao = false) AS cadastros_equipes_validas,
            sum(tb2_1.quantidade) FILTER (WHERE tb2_1.criterio_pontuacao = true) AS cadastros_equipes_validas_com_ponderacao
           FROM dados_publicos.sisab_cadastros_municipios_equipe_validas tb2_1
          WHERE (tb2_1.periodo_codigo::text IN ( SELECT pe.periodo_codigo
                   FROM dados_publicos.sisab_cadastros_municipios_equipe_validas pe
                  WHERE pe.periodo_codigo::text >= '2022.M5'::text
                  ORDER BY pe.periodo_codigo DESC
                 LIMIT 1))
          GROUP BY tb2_1.municipio_id_sus, tb2_1.periodo_codigo
        ), rel_parametro_equipes_validas AS (
         SELECT tb3_1.periodo_codigo,
            tb3_1.municipio_id_sus,
            tb3_1.parametro
           FROM dados_publicos.sisab_cadastros_parametro_municipios_equipes_validas tb3_1
          WHERE (tb3_1.periodo_codigo::text IN ( SELECT pe.periodo_codigo
                   FROM dados_publicos.sisab_cadastros_parametro_municipios_equipes_validas pe
                  WHERE pe.periodo_codigo::text >= '2022.M5'::text
                  ORDER BY pe.periodo_codigo DESC
                 LIMIT 1))
        )
 SELECT tb1.periodo_codigo,
    tb4.id_sus AS municipio_id_sus,
    tb4.nome AS municipio_nome,
    concat(tb4.nome, ' - ', tb4.uf_sigla) AS municipio_uf,
    p."2020" AS municipio_populacao_2020,
    mt.tipologia AS municipio_tipologia,
    tb1.equipe_total,
    tb2.cadastros_equipes_validas,
    tb2.cadastros_equipes_validas_com_ponderacao,
    tb3.parametro AS cadastro_parametro
   FROM listas_de_codigos.municipios tb4
     LEFT JOIN listas_de_codigos.municipio_tipologia mt ON tb4.id_ibge = mt.id_ibge::bpchar
     LEFT JOIN listas_de_codigos.populacao p ON tb4.id_sus = p.municipio_id_sus::bpchar
     LEFT JOIN rel_cadastros_equipes_todas tb1 ON tb4.id_sus = tb1.municipio_id_sus::bpchar
     LEFT JOIN rel_cadastros_equipes_validas tb2 ON tb4.id_sus = tb2.municipio_id_sus::bpchar
     LEFT JOIN rel_parametro_equipes_validas tb3 ON tb4.id_sus = tb3.municipio_id_sus::bpchar
WITH DATA;

-- View indexes:
CREATE INDEX caracterizacao_municipal_resumo_municipio_uf_idx ON impulso_previne.caracterizacao_municipal_resumo USING btree (municipio_uf);