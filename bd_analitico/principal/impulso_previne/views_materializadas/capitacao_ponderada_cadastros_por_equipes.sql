-- impulso_previne.capitacao_ponderada_cadastros_por_equipes source

CREATE MATERIALIZED VIEW impulso_previne.capitacao_ponderada_cadastros_por_equipes
TABLESPACE pg_default
AS SELECT res.periodo_codigo,
    pe.data_inicio,
    res.municipio_id_sus,
    concat(p.municipio_nome, ' - ', p.estado_id) AS municipio_uf,
    p.municipio_nome,
    mt.tipologia,
    mt.uf_nome,
    res.cnes_id,
    res.cnes_nome,
    res.equipe_id_ine,
    nei.nomeequipe AS equipe_nome,
    res.status AS equipe_status,
    efcp.cadastro_potencial AS municipio_ultimo_parametro,
    sum(res.quantidade) FILTER (WHERE res.criterio_pontuacao = false) AS cadastro_total,
    sum(res.quantidade) FILTER (WHERE res.criterio_pontuacao = true) AS cadastros_com_pontuacao,
    res."parâmetro" AS equipe_parametro,
        CASE
            WHEN sum(res.quantidade) FILTER (WHERE res.criterio_pontuacao = false) > 0 AND res."parâmetro" > 0 THEN trunc(sum(res.quantidade) FILTER (WHERE res.criterio_pontuacao = false)::numeric / res."parâmetro"::numeric, 4)
            ELSE 0::numeric
        END AS cobertura_parametro_porcentagem,
    CURRENT_TIMESTAMP AS criacao_data,
    CURRENT_TIMESTAMP AS atualizacao_data
   FROM ( SELECT tb1.periodo_codigo,
            tb1.municipio_id_sus,
            tb1.equipe_id_ine,
            tb1.cnes_id,
            tb1.cnes_nome,
            'Válidas'::text AS status,
            tb1.criterio_pontuacao,
            tb1.quantidade,
            p1."parâmetro"
           FROM dados_publicos.sisab_cadastros_municipios_equipe_validas tb1
             LEFT JOIN dados_publicos."__sisab_cadastros_parametro_cnes+ine_equipes_validas" p1 ON tb1.equipe_id_ine::text = lpad(p1.ine_id::text, 10, '0'::text) AND tb1.municipio_id_sus::text = p1.municipio_id_sus::text
        UNION ALL
         SELECT tb2.periodo_codigo,
            tb2.municipio_id_sus,
            tb2.equipe_id_ine,
            tb2.cnes_id,
            tb2.cnes_nome,
            'Homologadas'::text AS status,
            tb2.criterio_pontuacao,
            tb2.quantidade,
            p2."parâmetro"
           FROM dados_publicos.sisab_cadastros_municipios_equipe_homologadas tb2
             LEFT JOIN dados_publicos."__sisab_cadastros_parametro_cnes+ine_equipes_homologadas" p2 ON tb2.equipe_id_ine::text = lpad(p2.ine_id::text, 10, '0'::text) AND tb2.municipio_id_sus::text = p2.municipio_id_sus::text
          WHERE NOT (tb2.equipe_id_ine::text IN ( SELECT sisab_cadastros_municipios_equipe_validas.equipe_id_ine
                   FROM dados_publicos.sisab_cadastros_municipios_equipe_validas
                  WHERE tb2.municipio_id_sus::text = sisab_cadastros_municipios_equipe_validas.municipio_id_sus::text AND tb2.periodo_codigo::text = sisab_cadastros_municipios_equipe_validas.periodo_codigo::text AND tb2.criterio_pontuacao = sisab_cadastros_municipios_equipe_validas.criterio_pontuacao AND tb2.cnes_id::text = sisab_cadastros_municipios_equipe_validas.cnes_id::text))
        UNION ALL
         SELECT tb3.periodo_codigo,
            tb3.municipio_id_sus,
            tb3.equipe_id_ine,
            tb3.cnes_id,
            tb3.cnes_nome,
            'Cadastradas'::text AS status,
            tb3.criterio_pontuacao,
            tb3.quantidade,
            0 AS "parâmetro"
           FROM dados_publicos.sisab_cadastros_municipios_equipe_todas tb3
          WHERE NOT (tb3.equipe_id_ine::text IN ( SELECT sisab_cadastros_municipios_equipe_homologadas.equipe_id_ine
                   FROM dados_publicos.sisab_cadastros_municipios_equipe_homologadas
                  WHERE tb3.municipio_id_sus::text = sisab_cadastros_municipios_equipe_homologadas.municipio_id_sus::text AND tb3.periodo_codigo::text = sisab_cadastros_municipios_equipe_homologadas.periodo_codigo::text AND tb3.criterio_pontuacao = sisab_cadastros_municipios_equipe_homologadas.criterio_pontuacao AND tb3.cnes_id::text = sisab_cadastros_municipios_equipe_homologadas.cnes_id::text))) res
     JOIN listas_de_codigos.populacao p ON res.municipio_id_sus::text = p.municipio_id_sus::text
     JOIN listas_de_codigos.municipio_tipologia mt ON p.municipio_id::text = mt.id_ibge::text
     JOIN listas_de_codigos.periodos pe ON res.periodo_codigo::text = pe.codigo::text
     JOIN impulso_previne.egestor_financiamento_capitacao_parametro efcp ON efcp.municipio_id_sus::text = res.municipio_id_sus::text
     LEFT JOIN dados_publicos.obsoleta_scnes_equipe_ine_nome nei ON res.municipio_id_sus::text = nei.comunicipio::text AND res.equipe_id_ine::text = nei.coequipe::text
  GROUP BY res.periodo_codigo, pe.data_inicio, res.municipio_id_sus, res.cnes_id, res.cnes_nome, res.equipe_id_ine, nei.nomeequipe, res.status, p.municipio_nome, p.estado_id, mt.tipologia, res."parâmetro", mt.uf_nome, efcp.cadastro_potencial
WITH DATA;

-- View indexes:
CREATE INDEX capitacao_ponderada_cadastros_por_equipes_municipio_uf_idx ON impulso_previne.capitacao_ponderada_cadastros_por_equipes USING btree (municipio_uf, equipe_status, cadastro_total, cadastros_com_pontuacao, municipio_ultimo_parametro);
CREATE INDEX capitacao_ponderada_cadastros_por_equipes_periodo_codigo_idx ON impulso_previne.capitacao_ponderada_cadastros_por_equipes USING btree (periodo_codigo, municipio_id_sus, cnes_id, equipe_id_ine);