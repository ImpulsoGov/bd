-- impulso_previne.capitacao_ponderada_validacao_por_producao_por_aplicacao source

CREATE MATERIALIZED VIEW impulso_previne.capitacao_ponderada_validacao_por_producao_por_aplicacao
TABLESPACE pg_default
AS WITH rel_validacoes AS (
         SELECT tb1.periodo_codigo,
            tb1.municipio_id_sus,
            tb1.cnes_id,
            lpad(tb1.ine_id::text, 10, '0'::text) AS equipe_id_ine,
            tb1.validacao_nome,
            tb1.aplicacao,
            sum(tb1.validacao_quantidade) AS validacao_quantidade
           FROM dados_publicos.sisab_validacao_municipios_por_producao_ficha_por_aplicacao tb1
          WHERE tb1.no_prazo = false
          GROUP BY tb1.periodo_codigo, tb1.municipio_id_sus, tb1.cnes_id, tb1.ine_id, tb1.validacao_nome, tb1.aplicacao
        ), rel_cadastros AS (
         SELECT tb2.municipio_id_sus,
            tb2.cnes_id,
            tb2.equipe_id_ine,
            tb2.criterio_pontuacao
           FROM dados_publicos.sisab_cadastros_municipios_equipe_todas tb2
          WHERE tb2.criterio_pontuacao = false AND (tb2.periodo_codigo::text IN ( SELECT pe.periodo_codigo
                   FROM dados_publicos.sisab_cadastros_municipios_equipe_todas pe
                  WHERE pe.periodo_codigo::text >= '2022.M5'::text
                  ORDER BY pe.periodo_codigo DESC
                 LIMIT 1))
        )
 SELECT res.periodo_codigo,
    p.data_inicio AS periodo_data_inicio,
    p.data_fim AS periodo_data_fim,
    res.municipio_id_sus,
    concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
    res.cnes_id,
    cn.nome AS cnes_nome,
    res.equipe_id_ine,
    cnes.nomeequipe AS equipe_nome,
    res.aplicacao AS validacao_aplicacao,
    res.validacao_nome,
    res.validacao_quantidade,
    r.recomendacao,
    CURRENT_TIMESTAMP AS atualizacao_data
   FROM rel_validacoes res
     JOIN listas_de_codigos.periodos p ON res.periodo_codigo::text = p.codigo::text
     JOIN listas_de_codigos.municipios m ON res.municipio_id_sus = m.id_sus
     LEFT JOIN impulso_previne.validacao_premissas_recomendacoes r ON res.validacao_nome::text = r.validacao_nome::text
     LEFT JOIN dados_publicos.obsoleta_scnes_estabelecimentos cn ON res.cnes_id::bpchar = cn.id_cnes
     LEFT JOIN dados_publicos.obsoleta_scnes_equipe_ine_nome cnes ON res.municipio_id_sus = cnes.comunicipio::bpchar AND res.equipe_id_ine = cnes.coequipe::text
     JOIN rel_cadastros cad ON res.municipio_id_sus = cad.municipio_id_sus::bpchar AND res.cnes_id::text = cad.cnes_id::text AND res.equipe_id_ine = cad.equipe_id_ine::text
WITH DATA;

-- View indexes:
CREATE INDEX capitacao_ponderada_validacao_por_producao_por_aplicacao_mu ON impulso_previne.capitacao_ponderada_validacao_por_producao_por_aplicacao USING btree (municipio_uf, cnes_nome, validacao_aplicacao, validacao_nome);
CREATE INDEX capitacao_ponderada_validacao_por_producao_por_aplicacao_pe ON impulso_previne.capitacao_ponderada_validacao_por_producao_por_aplicacao USING btree (periodo_codigo, municipio_id_sus, cnes_id, equipe_id_ine, validacao_aplicacao, validacao_nome);