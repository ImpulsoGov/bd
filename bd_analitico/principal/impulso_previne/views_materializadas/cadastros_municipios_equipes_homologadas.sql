-- impulso_previne.cadastros_municipios_equipes_homologadas source

CREATE MATERIALIZED VIEW impulso_previne.cadastros_municipios_equipes_homologadas
TABLESPACE pg_default
AS SELECT res.periodo_data_inicio,
    res.periodo_data_fim,
    res.periodo_codigo,
    res.municipio_id_ibge,
    res.municipio_id_sus,
    res.municipio_nome,
    res.municipio_uf,
    res.equipe_id_ine,
    res.equipe_ine_nome,
    sum(res.cadastro_total) AS cadastro_total,
    sum(res.cadastro_parametro) AS cadastro_parametro,
    max(res.populacao_2020) AS populacao_2020,
    sum(res.cadastros_com_pontuacao) AS cadastros_com_pontuacao
   FROM ( SELECT p.data_inicio AS periodo_data_inicio,
            p.data_fim AS periodo_data_fim,
            tb1.periodo_codigo,
            tb1.municipio_id_sus,
            m.id_ibge AS municipio_id_ibge,
            m.nome AS municipio_nome,
            concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
            tb1.cnes_nome,
            tb1.equipe_id_ine,
            cein.nomeequipe AS equipe_ine_nome,
            sum(tb1.quantidade) FILTER (WHERE tb1.criterio_pontuacao = false) AS cadastro_total,
            sum(tb1.quantidade) FILTER (WHERE tb1.criterio_pontuacao = true) AS cadastros_com_pontuacao,
            tb2.parametro AS cadastro_parametro,
            p2."2020" AS populacao_2020
           FROM dados_publicos.sisab_cadastros_municipios_equipe_homologadas tb1
             JOIN listas_de_codigos.populacao p2 ON tb1.municipio_id_sus::text = p2.municipio_id_sus::text
             JOIN listas_de_codigos.periodos p ON tb1.periodo_codigo::text = p.codigo::text
             JOIN listas_de_codigos.municipios m ON tb1.municipio_id_sus::bpchar = m.id_sus
             LEFT JOIN dados_publicos.sisab_cadastros_parametro_cnes_ine_equipes_equipe_homologadas tb2 ON tb1.municipio_id_sus::text = tb2.municipio_id_sus::text AND tb1.periodo_codigo::text = tb2.periodo_codigo::text AND tb1.equipe_id_ine::text = tb2.equipe_id_ine::text AND tb1.cnes_id::text = tb2.cnes_id::text
             LEFT JOIN dados_publicos.obsoleta_scnes_equipe_ine_nome cein ON tb1.municipio_id_sus::text = cein.comunicipio::text AND tb1.equipe_id_ine::text = cein.coequipe::text
          GROUP BY p.data_inicio, p.data_fim, tb1.periodo_codigo, tb1.municipio_id_sus, m.id_ibge, m.nome, m.uf_sigla, tb1.cnes_nome, tb1.equipe_id_ine, cein.nomeequipe, tb2.parametro, p2."2020"
          ORDER BY tb1.periodo_codigo DESC, m.nome, tb1.cnes_nome) res
  GROUP BY res.periodo_codigo, res.municipio_id_sus, res.municipio_nome, res.municipio_uf, res.periodo_data_inicio, res.periodo_data_fim, res.municipio_id_ibge, res.equipe_id_ine, res.equipe_ine_nome
WITH DATA;