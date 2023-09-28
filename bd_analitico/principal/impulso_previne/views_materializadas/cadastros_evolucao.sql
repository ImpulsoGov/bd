-- impulso_previne.cadastros_evolucao source

CREATE MATERIALIZED VIEW impulso_previne.cadastros_evolucao
TABLESPACE pg_default
AS SELECT concat(pop.municipio_nome, ' - ', pop.estado_id) AS municipio_nome,
    p.data_inicio,
    cad.periodo_codigo,
    cad.municipio_id_sus,
    tipo.tipologia,
    pop."2020" AS municipio_populacao_ibge,
    mun_par.parametro AS municipio_parametro,
    cad.cnes_id,
    cad.cnes_nome,
    cad.ine_id,
    ine_par."parâmetro" AS ine_parametro,
    cad_total.quantidade AS cadastros_total,
    cad_pond.quantidade AS cadastros_com_ponderacao
   FROM dados_publicos.sisab_cadastros_municipios_equipe_validas cad
     JOIN listas_de_codigos.periodos p ON p.codigo::text = cad.periodo_codigo::text
     JOIN listas_de_codigos.populacao pop ON pop.municipio_id_sus::text = cad.municipio_id_sus::text
     JOIN listas_de_codigos.municipio_tipologia tipo ON tipo.id_ibge::text = pop.municipio_id::text
     JOIN dados_publicos."__sisab_cadastros_parametro_cnes+ine_equipes_validas" ine_par ON ine_par.ine_id::text = cad.ine_id::text
     JOIN dados_publicos.sisab_cadastros_parametro_municipios_equipes_validas mun_par ON mun_par.municipio_id_sus::text = cad.municipio_id_sus::text
     JOIN ( SELECT sisab_cadastros_municipios_equipe_validas.ine_id,
            sisab_cadastros_municipios_equipe_validas.quantidade,
            sisab_cadastros_municipios_equipe_validas.periodo_codigo
           FROM dados_publicos.sisab_cadastros_municipios_equipe_validas
          WHERE sisab_cadastros_municipios_equipe_validas.criterio_pontuacao = false) cad_total ON cad_total.ine_id::text = cad.ine_id::text AND cad_total.periodo_codigo::text = cad.periodo_codigo::text
     JOIN ( SELECT sisab_cadastros_municipios_equipe_validas.ine_id,
            sisab_cadastros_municipios_equipe_validas.quantidade,
            sisab_cadastros_municipios_equipe_validas.periodo_codigo
           FROM dados_publicos.sisab_cadastros_municipios_equipe_validas
          WHERE sisab_cadastros_municipios_equipe_validas.criterio_pontuacao = true) cad_pond ON cad_pond.ine_id::text = cad.ine_id::text AND cad_pond.periodo_codigo::text = cad.periodo_codigo::text
  WHERE p.data_inicio >= '2020-01-01'::date
WITH DATA;