-- impulso_previne.indicadores_historico source

CREATE MATERIALIZED VIEW impulso_previne.indicadores_historico
TABLESPACE pg_default
AS SELECT indicadores_desempenho_score_equipes_validas.municipio_id_sus,
    indicadores_desempenho_score_equipes_validas.municipio_uf,
    indicadores_desempenho_score_equipes_validas.periodo_codigo,
    indicadores_desempenho_score_equipes_validas.indicador_ordem,
    indicadores_desempenho_score_equipes_validas.indicador_nome,
    indicadores_desempenho_score_equipes_validas.indicador_numerador::numeric AS indicador_numerador,
    indicadores_desempenho_score_equipes_validas.indicador_denominador_informado::numeric AS indicador_denominador_informado,
    indicadores_desempenho_score_equipes_validas.indicador_denominador_estimado::numeric AS indicador_denominador_estimado,
    indicadores_desempenho_score_equipes_validas.indicador_denominador_utilizado::numeric AS indicador_denominador_utilizado,
    indicadores_desempenho_score_equipes_validas.indicador_denominador_utilizado_tipo,
    indicadores_desempenho_score_equipes_validas.indicador_nota_porcentagem,
    indicadores_desempenho_score_equipes_validas.indicador_meta,
    indicadores_desempenho_score_equipes_validas.indicador_diferenca_meta,
    round(abs(1::numeric - indicadores_desempenho_score_equipes_validas.indicador_denominador_informado::numeric / indicadores_desempenho_score_equipes_validas.indicador_denominador_estimado::numeric), 2) * 100::numeric AS indicador_diferenca_estimado
   FROM impulso_previne.indicadores_desempenho_score_equipes_validas
  WHERE 1 = 1 AND (indicadores_desempenho_score_equipes_validas.municipio_id_sus = ANY (ARRAY['352620'::bpchar, '210735'::bpchar, '317130'::bpchar, '260060'::bpchar, '111111'::bpchar, '315570'::bpchar, '313652'::bpchar, '261485'::bpchar, '315210'::bpchar, '316935'::bpchar, '521308'::bpchar, '520920'::bpchar, '171865'::bpchar, '120070'::bpchar, '310230'::bpchar, '240145'::bpchar, '250215'::bpchar, '210215'::bpchar, '311440'::bpchar, '411190'::bpchar, '160050'::bpchar, '521975'::bpchar, '355060'::bpchar, '211280'::bpchar])) AND indicadores_desempenho_score_equipes_validas.periodo_codigo::text = '2022.Q3'::text AND (indicadores_desempenho_score_equipes_validas.indicador_ordem = ANY (ARRAY[1, 2, 3]))
WITH DATA;