-- impulso_previne.indicadores_municipios_equipes_validas source

CREATE MATERIALIZED VIEW impulso_previne.indicadores_municipios_equipes_validas
TABLESPACE pg_default
AS SELECT p.data_inicio AS periodo_data_inicio,
    p.data_fim AS periodo_data_fim,
    tb1.periodo_codigo,
    m.id_ibge AS municipio_id_ibge,
    m.id_sus AS municipio_id_sus,
    m.nome AS municipio_nome,
    concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
    ir.ordem AS indicador_ordem,
    tb1.indicadores_nome AS indicador_nome,
    ir.meta AS indicador_meta,
    ir.peso AS indicador_peso,
    tb1.numerador AS indicador_numerador,
    tb1.denominador_informado AS indicador_denominador_informado,
    tb1.denominador_estimado AS indicador_denominador_estimado,
    GREATEST(tb1.denominador_informado, tb1.denominador_estimado) AS indicador_denominador_utilizado,
    tb1.nota_porcentagem AS indicador_resultado
   FROM dados_publicos.sisab_indicadores_municipios_equipes_validas tb1
     JOIN listas_de_codigos.municipios m ON tb1.municipio_id_sus::bpchar = m.id_sus
     JOIN previne_brasil.indicadores_regras ir ON tb1.indicadores_regras_id = ir.id
     JOIN listas_de_codigos.periodos p ON tb1.periodo_codigo::text = p.codigo::text
  ORDER BY ir.ordem, tb1.periodo_codigo DESC, m.nome
WITH DATA;