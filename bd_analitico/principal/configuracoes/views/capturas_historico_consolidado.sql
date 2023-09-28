-- configuracoes.capturas_historico_consolidado source

CREATE OR REPLACE VIEW configuracoes.capturas_historico_consolidado
AS SELECT capturas_historico.operacao_id,
    capturas_historico.periodo_id,
    capturas_historico.unidade_geografica_id,
    min(capturas_historico.data) AS capturado_em,
    max(capturas_historico.data) AS atualizado_em,
    count(DISTINCT capturas_historico.id) AS quantidade_registros
   FROM configuracoes.capturas_historico
  GROUP BY capturas_historico.operacao_id, capturas_historico.periodo_id, capturas_historico.unidade_geografica_id;