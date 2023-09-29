-- configuracoes.monitoramento_etl source

CREATE OR REPLACE VIEW configuracoes.monitoramento_etl
AS SELECT operacao.projuto,
    operacao.tabela_destino,
    unidade_geografica.tipo,
    unidade_geografica.nome,
    periodo.codigo AS competencia,
    historico.capturado_em,
    historico.quantidade_registros
   FROM configuracoes.capturas_historico_consolidado historico
     LEFT JOIN configuracoes.capturas_operacoes operacao ON historico.operacao_id = operacao.id
     LEFT JOIN listas_de_codigos.periodos periodo ON historico.periodo_id = periodo.id
     LEFT JOIN listas_de_codigos.unidades_geograficas unidade_geografica ON historico.unidade_geografica_id = unidade_geografica.id
  ORDER BY operacao.projuto, operacao.tabela_destino, unidade_geografica.tipo, unidade_geografica.nome, periodo.tipo, periodo.data_inicio, historico.capturado_em;