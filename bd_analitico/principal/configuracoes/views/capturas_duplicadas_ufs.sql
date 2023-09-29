-- configuracoes.capturas_duplicadas_ufs source

CREATE OR REPLACE VIEW configuracoes.capturas_duplicadas_ufs
AS SELECT op.tabela_destino,
    "left"(uf.id_sus::text, 2) AS uf_id_sus,
    uf.nome AS uf_nome,
    p.data_inicio AS periodo_data_inicio,
    capturas_historico.unidade_geografica_id,
    capturas_historico.periodo_id,
    count(DISTINCT capturas_historico.id) AS capturas_num,
    array_agg(DISTINCT capturas_historico.data ORDER BY capturas_historico.data DESC) AS capturas_datas,
    (array_agg(capturas_historico.data ORDER BY capturas_historico.data DESC))[1] - (array_agg(capturas_historico.data ORDER BY capturas_historico.data DESC))[2] AS dif_penultima_captura,
    capturas_historico.operacao_id
   FROM configuracoes.capturas_historico
     JOIN listas_de_codigos.ufs uf ON uf.id = capturas_historico.unidade_geografica_id
     LEFT JOIN listas_de_codigos.periodos p ON capturas_historico.periodo_id = p.id AND p.tipo::text = 'Mensal'::text
     LEFT JOIN configuracoes.capturas_operacoes op ON op.id = capturas_historico.operacao_id
  GROUP BY capturas_historico.operacao_id, op.tabela_destino, capturas_historico.unidade_geografica_id, capturas_historico.periodo_id, uf.id_sus, uf.nome, p.data_inicio
 HAVING count(capturas_historico.data) > 1
  ORDER BY op.tabela_destino, uf.nome, p.data_inicio;