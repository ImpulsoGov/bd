CREATE OR REPLACE VIEW configuracoes.monitoramento_etl_agendamentos
AS SELECT tb1.tabela_destino,
    co.descricao,
    co.projuto,
    p.codigo AS periodo_codigo,
    co.periodo_tipo,
    tb1.periodo_data_inicio,
    tb1.capturar_apos,
    CURRENT_DATE AS data_corrente,
    tb1.atualizacao_retroativa,
    'Atrasado'::text AS status_execucao_etl,
    CURRENT_DATE - tb1.capturar_apos AS tempo_atraso,
    tb1.uf_sigla,
    ug.nome AS unidade_geografica_nome
   FROM configuracoes.capturas_agendamentos tb1
     JOIN configuracoes.capturas_operacoes co ON tb1.operacao_id = co.id
     JOIN listas_de_codigos.periodos p ON tb1.periodo_id = p.id
     LEFT JOIN listas_de_codigos.unidades_geograficas ug ON tb1.unidade_geografica_id = ug.id
  WHERE tb1.capturar_apos < CURRENT_DATE
UNION ALL
 SELECT tb1.tabela_destino,
    co.descricao,
    co.projuto,
    p.codigo AS periodo_codigo,
    co.periodo_tipo,
    tb1.periodo_data_inicio,
    tb1.capturar_apos,
    CURRENT_DATE AS data_corrente,
    tb1.atualizacao_retroativa,
    'Dentro do prazo de divulgação'::text AS status_execucao_etl,
    CURRENT_DATE - tb1.capturar_apos AS tempo_atraso,
    tb1.uf_sigla,
    ug.nome AS unidade_geografica_nome
   FROM configuracoes.capturas_agendamentos tb1
     JOIN configuracoes.capturas_operacoes co ON tb1.operacao_id = co.id
     JOIN listas_de_codigos.periodos p ON tb1.periodo_id = p.id
     LEFT JOIN listas_de_codigos.unidades_geograficas ug ON tb1.unidade_geografica_id = ug.id
  WHERE tb1.capturar_apos >= CURRENT_DATE;