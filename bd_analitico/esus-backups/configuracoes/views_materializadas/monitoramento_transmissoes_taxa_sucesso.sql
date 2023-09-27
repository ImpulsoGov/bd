
CREATE MATERIALIZED VIEW configuracoes.monitoramento_transmissoes_taxa_sucesso
TABLESPACE pg_default
AS SELECT monitoramento_transmissoes_historico.municipio_id_sus,
    monitoramento_transmissoes_historico.municipio_uf,
    count(*) FILTER (WHERE monitoramento_transmissoes_historico.status_transmissao = 'Transmissão realizada'::text) AS transmissoes_realizadas,
    count(*) FILTER (WHERE monitoramento_transmissoes_historico.status_transmissao = 'Transmissão perdida'::text) AS "transmissoes_não_realizadas",
    count(*) AS transmissoes_todas_tentativas,
    round(count(*) FILTER (WHERE monitoramento_transmissoes_historico.status_transmissao = 'Transmissão realizada'::text)::numeric / count(*)::numeric, 4) AS percentual_transmissoes_realizadas
   FROM configuracoes.monitoramento_transmissoes_historico
  WHERE monitoramento_transmissoes_historico.status_transmissao <> 'Transmissão agendada'::text AND monitoramento_transmissoes_historico.transmissao_dia >= '2023-03-01'::date
  GROUP BY monitoramento_transmissoes_historico.municipio_id_sus, monitoramento_transmissoes_historico.municipio_uf
WITH DATA;