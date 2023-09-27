
CREATE MATERIALIZED VIEW configuracoes.monitoramento_transmissoes_registros_variacoes
TABLESPACE pg_default
AS WITH transmissoes_por_municipio_por_lista AS (
         SELECT max(tb1.execucao_data_hora::date) OVER (PARTITION BY tb1.municipio_id_sus, tb1.tabela_nome, (tb1.execucao_data_hora::date)) AS execucao_data_hora_unica,
            tb1.registros,
            tb1.municipio_id_sus,
            tb1.tabela_nome AS lista_nominal,
            concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf
           FROM configuracoes.transmissor_historico tb1
             JOIN listas_de_codigos.municipios m ON tb1.municipio_id_sus::bpchar = m.id_sus
          WHERE tb1.mensagem ~~ '%com sucesso%'::text
          ORDER BY tb1.execucao_data_hora, tb1.municipio_id_sus DESC
        ), transmissoes_unicas_delta AS (
         SELECT res_1.municipio_id_sus,
            res_1.municipio_uf,
            res_1.execucao_data_hora_unica,
            res_1.lista_nominal,
            res_1.registros,
                CASE
                    WHEN res_1.registros > 0 AND lag(res_1.registros) OVER (PARTITION BY res_1.municipio_id_sus, res_1.lista_nominal ORDER BY res_1.execucao_data_hora_unica) > 0 THEN (res_1.registros - lag(res_1.registros) OVER (PARTITION BY res_1.municipio_id_sus, res_1.lista_nominal ORDER BY res_1.execucao_data_hora_unica))::numeric
                    ELSE 0::numeric
                END AS delta
           FROM transmissoes_por_municipio_por_lista res_1
          GROUP BY res_1.municipio_id_sus, res_1.municipio_uf, res_1.lista_nominal, res_1.execucao_data_hora_unica, res_1.registros
          ORDER BY res_1.municipio_uf, res_1.lista_nominal, res_1.execucao_data_hora_unica DESC
        )
 SELECT res.municipio_id_sus,
    res.municipio_uf,
    res.execucao_data_hora_unica,
    res.lista_nominal,
    res.registros,
    res.delta,
        CASE
            WHEN res.execucao_data_hora_unica = min(res.execucao_data_hora_unica) OVER (PARTITION BY res.municipio_id_sus, res.lista_nominal) THEN 0::numeric
            ELSE
            CASE
                WHEN lag(res.registros) OVER (PARTITION BY res.municipio_id_sus, res.lista_nominal ORDER BY res.execucao_data_hora_unica) > 0 THEN round(res.delta / lag(res.registros) OVER (PARTITION BY res.municipio_id_sus, res.lista_nominal ORDER BY res.execucao_data_hora_unica)::numeric, 2)
                ELSE 0::numeric
            END
        END AS delta_porcentagem
   FROM transmissoes_unicas_delta res
  WHERE res.lista_nominal <> 'relatorio_mensal_indicadores'::text
WITH DATA;

-- View indexes:
CREATE INDEX monitoramento_transmissoes_registros_variacoes_municipio_id_sus ON configuracoes.monitoramento_transmissoes_registros_variacoes USING btree (municipio_id_sus, execucao_data_hora_unica);