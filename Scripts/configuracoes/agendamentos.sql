-- configuracoes.capturas_agendamentos source

CREATE OR REPLACE VIEW configuracoes.capturas_agendamentos AS
SELECT operacao.id AS operacao_id,
    periodo.id AS periodo_id,
    ug_definicao.tipo AS unidade_geografica_tipo,
    ug_por_projuto.unidade_geografica_id,
    ug_definicao.id_ibge AS unidade_geografica_id_ibge,
    ug_definicao.id_sus AS unidade_geografica_id_sus,
    operacao.tabela_destino,
    (periodo.data_fim + operacao.atraso_divulgacao)::date AS capturar_apos,
    operacao.parametros,
    periodo.data_inicio AS periodo_data_inicio,
    uf.sigla AS uf_sigla,
    periodo.codigo AS periodo_codigo,
    (
        CASE
            WHEN historico.quantidade_registros IS NULL THEN FALSE
            ELSE TRUE
        END
    ) AS atualizacao_retroativa
FROM configuracoes.unidades_geograficas_por_projuto ug_por_projuto
LEFT JOIN   
    configuracoes.capturas_operacoes operacao
ON
    operacao.projuto = ug_por_projuto.projuto
LEFT JOIN
    listas_de_codigos.periodos periodo
ON
    periodo.tipo::text = operacao.periodo_tipo
AND periodo.data_inicio >= operacao.data_minima
AND (
        operacao.desistir_apos IS NULL
    OR  now()::date <= (periodo.data_fim + operacao.desistir_apos)::date
)
AND (operacao.data_maxima IS NULL OR periodo.data_fim <= operacao.data_maxima)
JOIN listas_de_codigos.unidades_geograficas ug_definicao
ON
    ug_por_projuto.unidade_geografica_id = ug_definicao.id
AND operacao.unidade_geografica_tipo = ug_definicao.tipo
LEFT JOIN
    configuracoes.capturas_historico_consolidado historico
ON
    operacao.id = historico.operacao_id
AND ug_por_projuto.unidade_geografica_id = historico.unidade_geografica_id
AND periodo.id = historico.periodo_id
LEFT JOIN
    listas_de_codigos.ufs uf
ON
    ug_definicao.id = uf.id
WHERE
    operacao.ativa
-- Agendar apenas se já tiver passado o tempo mínimo para divulgação dos dados
AND now()::date >= (periodo.data_fim + operacao.atraso_divulgacao)
AND (
    -- Se não houver captura registrada no histórico, agendar!
        historico.quantidade_registros IS NULL
    -- Agendar também capturas adicionais para atualizações retroativas...
    OR  (
        -- ...se estiver configurado para aceitar atualização retroativa...
            operacao.atualizar_retroativo
        -- ...e estiver no intervalo em que essas atualizações são aceitas...
        AND now()::date <= (
            periodo.data_fim + operacao.atualizar_retroativo_desistir_apos
        )::date
        -- ...e o intervalo desde a última captura for maior do que o mínimo
        -- especificado
        AND now()::date >= (
            historico.atualizado_em
            + operacao.atualizar_retroativo_desistir_apos
        )::date
    )
)
ORDER BY
    operacao.tabela_destino,
    operacao.id,
    ((periodo.data_fim + operacao.atraso_divulgacao)::date),
    ug_definicao.id_ibge,
    ug_definicao.id_sus,
    ug_por_projuto.unidade_geografica_id
;