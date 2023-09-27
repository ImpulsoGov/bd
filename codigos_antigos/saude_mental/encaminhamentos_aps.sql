/******************************************************************************
 *                                                                            *
 *                ENCAMINHAMENTOS DA APS PARA AMBULATÓRIOS                    *
 *                                                                            *
 *                                                                            *
 ******************************************************************************/


DROP MATERIALIZED VIEW IF EXISTS
    saude_mental._aps_encaminhamentos_especializada
CASCADE;
CREATE MATERIALIZED VIEW 
    saude_mental._aps_encaminhamentos_especializada
AS
WITH
saude_mental_encaminhamentos_especializada AS (
    SELECT
        unidade_geografica_id,
        periodo_id,
        sum(quantidade_registrada) AS quantidade_registrada,
        'Encaminhamento para serviço especializado' AS conduta
    FROM dados_publicos.sisab_producao_municipios_por_conduta_por_problema_condicao_ava
    WHERE 
        conduta  = 'Encaminhamento p/ serviço especializado'
    AND problema_condicao_avaliada 
        = ANY (ARRAY[
            'Saúde mental'::text,
            'Usuário de álcool'::text,
            'Usuário de outras drogas'::TEXT
        ])
    GROUP BY 
        unidade_geografica_id,
        periodo_id
),
saude_mental_todas_condutas AS (
    SELECT
        unidade_geografica_id,
        periodo_id,
        sum(quantidade_registrada) AS quantidade_registrada,
        'Todas' AS conduta
    FROM dados_publicos.sisab_producao_municipios_por_conduta_por_problema_condicao_ava
    WHERE problema_condicao_avaliada = ANY (ARRAY[
        'Saúde mental',
        'Usuário de álcool',
        'Usuário de outras drogas'
    ])
    GROUP BY 
        unidade_geografica_id,
        periodo_id
),
saude_mental_condutas AS (
    SELECT * FROM saude_mental_encaminhamentos_especializada
    UNION
    SELECT * FROM saude_mental_todas_condutas
)
SELECT 
    saude_mental_condutas.unidade_geografica_id,
    unidade_geografica.id_sus AS unidade_geografica_id_sus,
    periodo.data_inicio AS competencia,
    saude_mental_condutas.periodo_id,
    saude_mental_condutas.conduta,
    saude_mental_condutas.quantidade_registrada
FROM saude_mental_condutas
LEFT JOIN listas_de_codigos.periodos periodo 
ON saude_mental_condutas.periodo_id = periodo.id
LEFT JOIN listas_de_codigos.unidades_geograficas unidade_geografica
ON saude_mental_condutas.unidade_geografica_id = unidade_geografica.id
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    _aps_encaminhamentos_especializada_un
ON saude_mental._aps_encaminhamentos_especializada (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    conduta,
    competencia DESC,
    periodo_id
);


DROP MATERIALIZED VIEW IF EXISTS
    saude_mental.aps_encaminhamentos_especializada
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.aps_encaminhamentos_especializada
AS
SELECT
    coalesce(
        competencia_atual.unidade_geografica_id,
        competencia_anterior.unidade_geografica_id
    ) AS unidade_geografica_id,
    coalesce(
        competencia_atual.unidade_geografica_id_sus,
        competencia_anterior.unidade_geografica_id_sus
    ) AS unidade_geografica_id_sus,
    competencia_atual.periodo_id,
    competencia_atual.competencia,
    listas_de_codigos.nome_mes(competencia_atual.competencia) AS nome_mes,
    coalesce(
        competencia_atual.conduta,
        competencia_anterior.conduta
    ) AS conduta,
    coalesce(
        competencia_atual.quantidade_registrada,
        0
    ) AS quantidade_registrada,
    coalesce(
        competencia_anterior.quantidade_registrada,
        0
    ) AS quantidade_registrada_anterior
FROM saude_mental._aps_encaminhamentos_especializada competencia_atual
LEFT JOIN listas_de_codigos.periodos_sucessao sucessao
ON 
    competencia_atual.periodo_id = sucessao.periodo_id 
AND sucessao.periodo_tipo::text = 'Mensal'::text
FULL JOIN saude_mental._aps_encaminhamentos_especializada competencia_anterior
ON 
    sucessao.ultimo_periodo_id = competencia_anterior.periodo_id
AND competencia_atual.unidade_geografica_id
    = competencia_anterior.unidade_geografica_id
AND competencia_atual.conduta = competencia_anterior.conduta
WHERE competencia_atual.quantidade_registrada IS NOT NULL
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    aps_encaminhamentos_especializada_un
ON saude_mental.aps_encaminhamentos_especializada (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    conduta,
    competencia DESC,
    periodo_id
);



DROP MATERIALIZED VIEW IF EXISTS 
    saude_mental.aps_encaminhamentos_especializada_resumo_ultimo_mes
CASCADE;
CREATE MATERIALIZED VIEW 
    saude_mental.aps_encaminhamentos_especializada_resumo_ultimo_mes 
AS
WITH
relacao_aps_especializada_ultimo_mes AS (
SELECT
    DISTINCT ON (
        unidade_geografica_id,
        unidade_geografica_id_sus,
        conduta
    )
    *
FROM saude_mental.aps_encaminhamentos_especializada
ORDER BY 
    unidade_geografica_id,
    unidade_geografica_id_sus,
    conduta,
    competencia DESC
),
relacao_aps_especializada_ultimo_mes_horizontalizado AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        competencia,
        nome_mes,
        sum(quantidade_registrada) FILTER (
            WHERE conduta = 'Encaminhamento para serviço especializado'
        ) AS encaminhamentos_especializada,
        sum(quantidade_registrada_anterior) FILTER (
            WHERE conduta = 'Encaminhamento para serviço especializado'
        ) AS encaminhamentos_especializada_anterior,
        sum(quantidade_registrada) FILTER (
            WHERE conduta = 'Todas'
        ) AS atendimentos_sm_aps,
        sum(quantidade_registrada_anterior) FILTER (
            WHERE conduta = 'Todas'
        ) AS atendimentos_sm_aps_anterior
    FROM relacao_aps_especializada_ultimo_mes
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        competencia,
        nome_mes
)
SELECT
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    competencia,
    nome_mes,
    encaminhamentos_especializada,
    atendimentos_sm_aps,
    round(
        100 * encaminhamentos_especializada
        / nullif(atendimentos_sm_aps, 0),
        1
    ) AS perc_encaminhamentos_especializada,
    (
        encaminhamentos_especializada - encaminhamentos_especializada_anterior
    ) AS dif_encaminhamentos_especializada_anterior
FROM relacao_aps_especializada_ultimo_mes_horizontalizado
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    aps_encaminhamentos_especializada_ultimo_mes_un
ON saude_mental.aps_encaminhamentos_especializada_resumo_ultimo_mes (
    unidade_geografica_id,
    unidade_geografica_id_sus
);



-- Criar versão "verticalizada" do resumo de encaminhamentos de atendimentos em
-- Saúde Mental da APS para a rede especializada
DROP MATERIALIZED VIEW IF EXISTS
    saude_mental.aps_encaminhamentos_especializada_resumo_ultimo_mes_vertical
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.aps_encaminhamentos_especializada_resumo_ultimo_mes_vertical
AS
SELECT
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    competencia,
    nome_mes,
    'Sim' AS encaminhamento,
    (perc_encaminhamentos_especializada) / 100 AS prop_atendimentos
FROM saude_mental.aps_encaminhamentos_especializada_resumo_ultimo_mes
UNION
SELECT
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    competencia,
    nome_mes,
    'Não' AS encaminhado,
    (100 - perc_encaminhamentos_especializada) / 100 AS prop_atendimentos
FROM saude_mental.aps_encaminhamentos_especializada_resumo_ultimo_mes
WITH NO DATA;
CREATE INDEX IF NOT EXISTS
    aps_encaminhamentos_especializada_resumo_ultimo_mes_vertical_ix
ON saude_mental.aps_encaminhamentos_especializada_resumo_ultimo_mes_vertical (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    competencia
);




/* ---------------------------------------------------------------------------*/
/*                                                                            */
/*                   ENCAMINHAMENTOS DA APS PARA CAPS                         */
/*                                                                            */
/* ---------------------------------------------------------------------------*/



DROP MATERIALIZED VIEW IF EXISTS
    saude_mental._aps_encaminhamentos_caps
CASCADE;
CREATE MATERIALIZED VIEW saude_mental._aps_encaminhamentos_caps
AS
WITH
saude_mental_encaminhamentos_caps AS (
    SELECT
        unidade_geografica_id,
        periodo_id,
        sum(quantidade_registrada) AS quantidade_registrada,
        'Encaminhamento para CAPS' AS conduta
    FROM dados_publicos.sisab_producao_municipios_por_conduta_por_problema_condicao_ava
    WHERE 
        conduta  = 'Encaminhamento p/ CAPS'
    AND problema_condicao_avaliada 
        = ANY (ARRAY[
            'Saúde mental'::text,
            'Usuário de álcool'::text,
            'Usuário de outras drogas'::TEXT
        ])
    GROUP BY 
        unidade_geografica_id,
        periodo_id
),
saude_mental_todas_condutas AS (
    SELECT
        unidade_geografica_id,
        periodo_id,
        sum(quantidade_registrada) AS quantidade_registrada,
        'Todas' AS conduta
    FROM dados_publicos.sisab_producao_municipios_por_conduta_por_problema_condicao_ava
    WHERE problema_condicao_avaliada = ANY (ARRAY[
        'Saúde mental',
        'Usuário de álcool',
        'Usuário de outras drogas'
    ])
    GROUP BY 
        unidade_geografica_id,
        periodo_id
),
saude_mental_condutas AS (
    SELECT * FROM saude_mental_encaminhamentos_caps
    UNION
    SELECT * FROM saude_mental_todas_condutas
)
SELECT 
    saude_mental_condutas.unidade_geografica_id,
    unidade_geografica.id_sus AS unidade_geografica_id_sus,
    periodo.data_inicio AS competencia,
    saude_mental_condutas.periodo_id,
    saude_mental_condutas.conduta,
    saude_mental_condutas.quantidade_registrada
FROM saude_mental_condutas
LEFT JOIN listas_de_codigos.periodos periodo 
ON saude_mental_condutas.periodo_id = periodo.id
LEFT JOIN listas_de_codigos.unidades_geograficas unidade_geografica
ON saude_mental_condutas.unidade_geografica_id = unidade_geografica.id
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS 
    _aps_encaminhamentos_caps_un
ON saude_mental._aps_encaminhamentos_caps (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    conduta,
    competencia DESC,
    periodo_id
);



DROP MATERIALIZED VIEW IF EXISTS
    saude_mental.aps_encaminhamentos_caps
CASCADE;
CREATE MATERIALIZED VIEW saude_mental.aps_encaminhamentos_caps
AS
SELECT
    coalesce(
        competencia_atual.unidade_geografica_id,
        competencia_anterior.unidade_geografica_id
    ) AS unidade_geografica_id,
    coalesce(
        competencia_atual.unidade_geografica_id_sus,
        competencia_anterior.unidade_geografica_id_sus
    ) AS unidade_geografica_id_sus,
    competencia_atual.periodo_id,
    competencia_atual.competencia,
    listas_de_codigos.nome_mes(competencia_atual.competencia) AS nome_mes,
    coalesce(
        competencia_atual.conduta,
        competencia_anterior.conduta
    ) AS conduta,
    coalesce(
        competencia_atual.quantidade_registrada,
        0
    ) AS quantidade_registrada,
    coalesce(
        competencia_anterior.quantidade_registrada,
        0
    ) AS quantidade_registrada_anterior
FROM saude_mental._aps_encaminhamentos_caps competencia_atual
LEFT JOIN listas_de_codigos.periodos_sucessao sucessao
ON
    competencia_atual.periodo_id = sucessao.periodo_id
AND sucessao.periodo_tipo::text = 'Mensal'::text
FULL JOIN saude_mental._aps_encaminhamentos_caps competencia_anterior 
ON 
    sucessao.ultimo_periodo_id = competencia_anterior.periodo_id
AND competencia_atual.unidade_geografica_id 
    = competencia_anterior.unidade_geografica_id
AND competencia_atual.conduta = competencia_anterior.conduta
WHERE competencia_atual.quantidade_registrada IS NOT NULL
WITH NO DATA;
CREATE UNIQUE INDEX 
    aps_encaminhamentos_caps_un
ON saude_mental.aps_encaminhamentos_caps (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    competencia,
    conduta
);



DROP MATERIALIZED VIEW IF EXISTS 
    saude_mental.aps_encaminhamentos_caps_resumo_ultimo_mes
CASCADE;
CREATE MATERIALIZED VIEW 
    saude_mental.aps_encaminhamentos_caps_resumo_ultimo_mes 
AS
WITH
relacao_aps_caps_ultimo_mes AS (
SELECT
    DISTINCT ON (
        unidade_geografica_id,
        unidade_geografica_id_sus,
        conduta
    )
    *
FROM saude_mental.aps_encaminhamentos_caps
ORDER BY 
    unidade_geografica_id,
    unidade_geografica_id_sus,
    conduta,
    competencia DESC
),
relacao_aps_caps_ultimo_mes_horizontalizado AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        competencia,
        nome_mes,
        sum(quantidade_registrada) FILTER (
            WHERE conduta = 'Encaminhamento para CAPS'
        ) AS encaminhamentos_caps,
        sum(quantidade_registrada_anterior) FILTER (
            WHERE conduta = 'Encaminhamento para CAPS'
        ) AS encaminhamentos_caps_anterior,
        sum(quantidade_registrada) FILTER (
            WHERE conduta = 'Todas'
        ) AS atendimentos_sm_aps,
        sum(quantidade_registrada_anterior) FILTER (
            WHERE conduta = 'Todas'
        ) AS atendimentos_sm_aps_anterior
    FROM relacao_aps_caps_ultimo_mes
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        competencia,
        nome_mes
)
SELECT
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    competencia,
    nome_mes,
    encaminhamentos_caps,
    atendimentos_sm_aps,
    round(
        100 * encaminhamentos_caps
        / nullif(atendimentos_sm_aps, 0),
        2
    ) AS perc_encaminhamentos_caps,
    (
        encaminhamentos_caps - encaminhamentos_caps_anterior
    ) AS dif_encaminhamentos_caps_anterior
FROM relacao_aps_caps_ultimo_mes_horizontalizado
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    aps_encaminhamentos_caps_ultimo_mes_un
ON saude_mental.aps_encaminhamentos_caps_resumo_ultimo_mes (
    unidade_geografica_id,
    unidade_geografica_id_sus
);



-- Criar versão "verticalizada" do resumo de encaminhamentos de atendimentos em
-- Saúde Mental da APS para a CAPS
DROP MATERIALIZED VIEW IF EXISTS
    saude_mental.aps_encaminhamentos_caps_resumo_ultimo_mes_vertical
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.aps_encaminhamentos_caps_resumo_ultimo_mes_vertical
AS
SELECT
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    competencia,
    nome_mes,
    'Sim' AS encaminhamento,
    (perc_encaminhamentos_caps) / 100 AS prop_atendimentos
FROM saude_mental.aps_encaminhamentos_caps_resumo_ultimo_mes
UNION
SELECT
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    competencia,
    nome_mes,
    'Não' AS encaminhado,
    (100 - perc_encaminhamentos_caps) / 100 AS prop_atendimentos
FROM saude_mental.aps_encaminhamentos_caps_resumo_ultimo_mes
WITH NO DATA;
CREATE INDEX IF NOT EXISTS
    aps_encaminhamentos_caps_resumo_ultimo_mes_vertical_ix
ON saude_mental.aps_encaminhamentos_caps_resumo_ultimo_mes_vertical (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    competencia
);
