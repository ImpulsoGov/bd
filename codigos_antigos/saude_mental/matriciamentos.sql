/******************************************************************************

          MATRICIAMENTOS ENTRE EQUIPES DE CAPS E DE APS

 ******************************************************************************/

DROP MATERIALIZED VIEW IF EXISTS
    saude_mental.matriciamentos_meta_por_caps_ultimo_ano
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.matriciamentos_meta_por_caps_ultimo_ano
AS
WITH
procedimentos_ano AS (
    SELECT 
        procedimento.*,
        ultima_competencia.periodo_data_inicio AS competencia
    FROM 
        dados_publicos.siasus_procedimentos_ambulatoriais
        AS procedimento
    LEFT JOIN
        -- última competência com dados
        saude_mental._procedimentos_ultima_competencia_disponivel
        AS ultima_competencia
    USING
        (unidade_geografica_id, periodo_id)
    WHERE
        -- apenas procedimentos que aconteceram no ano da última competência
        date_part('year', procedimento.realizacao_periodo_data_inicio)
        = date_part('year', ultima_competencia.periodo_data_inicio)
),
matriciamentos_por_caps_ultimo_ano AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        max(competencia) AS competencia,
        estabelecimento_id_cnes,
        date_part('month', max(competencia)) AS meses_decorridos,
        sum(quantidade_apresentada) FILTER (
            WHERE procedimento_id_sigtap = '0301080305'  -- MATRICIAMENTO C/ APS
        ) AS quantidade_registrada
    FROM procedimentos_ano
    WHERE
        estabelecimento_tipo_id_sigtap = '70'  -- CAPS
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_id_cnes
)
SELECT
    matriciamento.unidade_geografica_id,
    matriciamento.unidade_geografica_id_sus,
    matriciamento.competencia,
    date_part('year', matriciamento.competencia)::text AS ano,
    listas_de_codigos.nome_mes(matriciamento.competencia) AS ate_mes,
    coalesce(
        estabelecimento.nome_curto,
        estabelecimento.nome
    ) AS estabelecimento,
    coalesce(matriciamento.quantidade_registrada, 0) AS quantidade_registrada,
    -- É pactuado que cada CAPS faça pelo menos 24 matriciamentos anuais;
    -- se um CAPS fizer mais do que isso, o campo `faltam_no_ano` deve ser 0
    greatest(
        0,
        24 - coalesce(matriciamento.quantidade_registrada, 0)
    ) AS faltam_no_ano,
    round(
        (
            greatest(0, 24 - coalesce(matriciamento.quantidade_registrada, 0))
            / nullif(12 - matriciamento.meses_decorridos, 0)::numeric
        ),
        1
    ) AS media_mensal_para_meta
FROM matriciamentos_por_caps_ultimo_ano matriciamento
LEFT JOIN listas_de_codigos.estabelecimentos estabelecimento
ON matriciamento.estabelecimento_id_cnes = estabelecimento.id_scnes
WITH NO DATA
;
CREATE UNIQUE INDEX IF NOT EXISTS
    matriciamentos_meta_por_caps_ultimo_ano_un    
ON saude_mental.matriciamentos_meta_por_caps_ultimo_ano (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    competencia,
    estabelecimento
);

DROP MATERIALIZED VIEW IF EXISTS
    saude_mental.matriciamentos_meta_ultimo_ano
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.matriciamentos_meta_ultimo_ano
AS
SELECT
    unidade_geografica_id,
    unidade_geografica_id_sus,
    date_part('year', max(competencia))::text AS ano,
    listas_de_codigos.nome_mes(max(competencia)) AS ate_mes,
    sum(quantidade_registrada) AS quantidade_registrada,
    count(DISTINCT estabelecimento) FILTER (
        WHERE media_mensal_para_meta <= 2
    ) AS estabelecimentos_na_meta,
    count(DISTINCT estabelecimento) FILTER (
        WHERE media_mensal_para_meta > 2
    ) AS estabelecimentos_fora_meta
FROM saude_mental.matriciamentos_meta_por_caps_ultimo_ano
GROUP BY
    unidade_geografica_id,
    unidade_geografica_id_sus
WITH NO DATA
;
CREATE UNIQUE INDEX IF NOT EXISTS
    matriciamentos_meta_ultimo_ano_un
ON saude_mental.matriciamentos_meta_ultimo_ano (
    unidade_geografica_id,
    unidade_geografica_id_sus
);
