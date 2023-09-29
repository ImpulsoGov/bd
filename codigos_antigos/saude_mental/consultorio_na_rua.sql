/******************************************************************************
 *                                                                            *
 *                                                                            *
 *                ATENDIMENTOS INDIVIDUAIS DO CONSULTÓRIO NA RUA              *
 *                                                                            *
 *                                                                            *
 ******************************************************************************/


DROP MATERIALIZED VIEW IF EXISTS
    saude_mental._consultorio_na_rua_atendimentos
CASCADE;
DROP MATERIALIZED VIEW IF EXISTS 
    saude_mental._consultorio_na_rua_atendimentos
CASCADE;
CREATE MATERIALIZED VIEW saude_mental._consultorio_na_rua_atendimentos
AS 
WITH atendimentos AS (
    SELECT
        unidade_geografica_id,
        periodo_id,
        tipo_equipe,
        tipo_producao,
        sum(quantidade_registrada) AS quantidade_registrada
    FROM dados_publicos.sisab_producao_municipios_por_tipo_equipe_por_tipo_producao
    WHERE
        tipo_equipe = 'Eq. Consultório na Rua - ECR'
    AND tipo_producao = 'Atendimento Individual'
    GROUP BY
        unidade_geografica_id,
        periodo_id,
        tipo_equipe,
        tipo_producao
)
SELECT
    atendimentos.unidade_geografica_id,
    unidade_geografica.id_sus AS unidade_geografica_id_sus,
    periodo.data_inicio AS competencia,
    periodo.id AS periodo_id,
    atendimentos.tipo_equipe,
    atendimentos.tipo_producao,
    atendimentos.quantidade_registrada
FROM atendimentos
LEFT JOIN listas_de_codigos.periodos periodo
ON 
    atendimentos.periodo_id = periodo.id
AND periodo.tipo = 'Mensal'
LEFT JOIN listas_de_codigos.unidades_geograficas unidade_geografica
ON atendimentos.unidade_geografica_id = unidade_geografica.id
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS 
	_consultorio_na_rua_atendimentos_un
ON saude_mental._consultorio_na_rua_atendimentos (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    tipo_equipe,
    tipo_producao
);


DROP MATERIALIZED VIEW IF EXISTS 
    saude_mental.consultorio_na_rua_atendimentos
CASCADE;
CREATE MATERIALIZED VIEW saude_mental.consultorio_na_rua_atendimentos
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
    coalesce(
        competencia_atual.competencia::timestamp without time zone,
        date_trunc(
            'month'::text,
            competencia_anterior.competencia + '1 mon'::INTERVAL
        )
    ) AS competencia,
    listas_de_codigos.nome_mes(
        coalesce(
            competencia_atual.competencia::timestamp without time zone,
            competencia_anterior.competencia + '1 mon'::INTERVAL
        )::date
    ) AS nome_mes,
    coalesce(
        competencia_atual.tipo_equipe,
        competencia_anterior.tipo_equipe
    ) AS tipo_equipe,
    coalesce(
        competencia_atual.tipo_producao,
        competencia_anterior.tipo_producao
    ) AS tipo_producao,
    coalesce(
        competencia_atual.quantidade_registrada,
        0::bigint
    ) AS quantidade_registrada,
    coalesce(
        competencia_anterior.quantidade_registrada,
        0::bigint
    ) AS quantidade_registrada_anterior,
    (
        coalesce(competencia_atual.quantidade_registrada, 0)
        - coalesce(competencia_anterior.quantidade_registrada, 0)
    ) AS dif_quantidade_registrada_anterior
FROM saude_mental._consultorio_na_rua_atendimentos competencia_atual
LEFT JOIN listas_de_codigos.periodos_sucessao sucessao
ON 
    competencia_atual.periodo_id = sucessao.periodo_id
AND sucessao.periodo_tipo::text = 'Mensal'::text
FULL JOIN saude_mental._consultorio_na_rua_atendimentos competencia_anterior
ON
 	competencia_atual.unidade_geografica_id
 	= competencia_anterior.unidade_geografica_id
AND sucessao.ultimo_periodo_id = competencia_anterior.periodo_id
AND competencia_atual.tipo_equipe = competencia_anterior.tipo_equipe
AND competencia_atual.tipo_producao = competencia_anterior.tipo_producao
WHERE competencia_atual.quantidade_registrada IS NOT NULL
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
	consultorio_na_rua_atendimentos_un
ON saude_mental.consultorio_na_rua_atendimentos (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    tipo_equipe,
    tipo_producao
);

 
DROP MATERIALIZED VIEW IF EXISTS
    saude_mental.consultorio_na_rua_atendimentos_ultimo_mes
CASCADE;
CREATE MATERIALIZED VIEW 
    saude_mental.consultorio_na_rua_atendimentos_ultimo_mes
AS
SELECT 
    DISTINCT ON (
        unidade_geografica_id
    )
    *
FROM saude_mental.consultorio_na_rua_atendimentos
ORDER BY 
    unidade_geografica_id,
    unidade_geografica_id_sus,
    competencia DESC
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
	consultorio_na_rua_atendimentos_ultimo_mes_un
ON saude_mental.consultorio_na_rua_atendimentos_ultimo_mes (
    unidade_geografica_id,
    unidade_geografica_id_sus
);

 
DROP MATERIALIZED VIEW IF EXISTS
    saude_mental.consultorio_na_rua_atendimentos_ultimos_12meses
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.consultorio_na_rua_atendimentos_ultimos_12meses
AS
WITH cnr_12meses AS (
    SELECT 
        atendimentos.unidade_geografica_id,
        atendimentos.unidade_geografica_id_sus,
        ultimo_mes.periodo_id,
        ultimo_mes.competencia AS ate_competencia,
        min(atendimentos.competencia) AS a_partir_de_competencia,
        atendimentos.tipo_equipe,
        atendimentos.tipo_producao,
        sum(atendimentos.quantidade_registrada) AS quantidade_registrada
    FROM saude_mental.consultorio_na_rua_atendimentos atendimentos
    INNER JOIN 
        saude_mental.consultorio_na_rua_atendimentos_ultimo_mes ultimo_mes
    ON 
        ultimo_mes.unidade_geografica_id = atendimentos.unidade_geografica_id 
    AND ultimo_mes.tipo_producao = atendimentos.tipo_producao
    AND ultimo_mes.tipo_equipe = atendimentos.tipo_equipe
    AND atendimentos.competencia 
        > date_trunc('month', ultimo_mes.competencia - '12 months'::interval)
    GROUP BY
        atendimentos.unidade_geografica_id,
        atendimentos.unidade_geografica_id_sus,
        ultimo_mes.periodo_id,
        ultimo_mes.competencia,
        atendimentos.tipo_equipe,
        atendimentos.tipo_producao
),
cnr_24a12meses AS (
    SELECT 
        atendimentos.unidade_geografica_id,
        atendimentos.unidade_geografica_id_sus,
        atendimentos.tipo_equipe,
        atendimentos.tipo_producao,
        sum(atendimentos.quantidade_registrada) AS quantidade_registrada
    FROM saude_mental.consultorio_na_rua_atendimentos atendimentos
    INNER JOIN 
        saude_mental.consultorio_na_rua_atendimentos_ultimo_mes ultimo_mes
    ON 
        ultimo_mes.unidade_geografica_id = atendimentos.unidade_geografica_id 
    AND ultimo_mes.tipo_producao = atendimentos.tipo_producao
    AND ultimo_mes.tipo_equipe = atendimentos.tipo_equipe
    AND atendimentos.competencia 
        <= date_trunc('month', ultimo_mes.competencia - '12 months'::interval)
    AND atendimentos.competencia 
        > date_trunc('month', ultimo_mes.competencia - '24 months'::interval)
    GROUP BY
        atendimentos.unidade_geografica_id,
        atendimentos.unidade_geografica_id_sus,
        atendimentos.tipo_equipe,
        atendimentos.tipo_producao
)
SELECT
	coalesce(
        cnr_12meses.unidade_geografica_id,
        cnr_24a12meses.unidade_geografica_id
    ) AS unidade_geografica_id,
	coalesce(
        cnr_12meses.unidade_geografica_id_sus,
        cnr_24a12meses.unidade_geografica_id_sus
    ) AS unidade_geografica_id_sus,
  	coalesce(
        cnr_12meses.tipo_equipe,
        cnr_24a12meses.tipo_equipe
    ) AS tipo_equipe,
  	coalesce(
        cnr_12meses.tipo_producao,
        cnr_24a12meses.tipo_producao
    ) AS tipo_producao,
  	EXTRACT(
        YEAR FROM cnr_12meses.a_partir_de_competencia
    )::text AS a_partir_do_ano,
  	listas_de_codigos.nome_mes(
      	cnr_12meses.a_partir_de_competencia::date
    ) AS a_partir_do_mes,
  	EXTRACT(
        YEAR FROM cnr_12meses.ate_competencia
    )::text AS ate_ano,
  	listas_de_codigos.nome_mes(cnr_12meses.ate_competencia::date) AS ate_mes,
  	coalesce(cnr_12meses.quantidade_registrada, 0) AS quantidade_registrada,
    coalesce(
        cnr_24a12meses.quantidade_registrada,
        0
    ) AS quantidade_registrada_anterior,
    (
        coalesce(cnr_12meses.quantidade_registrada, 0)
        - coalesce(cnr_24a12meses.quantidade_registrada, 0)
    ) AS dif_quantidade_registrada_anterior
FROM cnr_12meses
FULL JOIN cnr_24a12meses
ON
    cnr_12meses.unidade_geografica_id = cnr_24a12meses.unidade_geografica_id
AND cnr_12meses.tipo_producao = cnr_24a12meses.tipo_producao
AND cnr_12meses.tipo_equipe = cnr_24a12meses.tipo_equipe
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
	consultorio_na_rua_atendimentos_ultimos_12meses_un
ON saude_mental.consultorio_na_rua_atendimentos_ultimos_12meses (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    tipo_equipe,
    tipo_producao
);
