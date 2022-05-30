/**************************************************************************

		CÁLCULO DE PRODUÇÃO POR ESTABELECIMENTO E POR PROFISSIONAL


 **************************************************************************/


CREATE INDEX IF NOT EXISTS
    siasus_procedimentos_ambulatoriais_unidade_geografica_id_idx
ON dados_publicos.siasus_procedimentos_ambulatoriais (unidade_geografica_id);
CREATE INDEX IF NOT EXISTS
    siasus_procedimentos_ambulatoriais_ocupacao_id_cbo_idx
ON dados_publicos.siasus_procedimentos_ambulatoriais (profissional_ocupacao_id_cbo);

CREATE INDEX IF NOT EXISTS
    cnes_vinculos_disseminacao_periodo_id_idx
ON dados_publicos.cnes_vinculos_disseminacao (periodo_id);
CREATE INDEX IF NOT EXISTS
    cnes_vinculos_disseminacao_estabelecimento_id_cnes_idx
ON dados_publicos.cnes_vinculos_disseminacao (estabelecimento_id_cnes);


CREATE INDEX IF NOT EXISTS
    cnes_vinculos_disseminacao_ug_x_periodo_x_estabelecimento_x_cbo_idx
ON dados_publicos.cnes_vinculos_disseminacao (
    unidade_geografica_id,
    periodo_id,
    estabelecimento_id_cnes,
    ocupacao_id_cbo
);


DROP MATERIALIZED VIEW IF EXISTS
    saude_mental._procedimentos_por_ocupacao_por_mes
CASCADE;
CREATE MATERIALIZED VIEW 
    saude_mental._procedimentos_por_ocupacao_por_mes
AS 
SELECT 
    unidade_geografica_id,
    unidade_geografica_id_sus,
    realizacao_periodo_data_inicio AS competencia,
    periodo_id,
    estabelecimento_id_cnes,
    profissional_ocupacao_id_cbo AS ocupacao_id_cbo,
    sum(quantidade_apresentada) FILTER (
        WHERE instrumento_registro_id_siasus IN ('A', 'B')
    ) AS procedimentos_registrados_raas,
    sum(quantidade_apresentada) FILTER (
        WHERE instrumento_registro_id_siasus IN ('C', 'I')
    ) AS procedimentos_registrados_bpa
FROM dados_publicos.siasus_procedimentos_ambulatoriais
WHERE estabelecimento_tipo_id_sigtap = '70'  -- CAPS
GROUP BY 
    unidade_geografica_id,
    unidade_geografica_id_sus,
    realizacao_periodo_data_inicio,
    periodo_id,
    estabelecimento_id_cnes,
    profissional_ocupacao_id_cbo
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    _procedimentos_por_ocupacao_por_mes_un
ON saude_mental._procedimentos_por_ocupacao_por_mes (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    competencia DESC,
    periodo_id,
    estabelecimento_id_cnes,
    ocupacao_id_cbo
);

CREATE INDEX IF NOT EXISTS
    cnes_vinculos_disseminacao_ug_x_competencia_x_estabelecimento_x_cbo_idx
ON dados_publicos.cnes_vinculos_disseminacao (
    unidade_geografica_id,
    periodo_id,
    estabelecimento_id_cnes,
    ocupacao_id_cbo
);


-- Cruzar o número de procedimentos registrados com o número de horas trabalhadas por cada categoria profissional em cada estabelecimento
DROP MATERIALIZED VIEW IF EXISTS
    saude_mental._procedimentos_por_hora_por_ocupacao_por_estabelecimento
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental._procedimentos_por_hora_por_ocupacao_por_estabelecimento 
AS
WITH disponibilidade_profissionais_por_ocupacao AS (
    SELECT 
        unidade_geografica_id,
        periodo_id,
        estabelecimento_id_cnes,
        ocupacao_id_cbo,
        sum(
            vinculos_profissionais.atendimento_carga_ambulatorial
            + vinculos_profissionais.atendimento_carga_outras
        ) / 5 AS horas_disponibilidade_diaria
    FROM dados_publicos.cnes_vinculos_disseminacao vinculos_profissionais
    GROUP BY 
        unidade_geografica_id,
        periodo_id,
        estabelecimento_id_cnes,
        ocupacao_id_cbo
)
SELECT
	procedimentos.unidade_geografica_id,
	procedimentos.unidade_geografica_id_sus,
	procedimentos.competencia,
    procedimentos.periodo_id,
    procedimentos.estabelecimento_id_cnes,
    procedimentos.ocupacao_id_cbo,
    coalesce(
        procedimentos.procedimentos_registrados_raas,
        0
    ) AS procedimentos_registrados_raas,
    coalesce(
        procedimentos.procedimentos_registrados_bpa,
        0
    ) AS procedimentos_registrados_bpa,
    (
        coalesce(procedimentos_registrados_raas, 0)
        + coalesce(procedimentos_registrados_bpa, 0)
    ) AS procedimentos_registrados_total,
    listas_de_codigos.datas_diferenca_dias_uteis(
        sucessao.periodo_data_inicio,
        sucessao.proximo_periodo_data_inicio
    ) * horas_disponibilidade_diaria AS horas_disponibilidade_profissionais
FROM saude_mental._procedimentos_por_ocupacao_por_mes procedimentos
LEFT JOIN disponibilidade_profissionais_por_ocupacao
USING (
    unidade_geografica_id,
    periodo_id,
    estabelecimento_id_cnes,
    ocupacao_id_cbo
)
LEFT JOIN listas_de_codigos.periodos_sucessao sucessao
ON
    sucessao.periodo_tipo = 'Mensal'
AND procedimentos.periodo_id = sucessao.periodo_id
AND disponibilidade_profissionais_por_ocupacao.periodo_id = sucessao.periodo_id
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    _procedimentos_por_hora_por_ocupacao_por_estabelecimento_un
ON saude_mental._procedimentos_por_hora_por_ocupacao_por_estabelecimento (
    unidade_geografica_id,
    estabelecimento_id_cnes,
    periodo_id,
    ocupacao_id_cbo
);
CREATE INDEX IF NOT EXISTS
    _procedimentos_por_hora_por_ocupacao_ocupacao_idx
ON saude_mental._procedimentos_por_hora_por_ocupacao_por_estabelecimento (
    ocupacao_id_cbo
);



-- TODO: lidar melhor com meses em que uma ocupação está faltante 
-- (atualmente, não aparece naquele mês, e a comparação no mês seguinte fica 
-- como NULL )
DROP MATERIALIZED VIEW IF EXISTS 
    saude_mental.procedimentos_por_hora_resumo
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.procedimentos_por_hora_resumo
AS
WITH 
producao_por_ocupacao_todos_estabelecimentos AS (
	SELECT
 		unidade_geografica_id,
		unidade_geografica_id_sus,
		competencia,
	    periodo_id,
		'0000000' AS estabelecimento_id_cnes,
    	ocupacao_id_cbo,
	    sum(procedimentos_registrados_raas) AS procedimentos_registrados_raas,
	    sum(procedimentos_registrados_bpa) AS procedimentos_registrados_bpa,
	    sum(procedimentos_registrados_total) AS procedimentos_registrados_total,
	    sum(
	       horas_disponibilidade_profissionais
	   ) AS horas_disponibilidade_profissionais
	FROM saude_mental._procedimentos_por_hora_por_ocupacao_por_estabelecimento
	GROUP BY
 		unidade_geografica_id,
		unidade_geografica_id_sus,
		competencia,
	    periodo_id,
    	ocupacao_id_cbo
),
producao_por_estabelecimento_todas_ocupacoes AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        estabelecimento_id_cnes,
        '000000' AS ocupacao_id_cbo,
        sum(procedimentos_registrados_raas) AS procedimentos_registrados_raas,
        sum(procedimentos_registrados_bpa) AS procedimentos_registrados_bpa,
        sum(procedimentos_registrados_total) AS procedimentos_registrados_total,
        sum(
            horas_disponibilidade_profissionais)
        AS horas_disponibilidade_profissionais
    FROM saude_mental._procedimentos_por_hora_por_ocupacao_por_estabelecimento
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        estabelecimento_id_cnes
),
producao_todos_estabelecimentos_todas_ocupacoes AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        '0000000' estabelecimento_id_cnes,
        '000000' AS ocupacao_id_cbo,
        sum(procedimentos_registrados_raas) AS procedimentos_registrados_raas,
        sum(procedimentos_registrados_bpa) AS procedimentos_registrados_bpa,
        sum(procedimentos_registrados_total) AS procedimentos_registrados_total,
        sum(
            horas_disponibilidade_profissionais
        ) AS horas_disponibilidade_profissionais
    FROM saude_mental._procedimentos_por_hora_por_ocupacao_por_estabelecimento
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id
),
producao AS (
	SELECT *
	FROM saude_mental._procedimentos_por_hora_por_ocupacao_por_estabelecimento
	UNION 
	SELECT *
	FROM producao_por_ocupacao_todos_estabelecimentos
	UNION
	SELECT *
	FROM producao_por_estabelecimento_todas_ocupacoes
	UNION
	SELECT *
	FROM producao_todos_estabelecimentos_todas_ocupacoes
),
producao_comparacao_anterior AS (
    SELECT
    	coalesce(
    	   competencia_atual.unidade_geografica_id,
    	   competencia_anterior.unidade_geografica_id
    	) AS unidade_geografica_id,
    	coalesce(
    	   competencia_atual.unidade_geografica_id_sus,
    	   competencia_anterior.unidade_geografica_id_sus
    	) AS unidade_geografica_id_sus,
    	sucessao.periodo_data_inicio AS competencia,
        sucessao.periodo_id,
        coalesce(
            competencia_atual.estabelecimento_id_cnes,
            competencia_anterior.estabelecimento_id_cnes
        ) AS estabelecimento_id_cnes,
        coalesce(
            competencia_atual.ocupacao_id_cbo,
            competencia_anterior.ocupacao_id_cbo
        ) AS ocupacao_id_cbo,
        coalesce(
            competencia_atual.procedimentos_registrados_raas,
            0
        ) AS procedimentos_registrados_raas,
        coalesce(
            competencia_atual.procedimentos_registrados_bpa,
            0) AS procedimentos_registrados_bpa_i,
        coalesce(
            competencia_atual.procedimentos_registrados_total,
            0
        ) AS procedimentos_registrados_total,
        round(
            competencia_atual.procedimentos_registrados_total::numeric 
            / nullif(competencia_atual.horas_disponibilidade_profissionais, 0),
            2
        ) AS procedimentos_por_hora,
        coalesce(
            competencia_anterior.procedimentos_registrados_raas,
            0
        ) AS procedimentos_registrados_raas_anterior,
        coalesce(
            competencia_anterior.procedimentos_registrados_bpa,
            0
        ) AS procedimentos_registrados_bpa_i_anterior,
        coalesce(
            competencia_anterior.procedimentos_registrados_total,
            0
        ) AS procedimentos_registrados_total_anterior,
        round(
            competencia_anterior.procedimentos_registrados_total::numeric 
            / nullif(
                competencia_anterior.horas_disponibilidade_profissionais,
                0
            ),
            2
        ) AS procedimentos_por_hora_anterior
    FROM producao competencia_atual
    LEFT JOIN listas_de_codigos.periodos_sucessao sucessao
	ON
	    sucessao.periodo_tipo = 'Mensal'
	AND competencia_atual.periodo_id = sucessao.periodo_id
    FULL JOIN producao competencia_anterior
	ON 
	    competencia_atual.unidade_geografica_id
	    = competencia_anterior.unidade_geografica_id
	AND competencia_anterior.periodo_id = sucessao.ultimo_periodo_id
	AND competencia_atual.estabelecimento_id_cnes
	    = competencia_anterior.estabelecimento_id_cnes
	AND competencia_atual.ocupacao_id_cbo
	    = competencia_anterior.ocupacao_id_cbo
	WHERE competencia_atual.periodo_id IS NOT NULL
)
SELECT
    unidade_geografica_id,
    unidade_geografica_id_sus,
    competencia,
    periodo_id,
    saude_mental.classificar_linha_perfil(
        estabelecimento.nome
    ) AS estabelecimento_linha_perfil,
    coalesce(
        estabelecimento.nome_curto,
        estabelecimento.nome,
        'Todos'
    ) AS estabelecimento,
    coalesce(ocupacao.ocupacao_descricao, 'Todas') AS ocupacao,
    procedimentos_registrados_raas,
    procedimentos_registrados_bpa_i,
    procedimentos_registrados_total,
    procedimentos_por_hora,
    (
        procedimentos_registrados_raas
        - procedimentos_registrados_raas_anterior
    ) AS dif_procedimentos_registrados_raas_anterior,
    (
        procedimentos_registrados_bpa_i
        - procedimentos_registrados_bpa_i_anterior
    ) AS dif_procedimentos_registrados_bpa_i_anterior,
    (
        procedimentos_registrados_total
        - procedimentos_registrados_total_anterior
    ) AS dif_procedimentos_registrados_total_anterior,
    round(
        100 * procedimentos_por_hora::numeric
        / nullif(procedimentos_por_hora_anterior, 0),
        1
    ) - 100 AS perc_dif_procedimentos_por_hora_anterior
FROM producao_comparacao_anterior
LEFT JOIN listas_de_codigos.estabelecimentos estabelecimento
ON 
    producao_comparacao_anterior.estabelecimento_id_cnes
    = estabelecimento.id_scnes
-- TODO: trocar tabela de ocupações por uma em `listas_de_codigos`
LEFT JOIN saude_mental.ocupacoes ocupacao
ON producao_comparacao_anterior.ocupacao_id_cbo = ocupacao.ocupacao_id
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    procedimentos_por_hora_resumo_un
ON saude_mental.procedimentos_por_hora_resumo (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    estabelecimento,
    ocupacao,
    competencia DESC,
    periodo_id
);


DROP MATERIALIZED VIEW IF EXISTS 
    saude_mental.procedimentos_por_hora_resumo_ultimo_mes
CASCADE;
CREATE MATERIALIZED VIEW 
    saude_mental.procedimentos_por_hora_resumo_ultimo_mes
AS
SELECT
	DISTINCT ON (
		unidade_geografica_id,
		unidade_geografica_id_sus,
		estabelecimento,
		ocupacao
	)
	*
FROM saude_mental.procedimentos_por_hora_resumo
ORDER BY
	unidade_geografica_id,
	unidade_geografica_id_sus,
	estabelecimento,
	ocupacao,
	competencia DESC
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    procedimentos_por_hora_resumo_ultimo_mes_un
ON saude_mental.procedimentos_por_hora_resumo_ultimo_mes (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    estabelecimento,
    ocupacao,
    competencia DESC
);


CREATE INDEX IF NOT EXISTS
    procedimentos_ambulatoriais_competencia_x_caps_x_procedimento_idx
ON dados_publicos.siasus_procedimentos_ambulatoriais (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    realizacao_periodo_data_inicio,
    periodo_id,
    estabelecimento_id_cnes,
    procedimento_id_sigtap
)
WHERE estabelecimento_tipo_id_sigtap = '70'  -- CAPS
;


DROP MATERIALIZED VIEW IF EXISTS
    saude_mental.procedimentos_realizados_por_tipo
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.procedimentos_realizados_por_tipo
AS
WITH
procedimentos_por_tipo AS (
    SELECT 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        realizacao_periodo_data_inicio AS competencia,
        periodo_id,
        estabelecimento_id_cnes,
        procedimento_id_sigtap,
        coalesce(
            sum(quantidade_apresentada) FILTER (
                WHERE instrumento_registro_id_siasus IN ('A', 'B')
            ),
            0
        ) AS procedimentos_registrados_raas,
        coalesce(
            sum(quantidade_apresentada) FILTER (
                WHERE instrumento_registro_id_siasus IN ('C', 'I')
            ),
            0
        ) AS procedimentos_registrados_bpa
    FROM dados_publicos.siasus_procedimentos_ambulatoriais
    WHERE estabelecimento_tipo_id_sigtap = '70'  -- CAPS
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        realizacao_periodo_data_inicio,
        periodo_id,
        estabelecimento_id_cnes,
        procedimento_id_sigtap
)
SELECT 
    unidade_geografica_id,
    unidade_geografica_id_sus,
    competencia,
    periodo_id,
    coalesce(
        estabelecimento.nome_curto,
        estabelecimento.nome
    ) AS estabelecimento,
    procedimento.procedimento_nome AS procedimento,
    procedimentos_registrados_raas,
    procedimentos_registrados_bpa,
    (
        procedimentos_registrados_raas + procedimentos_registrados_bpa
    ) AS procedimentos_registrados_total
FROM procedimentos_por_tipo
LEFT JOIN listas_de_codigos.estabelecimentos estabelecimento
ON procedimentos_por_tipo.estabelecimento_id_cnes = estabelecimento.id_scnes
-- TODO: trocar tabela de procedimentos por uma em `listas_de_codigos`
LEFT JOIN saude_mental.sigtap_procedimentos procedimento
ON procedimentos_por_tipo.procedimento_id_sigtap = procedimento.procedimento_id
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    procedimentos_realizados_por_tipo_UN
ON saude_mental.procedimentos_realizados_por_tipo (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    competencia,
    periodo_id,
    estabelecimento,
    procedimento
);
