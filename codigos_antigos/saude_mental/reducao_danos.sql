/**************************************************************************

						AÇÕES DE REDUÇÃO DE DANOS


 **************************************************************************/


/* Somar quantidade de procedimentos de ações de redução de danos *
 * por CAPS e por categoria profissional por mês                  */
DROP MATERIALIZED VIEW IF EXISTS
	saude_mental._reducao_danos_acoes_por_estabelecimento_por_mes
CASCADE;
CREATE MATERIALIZED VIEW 
	saude_mental._reducao_danos_acoes_por_estabelecimento_por_mes
AS
SELECT 
	procedimento.unidade_geografica_id,
	procedimento.unidade_geografica_id_sus,
	procedimento.periodo_id,
	procedimento.realizacao_periodo_data_inicio AS periodo_data_inicio,
	procedimento.estabelecimento_id_cnes,
	procedimento.profissional_ocupacao_id_cbo,
	sum(procedimento.quantidade_apresentada) AS quantidade_registrada
FROM dados_publicos.siasus_procedimentos_ambulatoriais procedimento
WHERE
	procedimento.procedimento_id_sigtap = '0301080313' -- AÇÕES DE REDUÇÃO DE DANOS
AND	estabelecimento_tipo_id_sigtap = '70'  -- CAPS
GROUP BY
	procedimento.unidade_geografica_id,
	procedimento.unidade_geografica_id_sus,
	procedimento.periodo_id,
	procedimento.realizacao_periodo_data_inicio,
	procedimento.estabelecimento_id_cnes,
	procedimento.profissional_ocupacao_id_cbo
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS 
	_reducao_danos_acoes_por_estabelecimento_por_mes_un
ON saude_mental._reducao_danos_acoes_por_estabelecimento_por_mes (
	unidade_geografica_id,
	unidade_geografica_id_sus,
	periodo_id,
	periodo_data_inicio,
	estabelecimento_id_cnes,
	profissional_ocupacao_id_cbo
);


/* Obter nomes dos estabelecimentos e categorias profissionais,  *
 * comparar entre competências consecutivas e totalizar por CAPS *
 * e por categoria profissional                                  */
DROP MATERIALIZED VIEW IF EXISTS 
	saude_mental.reducao_danos_acoes_por_estabelecimento_por_mes
CASCADE;
CREATE MATERIALIZED VIEW
	saude_mental.reducao_danos_acoes_por_estabelecimento_por_mes
AS
WITH
acoes_por_cbo AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        '0000000' AS estabelecimento_id_cnes,
        profissional_ocupacao_id_cbo,
        sum(quantidade_registrada) AS quantidade_registrada
    FROM saude_mental._reducao_danos_acoes_por_estabelecimento_por_mes
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        profissional_ocupacao_id_cbo
),
acoes_por_estabelecimento AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        estabelecimento_id_cnes,
        '000000' AS profissional_ocupacao_id_cbo,
        sum(quantidade_registrada) AS quantidade_registrada
    FROM saude_mental._reducao_danos_acoes_por_estabelecimento_por_mes
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        estabelecimento_id_cnes
),
acoes_geral AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        '0000000' AS estabelecimento_id_cnes,
        '000000' AS profissional_ocupacao_id_cbo,
        sum(quantidade_registrada) AS quantidade_registrada
    FROM saude_mental._reducao_danos_acoes_por_estabelecimento_por_mes
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio
),
acoes_com_totais AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        estabelecimento_id_cnes,
        profissional_ocupacao_id_cbo,
        quantidade_registrada
    FROM saude_mental._reducao_danos_acoes_por_estabelecimento_por_mes
    UNION
    SELECT * FROM acoes_por_cbo 
    UNION
    SELECT * FROM acoes_por_estabelecimento
    UNION
    SELECT * FROM acoes_geral
),
competencia_anterior AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        sucessao.periodo_id,
        sucessao.periodo_data_inicio,
        estabelecimento_id_cnes,
        profissional_ocupacao_id_cbo,
        quantidade_registrada
    FROM acoes_com_totais
    LEFT JOIN listas_de_codigos.periodos_sucessao sucessao
    ON 
        acoes_com_totais.periodo_id = sucessao.ultimo_periodo_id
    AND sucessao.periodo_tipo::text = 'Mensal'::text
),
comparacao_competencia_anterior AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        estabelecimento_id_cnes,
        profissional_ocupacao_id_cbo,
        coalesce(
           competencia_atual.quantidade_registrada,
           0
       )::bigint AS quantidade_registrada,
        coalesce(
           competencia_anterior.quantidade_registrada,
           0::bigint
       ) AS quantidade_registrada_anterior,
       (
            coalesce(competencia_atual.quantidade_registrada, 0)
            - coalesce(competencia_anterior.quantidade_registrada, 0)
        ) AS dif_quantidade_registrada_anterior
    FROM acoes_com_totais competencia_atual
    FULL JOIN competencia_anterior 
    USING (
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        estabelecimento_id_cnes,
        profissional_ocupacao_id_cbo
    )
),
final AS (
    SELECT
        comparacao_competencia_anterior.unidade_geografica_id,
        comparacao_competencia_anterior.unidade_geografica_id_sus,
        comparacao_competencia_anterior.periodo_id,
        comparacao_competencia_anterior.periodo_data_inicio AS competencia,
        listas_de_codigos.nome_mes(
            comparacao_competencia_anterior.periodo_data_inicio
        ) AS nome_mes,
        coalesce(
            estabelecimento.nome_curto,
            estabelecimento.nome,
            'Todos'
        ) AS estabelecimento,
        coalesce(ocupacao.descricao_cbo2002, 'Todas') AS ocupacao,
        quantidade_registrada,
        quantidade_registrada_anterior,
        dif_quantidade_registrada_anterior
    FROM comparacao_competencia_anterior
    LEFT JOIN listas_de_codigos.estabelecimentos estabelecimento
    ON
        comparacao_competencia_anterior.estabelecimento_id_cnes
        = estabelecimento.id_scnes
    LEFT JOIN listas_de_codigos.ocupacoes ocupacao
    ON profissional_ocupacao_id_cbo = ocupacao.id_cbo2002
    LEFT JOIN
       saude_mental._procedimentos_ultima_competencia_disponivel
       ultima_competencia
    USING (unidade_geografica_id)
    WHERE
       comparacao_competencia_anterior.periodo_data_inicio 
       <= ultima_competencia.periodo_data_inicio
)
SELECT
    *
FROM final
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS 
	reducao_danos_acoes_por_estabelecimento_por_mes_un
ON saude_mental.reducao_danos_acoes_por_estabelecimento_por_mes (
	unidade_geografica_id,
	unidade_geografica_id_sus,
	periodo_id,
	competencia,
	nome_mes,
	ocupacao,
	estabelecimento
);


/* Filtrar apenas os dados da última competência disponível */
DROP MATERIALIZED VIEW IF EXISTS 
	saude_mental.reducao_danos_acoes_por_estabelecimento_ultimo_mes
CASCADE;
CREATE MATERIALIZED VIEW
	saude_mental.reducao_danos_acoes_por_estabelecimento_ultimo_mes
AS
SELECT reducao_danos.*
FROM saude_mental.reducao_danos_acoes_por_estabelecimento_por_mes reducao_danos
INNER JOIN saude_mental."_procedimentos_ultima_competencia_disponivel" ultima_competencia
	ON
		reducao_danos.unidade_geografica_id = ultima_competencia.unidade_geografica_id
	AND reducao_danos.periodo_id = ultima_competencia.periodo_id
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS 
    reducao_danos_acoes_por_estabelecimento_ultimo_mes_un
ON saude_mental.reducao_danos_acoes_por_estabelecimento_ultimo_mes (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    ocupacao,
    estabelecimento
);


DROP MATERIALIZED VIEW IF EXISTS
	saude_mental.reducao_danos_acoes_ultimos_12m
CASCADE;
CREATE MATERIALIZED VIEW
	saude_mental.reducao_danos_acoes_ultimos_12m
AS
WITH ultimos_12m AS (
	SELECT
		reducao_danos.unidade_geografica_id,
		reducao_danos.unidade_geografica_id_sus,
		EXTRACT(
            'year' FROM min(reducao_danos.competencia)
		)::text AS a_partir_de_ano,
		listas_de_codigos.nome_mes(
            min(reducao_danos.competencia)::date
        ) AS a_partir_de_mes,
		EXTRACT(
            'year' FROM max(reducao_danos.competencia)
        )::text AS ate_ano,
		listas_de_codigos.nome_mes(
            max(reducao_danos.competencia)::date
        ) AS ate_mes,
		reducao_danos.estabelecimento,
		reducao_danos.ocupacao,
		sum(
            reducao_danos.quantidade_registrada
        ) AS quantidade_registrada_ultimos_12m
	FROM 
	   saude_mental.reducao_danos_acoes_por_estabelecimento_por_mes
	   reducao_danos
	LEFT JOIN 
	   saude_mental._procedimentos_ultima_competencia_disponivel
	   ultima_competencia_disponivel
	ON 
	   reducao_danos.unidade_geografica_id 
	   = ultima_competencia_disponivel.unidade_geografica_id
	WHERE 
	   reducao_danos.competencia 
	   > ultima_competencia_disponivel.periodo_data_inicio - '12 mon'::interval
	GROUP BY
		reducao_danos.unidade_geografica_id,
		reducao_danos.unidade_geografica_id_sus,
		reducao_danos.estabelecimento,
		reducao_danos.ocupacao
),
penultimos_12m AS (
	SELECT
		reducao_danos.unidade_geografica_id,
		reducao_danos.unidade_geografica_id_sus,
		reducao_danos.estabelecimento,
		reducao_danos.ocupacao,
		sum(
            reducao_danos.quantidade_registrada
        ) AS quantidade_registrada_penultimos_12m
	FROM 
	   saude_mental.reducao_danos_acoes_por_estabelecimento_por_mes
	   reducao_danos
	LEFT JOIN 
	   saude_mental._procedimentos_ultima_competencia_disponivel
	   ultima_competencia_disponivel
	ON 
	   reducao_danos.unidade_geografica_id 
	   = ultima_competencia_disponivel.unidade_geografica_id
	WHERE 
		reducao_danos.competencia 
		<= ( 
		  ultima_competencia_disponivel.periodo_data_inicio 
		  - '12 mon'::INTERVAL
		)
	AND	
	   reducao_danos.competencia 
	   > ultima_competencia_disponivel.periodo_data_inicio - '24 mon'::interval
	GROUP BY
		reducao_danos.unidade_geografica_id,
		reducao_danos.unidade_geografica_id_sus,
		reducao_danos.estabelecimento,
		reducao_danos.ocupacao
)
SELECT
	ultimos_12m.*,
	penultimos_12m.quantidade_registrada_penultimos_12m,
	(
        ultimos_12m.quantidade_registrada_ultimos_12m
        - penultimos_12m.quantidade_registrada_penultimos_12m
    ) AS dif_quantidade_registrada_anterior
FROM ultimos_12m
LEFT JOIN penultimos_12m
USING (
	unidade_geografica_id,
	unidade_geografica_id_sus,
	estabelecimento,
	ocupacao
)
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
	reducao_danos_acoes_ultimos_12m_un
ON saude_mental.reducao_danos_acoes_ultimos_12m (
	unidade_geografica_id,
	unidade_geografica_id_sus,
	estabelecimento,
	ocupacao
);
