/******************************************************************************
 *                                                                            *
 *                                                                            *
 *    CONTAGEM DO NÚMERO DE PROCEDIMENTOS REALIZADOS POR USUÁRIO EM CAPS      *
 *                                                                            *
 *                                                                            *
 ******************************************************************************/


DROP MATERIALIZED VIEW IF EXISTS
    saude_mental.caps_procedimentos_total_e_acolhimentos
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.caps_procedimentos_total_e_acolhimentos
AS
SELECT 
    procedimento.unidade_geografica_id,
    procedimento.unidade_geografica_id_sus,
    procedimento.realizacao_periodo_data_inicio AS competencia,
    procedimento.periodo_id,
    estabelecimento.id_cnes AS estabelecimento_id_cnes,
    coalesce(
        estabelecimento.nome_curto,
        estabelecimento.nome
    ) AS estabelecimento,
    coalesce(
       sum(procedimento.quantidade_apresentada),
       0
    ) AS quantidade_registrada,
    coalesce(
       sum(procedimento.quantidade_apresentada) FILTER (
           WHERE procedimento_id_sigtap IN (
            '0301080232'  -- ACOLHIMENTO INICIAL POR CAPS
            '0301040079'  -- ESCUTA INICIAL/ORIENTAÇÃO (ACOLHIM DEMANDA ESPONT)
        ) 
       ),
       0
    ) AS quantidade_registrada_acolhimentos_iniciais_caps
FROM dados_publicos.siasus_procedimentos_ambulatoriais procedimento
LEFT JOIN listas_de_codigos.estabelecimentos estabelecimento
    ON procedimento.estabelecimento_id_cnes = estabelecimento.id_scnes
WHERE procedimento.estabelecimento_tipo_id_sigtap = '70' -- CAPS
GROUP BY 
    procedimento.unidade_geografica_id,
    procedimento.unidade_geografica_id_sus,
    procedimento.realizacao_periodo_data_inicio,
    procedimento.periodo_id,
    estabelecimento.id_cnes,
    estabelecimento.nome
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW
    saude_mental.caps_procedimentos_total_e_acolhimentos
IS 
'Quantidade de procedimentos ambulatoriais registrados em RAAS, 
BPA-i e BPA-c nos Centros de Atenção Psicossocial. Diferencia o 
total de procedimentos e o número de procedimentos que são 
acolhimentos iniciais.'
;
CREATE UNIQUE INDEX IF NOT EXISTS
    caps_procedimentos_total_e_acolhimentos_un
ON saude_mental.caps_procedimentos_total_e_acolhimentos (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    competencia,
    periodo_id,
    estabelecimento_id_cnes
);


/* ---------------------------------------------------------------- */


DROP MATERIALIZED VIEW IF EXISTS
	saude_mental._usuarios_procedimentos_por_mes
CASCADE;
CREATE MATERIALIZED VIEW
	saude_mental._usuarios_procedimentos_por_mes
AS
WITH
ativos_raas AS (
    SELECT 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        realizacao_periodo_data_inicio AS competencia,
        estabelecimento_id_cnes,
        usuario_cns_criptografado,
        sum(quantidade_apresentada) AS procedimentos_registrados_raas
    FROM dados_publicos.siasus_raas_psicossocial_disseminacao
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        realizacao_periodo_data_inicio,
        estabelecimento_id_cnes,
        usuario_cns_criptografado
),
ativos_bpa_i AS (
    SELECT 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        realizacao_periodo_data_inicio AS competencia,
        estabelecimento_id_cnes,
        usuario_cns_criptografado,
        sum(quantidade_apresentada) AS procedimentos_registrados_bpa_i,
        sum(quantidade_apresentada) FILTER (
            WHERE procedimento_id_sigtap IN (
                '0301080232'  -- ACOLHIMENTO INICIAL POR CAPS
                '0301040079'  -- ESCUTA INICIAL/ORIENTAÇÃO (AC DEMANDA ESPONT)
            )
        ) AS acolhimentos_iniciais_em_caps
    FROM dados_publicos.siasus_bpa_i_disseminacao
    WHERE estabelecimento_tipo_id_sigtap = '70'  -- CAPS
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        realizacao_periodo_data_inicio,
        estabelecimento_id_cnes,
        usuario_cns_criptografado
),
procedimentos_raas_bpa_i AS (
    SELECT
    	coalesce(
    		ativos_raas.unidade_geografica_id,
    		ativos_bpa_i.unidade_geografica_id
    	) AS unidade_geografica_id,
        coalesce(
            ativos_raas.unidade_geografica_id_sus,
            ativos_bpa_i.unidade_geografica_id_sus
        ) AS unidade_geografica_id_sus,
    	coalesce(
    		ativos_raas.periodo_id,
    		ativos_bpa_i.periodo_id
    	) AS periodo_id,
        coalesce(
            ativos_raas.competencia,
            ativos_bpa_i.competencia
        ) AS competencia,
    	coalesce(
    		ativos_raas.estabelecimento_id_cnes,
    		ativos_bpa_i.estabelecimento_id_cnes
    	) AS estabelecimento_id_cnes,
    	coalesce(
    		ativos_raas.usuario_cns_criptografado,
    		ativos_bpa_i.usuario_cns_criptografado
    	) AS usuario_cns_criptografado,
    	coalesce(
    		procedimentos_registrados_raas,
    		0
    	) AS procedimentos_registrados_raas,
    	coalesce(
    		procedimentos_registrados_bpa_i,
    		0
    	) AS procedimentos_registrados_bpa_i,
    	coalesce(
    		acolhimentos_iniciais_em_caps,
    		0
    	) AS acolhimentos_iniciais_em_caps
    FROM ativos_raas
    FULL JOIN  ativos_bpa_i
    ON
    	ativos_raas.unidade_geografica_id = ativos_bpa_i.unidade_geografica_id
    AND ativos_raas.periodo_id = ativos_bpa_i.periodo_id
    AND ativos_raas.estabelecimento_id_cnes
        = ativos_bpa_i.estabelecimento_id_cnes
    AND ativos_raas.usuario_cns_criptografado
        = ativos_bpa_i.usuario_cns_criptografado
),
usuario_primeiro_procedimento AS (
    SELECT 
        DISTINCT ON (
            unidade_geografica_id,
            unidade_geografica_id_sus,
            estabelecimento_id_cnes,
            usuario_cns_criptografado
        )
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_id_cnes,
        usuario_cns_criptografado,
        competencia AS competencia_primeiro_procedimento
    FROM procedimentos_raas_bpa_i
    ORDER BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_id_cnes,
        usuario_cns_criptografado,
        competencia ASC
)
SELECT 
    procedimentos_raas_bpa_i.*,
    (
       procedimentos_registrados_raas 
       + procedimentos_registrados_bpa_i
       - acolhimentos_iniciais_em_caps
    ) AS procedimentos_exceto_acolhimento,
    saude_mental.classificar_tempo_no_servico(
        competencia_primeiro_procedimento,
        competencia
    ) AS usuario_tempo_servico
FROM procedimentos_raas_bpa_i
LEFT JOIN usuario_primeiro_procedimento
USING (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    estabelecimento_id_cnes,
    usuario_cns_criptografado
)
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW 
	saude_mental._usuarios_procedimentos_por_mes
IS '
Quantidade de procedimentos ambulatoriais registrados em RAAS ou BPA-i por  
usuário por mês em cada estabelecimento vinculado ao SUS.
'
;
CREATE UNIQUE INDEX IF NOT EXISTS
	_usuarios_procedimentos_por_mes_un
ON saude_mental._usuarios_procedimentos_por_mes (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    estabelecimento_id_cnes,
    usuario_cns_criptografado,
    competencia,
    periodo_id
);


/* ---------------------------------------------------------------- */


DROP MATERIALIZED VIEW IF EXISTS
	saude_mental._procedimentos_por_usuario_por_estabelecimento
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental._procedimentos_por_usuario_por_estabelecimento
AS
WITH
por_estabelecimento AS (
    SELECT
    	unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
    	periodo_id,
    	estabelecimento_id_cnes,
    	sum(
    	   procedimentos_exceto_acolhimento
    	) AS procedimentos_exceto_acolhimento,
    	count(DISTINCT usuario_cns_criptografado) AS ativos_mes,
    	round(
    		(
    			sum(procedimentos_exceto_acolhimento)::numeric
    			/ nullif(count(DISTINCT usuario_cns_criptografado), 0)
    		),
    		1
    	) AS procedimentos_por_usuario
    FROM saude_mental._usuarios_procedimentos_por_mes
    WHERE (
        procedimentos_registrados_raas 
        + procedimentos_registrados_bpa_i 
        - acolhimentos_iniciais_em_caps
    ) > 0
    GROUP BY
    	unidade_geografica_id,
    	unidade_geografica_id_sus,
    	estabelecimento_id_cnes,
    	competencia,
        periodo_id
),
todos_estabelecimentos AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        '0000000' AS estabelecimento_id_cnes,
        sum(
            procedimentos_exceto_acolhimento
        ) AS procedimentos_exceto_acolhimento,
        sum(ativos_mes) AS ativos_mes,
        round(
            (
                sum(procedimentos_exceto_acolhimento)::numeric
                / nullif(sum(ativos_mes), 0)
            ),
            1
        ) AS procedimentos_por_usuario
    FROM por_estabelecimento
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id
),
por_estabelecimento_com_total AS (
    SELECT *
    FROM por_estabelecimento
    UNION
    SELECT *
    FROM todos_estabelecimentos
),
caps_maior_taxa AS (
    SELECT
        DISTINCT ON (
            unidade_geografica_id,
            periodo_id
        )
        unidade_geografica_id,
        periodo_id,
        coalesce(
            estabelecimento.nome_curto,
            estabelecimento.nome
        ) AS estabelecimento_maior_taxa,
        procedimentos_por_usuario AS maior_taxa
        FROM por_estabelecimento
        LEFT JOIN listas_de_codigos.estabelecimentos estabelecimento
        ON por_estabelecimento.estabelecimento_id_cnes = estabelecimento.id_scnes
        ORDER BY
            unidade_geografica_id,
            periodo_id,
            procedimentos_por_usuario DESC
)
SELECT
    *
FROM por_estabelecimento_com_total
LEFT JOIN caps_maior_taxa
USING (
    unidade_geografica_id,
    periodo_id
)
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
	_procedimentos_por_usuario_por_estabelecimento_un
ON saude_mental._procedimentos_por_usuario_por_estabelecimento (
	unidade_geografica_id,
	unidade_geografica_id_sus,
	estabelecimento_id_cnes,
	periodo_id
);


/* ---------------------------------------------------------------- */


DROP MATERIALIZED VIEW IF EXISTS
	saude_mental.procedimentos_por_usuario_por_caps
CASCADE;
CREATE MATERIALIZED VIEW 
	saude_mental.procedimentos_por_usuario_por_caps
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
    saude_mental.classificar_linha_perfil(
       coalesce(estabelecimento.nome, 'Todos')
    ) AS estabelecimento_linha_perfil,
	coalesce(
        estabelecimento.nome_curto,
        estabelecimento.nome,
        'Todos'
    ) AS estabelecimento,
	sucessao.periodo_id,
	sucessao.periodo_data_inicio AS competencia,
	listas_de_codigos.nome_mes(sucessao.periodo_data_inicio) AS nome_mes,
	coalesce(
		competencia_atual.procedimentos_exceto_acolhimento,	0
	) AS procedimentos_exceto_acolhimento,
	coalesce(
		competencia_atual.ativos_mes,
		0
	) AS ativos_mes,
	coalesce(
		competencia_atual.procedimentos_por_usuario, 0
	) AS procedimentos_por_usuario,
	coalesce(
		competencia_anterior.procedimentos_exceto_acolhimento, 0
	) AS procedimentos_exceto_acolhimento_anterior,
	coalesce(
		competencia_anterior.ativos_mes, 0
	) AS ativos_mes_anterior,
    coalesce(
        competencia_anterior.ativos_mes,
        0
    )  AS procedimentos_por_usuario_anterior,
    round(
        100 * (
            coalesce(competencia_atual.procedimentos_por_usuario, 0)
            - coalesce(competencia_anterior.procedimentos_por_usuario, 0)
        )::numeric
        / nullif(
            coalesce(competencia_anterior.procedimentos_por_usuario, 0),
            0
        ),
        1
    ) AS dif_procedimentos_por_usuario_anterior_perc,
    competencia_atual.estabelecimento_maior_taxa,
    competencia_atual.maior_taxa
FROM
    saude_mental._procedimentos_por_usuario_por_estabelecimento
    competencia_atual
LEFT JOIN listas_de_codigos.periodos_sucessao sucessao
ON 
    sucessao.periodo_tipo = 'Mensal'
AND competencia_atual.periodo_id = sucessao.periodo_id
FULL JOIN 
    saude_mental._procedimentos_por_usuario_por_estabelecimento
    competencia_anterior
ON 
	sucessao.ultimo_periodo_id = competencia_anterior.periodo_id
AND competencia_atual.unidade_geografica_id
    = competencia_anterior.unidade_geografica_id
AND competencia_atual.estabelecimento_id_cnes
    = competencia_anterior.estabelecimento_id_cnes
LEFT JOIN listas_de_codigos.estabelecimentos estabelecimento
ON 
    coalesce(
        competencia_atual.estabelecimento_id_cnes,
        competencia_anterior.estabelecimento_id_cnes
    ) = estabelecimento.id_scnes
WHERE competencia_atual.procedimentos_exceto_acolhimento IS NOT NULL
WITH NO DATA;
COMMENT
ON MATERIALIZED VIEW saude_mental.procedimentos_por_usuario_por_caps
IS 
'Quantidade de usuários únicos e de procedimentos ambulatoriais registrados 
por mês em RAAS ou BPA-i nos Centros de Atenção Psicossocial (exceto 
acolhimentos iniciais). Inclui comparação com mês anterior.'
;
CREATE UNIQUE INDEX IF NOT EXISTS
	procedimentos_por_usuario_por_caps_un
ON saude_mental.procedimentos_por_usuario_por_caps (
	unidade_geografica_id,
	unidade_geografica_id_sus,
	estabelecimento,
	competencia DESC,
	periodo_id
);



/* ---------------------------------------------------------------- */



DROP MATERIALIZED VIEW IF EXISTS
	saude_mental.procedimentos_por_usuario_por_caps_ultimo_mes
CASCADE;
CREATE MATERIALIZED VIEW
	saude_mental.procedimentos_por_usuario_por_caps_ultimo_mes
AS
SELECT
DISTINCT ON (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    estabelecimento
)
	*
FROM saude_mental.procedimentos_por_usuario_por_caps
ORDER BY 
  	unidade_geografica_id,
  	unidade_geografica_id_sus,
	estabelecimento,
	competencia DESC
WITH NO DATA;
COMMENT ON MATERIALIZED VIEW
	saude_mental.procedimentos_por_usuario_por_caps_ultimo_mes
IS 
'Quantidade de usuários únicos e de procedimentos ambulatoriais '
'registrados no último mês em RAAS ou BPA-i nos Centros de Atenção '
'Psicossocial (exceto acolhimentos iniciais). Inclui comparação com '
'mês anterior.'
;
CREATE UNIQUE INDEX
	procedimentos_por_usuario_por_caps_ultimo_mes_un
ON saude_mental.procedimentos_por_usuario_por_caps_ultimo_mes (
	unidade_geografica_id,
	unidade_geografica_id_sus,
	estabelecimento
);


/* ---------------------------------------------------------------- */


DROP MATERIALIZED VIEW IF EXISTS
    saude_mental.procedimentos_por_usuario_por_tempo_servico
CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS 
    saude_mental.procedimentos_por_usuario_por_tempo_servico
AS
WITH
por_tempo_servico_por_estabelecimento AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_id_cnes,
        competencia,
        periodo_id,
        usuario_tempo_servico,
        round(
            (
                sum(procedimentos_exceto_acolhimento)::numeric
                / nullif(count(DISTINCT usuario_cns_criptografado), 0)
            ),
            1
        ) AS procedimentos_por_usuario
    FROM saude_mental._usuarios_procedimentos_por_mes
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_id_cnes,
        competencia,
        periodo_id,
        usuario_tempo_servico
),
por_tempo_servico_todos_estabelecimentos AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        '0000000' AS estabelecimento_id_cnes,
        competencia,
        periodo_id,
        usuario_tempo_servico,
        round(
            (
                sum(procedimentos_exceto_acolhimento)::numeric
                / nullif(count(DISTINCT usuario_cns_criptografado), 0)
            ),
            1
        ) AS procedimentos_por_usuario
    FROM saude_mental._usuarios_procedimentos_por_mes
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        usuario_tempo_servico
),
por_tempo_servico AS (
    SELECT * FROM por_tempo_servico_por_estabelecimento
    UNION
    SELECT * FROM por_tempo_servico_todos_estabelecimentos
)
SELECT
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    competencia,
    listas_de_codigos.nome_mes(competencia) AS nome_mes,
    coalesce(
        estabelecimento.nome_curto,
        estabelecimento.nome,
        'Todos'
    ) AS estabelecimento,
    usuario_tempo_servico,
    procedimentos_por_usuario
FROM por_tempo_servico
LEFT JOIN listas_de_codigos.estabelecimentos estabelecimento
ON por_tempo_servico.estabelecimento_id_cnes = estabelecimento.id_scnes
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    procedimentos_por_usuario_por_tempo_servico_un
ON saude_mental.procedimentos_por_usuario_por_tempo_servico (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    estabelecimento,
    competencia DESC,
    periodo_id,
    usuario_tempo_servico
);


DROP MATERIALIZED VIEW IF EXISTS
    saude_mental.procedimentos_por_usuario_por_tempo_servico_resumo_ultimo_mes
CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS 
    saude_mental.procedimentos_por_usuario_por_tempo_servico_resumo_ultimo_mes
AS
SELECT
    DISTINCT ON (
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento
    )
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    competencia,
    nome_mes,
    estabelecimento,
    usuario_tempo_servico AS tempo_servico_maior_taxa,
    procedimentos_por_usuario AS maior_taxa
FROM saude_mental.procedimentos_por_usuario_por_tempo_servico
ORDER BY
    unidade_geografica_id,
    unidade_geografica_id_sus,
    estabelecimento,
    competencia DESC,
    procedimentos_por_usuario DESC
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS 
    procedimentos_por_usuario_por_tempo_servico_resumo_ultimo_mes_un
ON saude_mental.procedimentos_por_usuario_por_tempo_servico_resumo_ultimo_mes (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    estabelecimento
);