/******************************************************************************
 *                                                                            *
 *                                                                            *
 *                         PERFIL DOS USUÁRIOS ATIVOS EM CAPS                 *
 *                                                                            *
 *                                                                            *
 ******************************************************************************/


CREATE INDEX IF NOT EXISTS
    siasus_raas_psicossocial_disseminacao_ug_x_cnes_x_cns_idx
ON dados_publicos.siasus_raas_psicossocial_disseminacao (
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_id_cnes,
        usuario_cns_criptografado
);


DROP MATERIALIZED VIEW IF EXISTS
    saude_mental._usuarios_ativos
CASCADE;
CREATE MATERIALIZED VIEW saude_mental._usuarios_ativos AS
WITH
procedimentos_caps_raas AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        realizacao_periodo_data_inicio AS periodo_data_inicio,
        estabelecimento_id_cnes,
        usuario_cns_criptografado,
        usuario_data_nascimento,
        saude_mental.classificar_faixa_etaria(
            usuario_data_nascimento,
            realizacao_periodo_data_inicio
        ) AS usuario_faixa_etaria,
        usuario_sexo_id_sigtap,
        condicao_principal_id_cid10,
        usuario_raca_cor_id_siasus,
        usuario_situacao_rua,
        usuario_abuso_substancias
    FROM dados_publicos.siasus_raas_psicossocial_disseminacao
    WHERE quantidade_apresentada > 0
),
procedimentos_caps_bpa_i AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        realizacao_periodo_data_inicio AS periodo_data_inicio,
        estabelecimento_id_cnes,
        usuario_cns_criptografado,
        usuario_data_nascimento,
        saude_mental.classificar_faixa_etaria(
            usuario_data_nascimento,
            realizacao_periodo_data_inicio
        ) AS usuario_faixa_etaria,
        usuario_sexo_id_sigtap,
        condicao_principal_id_cid10,
        usuario_raca_cor_id_siasus,
        NULL::bool AS usuario_situacao_rua,
        NULL::bool AS usuario_abuso_substancias
    FROM dados_publicos.siasus_bpa_i_disseminacao
    WHERE 
        estabelecimento_tipo_id_sigtap = '70'  -- CAPS
        -- ignorar acolhimentos iniciais
    AND procedimento_id_sigtap NOT IN (
        '0301080232',  -- ACOLHIMENTO INICIAL POR CAPS
        '0301040079'  -- ESCUTA INICIAL/ORIENTAÇÃO (ACOLHIM DEMANDA ESPONT)
    ) 
    AND quantidade_apresentada > 0
),
procedimentos_caps AS (
    SELECT * FROM procedimentos_caps_raas
    UNION
    SELECT * FROM procedimentos_caps_bpa_i
),
usuarios_por_mes AS (
    SELECT
        DISTINCT ON (
            estabelecimento_id_cnes,
            usuario_cns_criptografado,
            periodo_data_inicio
        )
        *
    FROM procedimentos_caps
    ORDER BY
        estabelecimento_id_cnes,
        usuario_cns_criptografado,
        periodo_data_inicio,
        -- se houver registros de RAAS e de BPA dentro do mesmo mês, prefere os
        -- registros de RAAS, nos quais os campos `usuario_situacao_rua` e 
        -- `usuario_abuso_substancias` NÃO são nulos
        usuario_situacao_rua,
        usuario_abuso_substancias,
        condicao_principal_id_cid10
),
ativos_mes AS (
    SELECT
        estabelecimento_id_cnes,
        usuario_cns_criptografado,
        periodo.data_inicio AS periodo_data_inicio,
        TRUE AS ativo_mes
    FROM usuarios_por_mes usuario
    INNER JOIN listas_de_codigos.periodos periodo
    ON
        periodo.tipo = 'Mensal'
    AND periodo.data_inicio = usuario.periodo_data_inicio
),
ativos_3meses AS (
    SELECT
        DISTINCT ON (
            estabelecimento_id_cnes,
            usuario_cns_criptografado,
            periodo.data_inicio
        )
        estabelecimento_id_cnes,
        usuario_cns_criptografado,
        periodo.data_inicio AS periodo_data_inicio,
        TRUE AS ativo_3meses
    FROM usuarios_por_mes usuario
    INNER JOIN listas_de_codigos.periodos periodo
    ON 
        periodo.tipo = 'Mensal'
    AND periodo.data_inicio >= usuario.periodo_data_inicio
    AND periodo.data_inicio < usuario.periodo_data_inicio + '3 mon'::interval
    ORDER BY 
        estabelecimento_id_cnes,
        usuario_cns_criptografado,
        periodo.data_inicio,
        usuario.periodo_data_inicio DESC
),
-- TODO: as subqueries `ativos_mes` e `ativos_3meses` podem ser derivadas desta,
-- melhorando a performance.
ativos_4meses AS (
    SELECT
        DISTINCT ON (
            estabelecimento_id_cnes,
            usuario_cns_criptografado,
            periodo.data_inicio
        )
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo.id AS periodo_id,
        periodo.data_inicio AS periodo_data_inicio,
        estabelecimento_id_cnes,
        usuario_cns_criptografado,
        usuario_data_nascimento,
        usuario_faixa_etaria,
        usuario_sexo_id_sigtap,
        condicao_principal_id_cid10,
        usuario_raca_cor_id_siasus,
        usuario_situacao_rua,
        usuario_abuso_substancias,
        TRUE AS ativo_4meses
    FROM usuarios_por_mes usuario
    INNER JOIN listas_de_codigos.periodos periodo
    ON
        periodo.tipo = 'Mensal'
    AND periodo.data_inicio >= usuario.periodo_data_inicio
    AND periodo.data_inicio < usuario.periodo_data_inicio + '4 mon'::INTERVAL
    ORDER BY 
        estabelecimento_id_cnes,
        usuario_cns_criptografado,
        periodo.data_inicio,
        usuario.periodo_data_inicio DESC
)
SELECT
    ativos_4meses.*,
    coalesce(ativos_3meses.ativo_3meses, FALSE) AS ativo_3meses,
    coalesce(ativos_mes.ativo_mes, FALSE) AS ativo_mes,
    EXTRACT(YEAR FROM age(
        ativos_4meses.periodo_data_inicio,
        ativos_4meses.usuario_data_nascimento
    )) AS usuario_idade
FROM ativos_4meses
LEFT JOIN ativos_3meses
USING (
    estabelecimento_id_cnes,
    usuario_cns_criptografado,
    periodo_data_inicio
)
LEFT JOIN ativos_mes
USING (
    estabelecimento_id_cnes,
    usuario_cns_criptografado,
    periodo_data_inicio
)
ORDER BY 
    ativos_4meses.usuario_cns_criptografado,
    ativos_4meses.estabelecimento_id_cnes,
    ativos_4meses.periodo_data_inicio DESC
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    _usuarios_ativos_un
ON saude_mental._usuarios_ativos (
    estabelecimento_id_cnes,
    usuario_cns_criptografado,
    periodo_id,
    periodo_data_inicio
);

 
DROP MATERIALIZED VIEW IF EXISTS 
    saude_mental.usuarios_ativos_perfil
;
CREATE MATERIALIZED VIEW saude_mental.usuarios_ativos_perfil AS
WITH
usuario_um_registro_por_municipio_por_mes AS (
    SELECT 
    DISTINCT ON (
        unidade_geografica_id,
        periodo_data_inicio,
        usuario_cns_criptografado
    )
    *
    FROM saude_mental._usuarios_ativos
    ORDER BY 
        unidade_geografica_id,
        periodo_data_inicio DESC,
        usuario_cns_criptografado,
        ativo_mes DESC,
        ativo_3meses DESC,
        ativo_4meses DESC
),
por_estabelecimentos AS (
    SELECT 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio AS competencia,
        estabelecimento_id_cnes,
        usuario_faixa_etaria,
        usuario_sexo_id_sigtap,
        cid.cid_grupo_descricao_curta,
        usuario_raca_cor_id_siasus,
        usuario_situacao_rua,
        usuario_abuso_substancias,
        count (DISTINCT usuario_cns_criptografado) FILTER (
            WHERE ativo_mes
        ) AS ativos_mes,
        count (DISTINCT usuario_cns_criptografado) FILTER (
            WHERE ativo_3meses
        ) AS ativos_3meses,
        count (DISTINCT usuario_cns_criptografado) FILTER (
            WHERE ativo_4meses AND NOT ativo_3meses
        ) AS tornandose_inativos
    FROM usuario_um_registro_por_municipio_por_mes usuarios_ativos
    -- TODO: trocar por tabela de cids do schema `listas_de_codigos`
    LEFT JOIN saude_mental.cids cid
    ON usuarios_ativos.condicao_principal_id_cid10 = cid.cid_id
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        estabelecimento_id_cnes,
        usuario_faixa_etaria,
        usuario_sexo_id_sigtap,
        cid.cid_grupo_descricao_curta,
        usuario_raca_cor_id_siasus,
        usuario_situacao_rua,
        usuario_abuso_substancias
),
todos_estabelecimentos_por_municipio AS (
    SELECT 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        competencia,
        '0000000' AS estabelecimento_id_cnes,
        usuario_faixa_etaria,
        usuario_sexo_id_sigtap,
        cid_grupo_descricao_curta,
        usuario_raca_cor_id_siasus,
        usuario_situacao_rua,
        usuario_abuso_substancias,
        sum(ativos_mes) AS ativos_mes,
        sum(ativos_3meses) AS ativos_3meses,
        sum(tornandose_inativos) AS tornandose_inativos
    FROM por_estabelecimentos
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        competencia,
        usuario_faixa_etaria,
        usuario_sexo_id_sigtap,
        cid_grupo_descricao_curta,
        usuario_raca_cor_id_siasus,
        usuario_situacao_rua,
        usuario_abuso_substancias
),
todos_estabelecimentos_incluindo_total AS (
    SELECT *
    FROM por_estabelecimentos
    UNION
    SELECT *
    FROM todos_estabelecimentos_por_municipio
)
SELECT
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    competencia,
    coalesce(
        estabelecimento.nome_curto,
        estabelecimento.nome,
        'Todos'
    ) AS estabelecimento,
    usuario_faixa_etaria,
    sexo.nome AS usuario_sexo,
    cid_grupo_descricao_curta,
    raca_cor.nome AS usuario_raca_cor,
    saude_mental.classificar_binarios(
        usuario_situacao_rua
    ) AS usuario_situacao_rua,
    saude_mental.classificar_binarios(
        usuario_abuso_substancias
    ) AS usuario_abuso_substancias,
    ativos_mes,
    ativos_3meses,
    tornandose_inativos
FROM todos_estabelecimentos_incluindo_total usuario_perfil
LEFT JOIN listas_de_codigos.estabelecimentos estabelecimento
ON usuario_perfil.estabelecimento_id_cnes = estabelecimento.id_scnes
LEFT JOIN listas_de_codigos.sexos sexo
ON usuario_perfil.usuario_sexo_id_sigtap = sexo.id_sigtap
LEFT JOIN listas_de_codigos.racas_cores raca_cor
ON usuario_perfil.usuario_raca_cor_id_siasus = raca_cor.id_siasus
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    usuarios_ativos_perfil_un
ON saude_mental.usuarios_ativos_perfil (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    competencia,
    estabelecimento,
    usuario_faixa_etaria,
    usuario_sexo,
    cid_grupo_descricao_curta,
    usuario_raca_cor,
    usuario_situacao_rua,
    usuario_abuso_substancias
);


DROP MATERIALIZED VIEW IF EXISTS 
	saude_mental._usuarios_ativos_por_estabelecimento
CASCADE;
CREATE MATERIALIZED VIEW saude_mental._usuarios_ativos_por_estabelecimento AS
WITH por_estabelecimentos AS (
	SELECT 
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		periodo_data_inicio,
		estabelecimento_id_cnes,
		count (DISTINCT usuario_cns_criptografado) FILTER (
            WHERE ativo_mes
        ) AS ativos_mes,
		count (DISTINCT usuario_cns_criptografado) FILTER (
            WHERE ativo_3meses
        ) AS ativos_3meses,
		count (DISTINCT usuario_cns_criptografado) FILTER (
            WHERE ativo_4meses AND NOT ativo_3meses
        ) AS tornandose_inativos
	FROM saude_mental._usuarios_ativos
	GROUP BY 
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		periodo_data_inicio,
		estabelecimento_id_cnes
),
todos_estabelecimentos_por_municipio AS (
    SELECT 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio,
        '0000000' AS estabelecimento_id_cnes,
        count (DISTINCT usuario_cns_criptografado) FILTER (
            WHERE ativo_mes
        ) AS ativos_mes,
        count (DISTINCT usuario_cns_criptografado) FILTER (
            WHERE ativo_3meses
        ) AS ativos_3meses,
        count (DISTINCT usuario_cns_criptografado) FILTER (
            WHERE ativo_4meses AND NOT ativo_3meses
        ) AS tornandose_inativos
    FROM saude_mental._usuarios_ativos
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        periodo_data_inicio
)
SELECT *
FROM por_estabelecimentos
UNION
SELECT *
FROM todos_estabelecimentos_por_municipio
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
	_usuarios_ativos_por_estabelecimento_un
ON saude_mental._usuarios_ativos_por_estabelecimento (
	unidade_geografica_id,
	unidade_geografica_id_sus,
	periodo_id,
	periodo_data_inicio,
	estabelecimento_id_cnes
);



DROP MATERIALIZED VIEW IF EXISTS 
	saude_mental._usuarios_ativos_por_estabelecimento_resumo
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental._usuarios_ativos_por_estabelecimento_resumo
AS
WITH usuarios_ativos_desconsiderar_estabelecimento AS (
	SELECT
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		periodo_data_inicio,
		'0000000' AS estabelecimento_id_cnes,
		usuario_sexo_id_sigtap,
		usuario_idade,
		usuario_cns_criptografado,
		ativo_3meses
	FROM saude_mental._usuarios_ativos
),
usuarios_ativos_com_soma_estabelecimentos AS (
	SELECT
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		periodo_data_inicio,
		estabelecimento_id_cnes,
		usuario_sexo_id_sigtap,
		usuario_idade,
		usuario_cns_criptografado,
		ativo_3meses
	FROM saude_mental._usuarios_ativos
	union
	SELECT * FROM usuarios_ativos_desconsiderar_estabelecimento
),
sexo_predominante AS (
	SELECT DISTINCT ON (
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		periodo_data_inicio,
		estabelecimento_id_cnes
	)
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		periodo_data_inicio,
		estabelecimento_id_cnes,
		usuario_sexo_id_sigtap AS sexo_predominante_id_sigtap,
		usuarios_sexo_quantidade AS sexo_predominante_quantidade
	FROM (
		SELECT
			unidade_geografica_id,
			unidade_geografica_id_sus,
			periodo_id,
			periodo_data_inicio,
			estabelecimento_id_cnes,
			usuario_sexo_id_sigtap,
			count(
                DISTINCT usuario_cns_criptografado
            ) AS usuarios_sexo_quantidade
		FROM usuarios_ativos_com_soma_estabelecimentos
		WHERE ativo_3meses
		GROUP BY 
			unidade_geografica_id,
			unidade_geografica_id_sus,
			periodo_id,
			periodo_data_inicio,
			estabelecimento_id_cnes,
			usuario_sexo_id_sigtap
		) AS t
	ORDER BY
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		periodo_data_inicio,
		estabelecimento_id_cnes,
		usuarios_sexo_quantidade DESC
),
idade_media AS (
	SELECT
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		periodo_data_inicio,
		estabelecimento_id_cnes,
		round(avg(usuario_idade)) AS usuarios_idade_media
	FROM usuarios_ativos_com_soma_estabelecimentos
	WHERE ativo_3meses
	GROUP BY 
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		periodo_data_inicio,
		estabelecimento_id_cnes
)
SELECT 
	usuarios_ativos.*,
	sexo_predominante.sexo_predominante_id_sigtap,
	sexo_predominante.sexo_predominante_quantidade,
	idade_media.usuarios_idade_media
FROM saude_mental._usuarios_ativos_por_estabelecimento usuarios_ativos
FULL JOIN sexo_predominante
ON 
	usuarios_ativos.unidade_geografica_id
	= sexo_predominante.unidade_geografica_id
AND usuarios_ativos.unidade_geografica_id_sus
    = sexo_predominante.unidade_geografica_id_sus
AND usuarios_ativos.periodo_id = sexo_predominante.periodo_id
AND usuarios_ativos.periodo_data_inicio
    = sexo_predominante.periodo_data_inicio
AND usuarios_ativos.estabelecimento_id_cnes
    = sexo_predominante.estabelecimento_id_cnes
FULL JOIN idade_media
ON 
	sexo_predominante.unidade_geografica_id = idade_media.unidade_geografica_id
AND sexo_predominante.unidade_geografica_id_sus
    = idade_media.unidade_geografica_id_sus
AND sexo_predominante.periodo_id = idade_media.periodo_id
AND sexo_predominante.periodo_data_inicio = idade_media.periodo_data_inicio
AND sexo_predominante.estabelecimento_id_cnes = idade_media.estabelecimento_id_cnes
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
	_usuarios_ativos_por_estabelecimento_resumo_un
ON saude_mental._usuarios_ativos_por_estabelecimento_resumo (
	unidade_geografica_id,
	unidade_geografica_id_sus,
	periodo_id,
	periodo_data_inicio,
	estabelecimento_id_cnes
);


DROP MATERIALIZED VIEW IF EXISTS 
	saude_mental.usuarios_ativos_por_estabelecimento_resumo
CASCADE;
CREATE MATERIALIZED VIEW 
	saude_mental.usuarios_ativos_por_estabelecimento_resumo 
AS
WITH
competencia_atual AS (
    SELECT 
        _usuarios_ativos_por_estabelecimento_resumo.unidade_geografica_id,
        _usuarios_ativos_por_estabelecimento_resumo.unidade_geografica_id_sus,
        sucessao.periodo_id,
        sucessao.periodo_data_inicio AS competencia,
        _usuarios_ativos_por_estabelecimento_resumo.estabelecimento_id_cnes,
        _usuarios_ativos_por_estabelecimento_resumo.ativos_mes,
        _usuarios_ativos_por_estabelecimento_resumo.ativos_3meses,
        _usuarios_ativos_por_estabelecimento_resumo.tornandose_inativos,
        _usuarios_ativos_por_estabelecimento_resumo.sexo_predominante_id_sigtap,
        _usuarios_ativos_por_estabelecimento_resumo.sexo_predominante_quantidade,
        _usuarios_ativos_por_estabelecimento_resumo.usuarios_idade_media
    FROM saude_mental._usuarios_ativos_por_estabelecimento_resumo
    LEFT JOIN listas_de_codigos.periodos_sucessao sucessao
    ON
        _usuarios_ativos_por_estabelecimento_resumo.periodo_id
        = sucessao.periodo_id
    AND sucessao.periodo_tipo::text = 'Mensal'::text
),
competencia_anterior AS (
    SELECT
        _usuarios_ativos_por_estabelecimento_resumo.unidade_geografica_id,
        _usuarios_ativos_por_estabelecimento_resumo.unidade_geografica_id_sus,
        sucessao.periodo_id AS proximo_periodo_id,
        sucessao.periodo_data_inicio AS proxima_competencia,
        _usuarios_ativos_por_estabelecimento_resumo.estabelecimento_id_cnes,
        _usuarios_ativos_por_estabelecimento_resumo.ativos_mes,
        _usuarios_ativos_por_estabelecimento_resumo.ativos_3meses,
        _usuarios_ativos_por_estabelecimento_resumo.tornandose_inativos
    FROM saude_mental._usuarios_ativos_por_estabelecimento_resumo
    LEFT JOIN listas_de_codigos.periodos_sucessao sucessao
    ON
        _usuarios_ativos_por_estabelecimento_resumo.periodo_id
        = sucessao.ultimo_periodo_id
    AND sucessao.periodo_tipo::text = 'Mensal'::text
),
comparacao_competencias AS (
    SELECT
    	coalesce(
    		competencia_atual.unidade_geografica_id,
    		competencia_anterior.unidade_geografica_id
    	) AS unidade_geografica_id,
    	coalesce(
    		competencia_atual.unidade_geografica_id_sus,
    		competencia_anterior.unidade_geografica_id_sus
    	) AS unidade_geografica_id_sus,
    	coalesce(
    		competencia_atual.periodo_id,
    		competencia_anterior.proximo_periodo_id
    	) AS periodo_id,
    	coalesce(
    		competencia_atual.competencia,
    		competencia_anterior.proxima_competencia
    	) AS competencia,
    	coalesce(
            competencia_atual.estabelecimento_id_cnes,
            competencia_anterior.estabelecimento_id_cnes
        ) AS estabelecimento_id_cnes,
    	competencia_atual.sexo_predominante_id_sigtap,
        competencia_atual.sexo_predominante_quantidade,
    	competencia_atual.usuarios_idade_media,
    	coalesce(competencia_atual.ativos_mes, 0) AS ativos_mes,
    	coalesce(competencia_atual.ativos_3meses, 0) AS ativos_3meses,
    	coalesce(
    	   competencia_atual.tornandose_inativos,
    	   0
    	) AS tornandose_inativos,
    	coalesce(competencia_anterior.ativos_mes, 0) AS ativos_mes_anterior,
    	coalesce(
    	   competencia_anterior.ativos_3meses,
    	   0
    	) AS ativos_3meses_anterior,
    	coalesce(
    	   competencia_anterior.tornandose_inativos,
    	   0
    	) AS tornandose_inativos_anterior
    FROM competencia_atual 
    FULL JOIN competencia_anterior 
    ON 
    	competencia_atual.periodo_id = competencia_anterior.proximo_periodo_id
	AND competencia_atual.unidade_geografica_id
	    = competencia_anterior.unidade_geografica_id
	AND competencia_atual.estabelecimento_id_cnes
	    = competencia_anterior.estabelecimento_id_cnes
)
SELECT
    comparacao_competencias.unidade_geografica_id,
    comparacao_competencias.unidade_geografica_id_sus,
    comparacao_competencias.periodo_id,
    comparacao_competencias.competencia,
    coalesce(
        estabelecimento.nome_curto,
        estabelecimento.nome,
        'Todos'
    ) AS estabelecimento,
    sexo.nome AS sexo_predominante,
    comparacao_competencias.sexo_predominante_quantidade,
    comparacao_competencias.usuarios_idade_media,
    comparacao_competencias.ativos_mes,
    comparacao_competencias.ativos_3meses,
    comparacao_competencias.tornandose_inativos,
    comparacao_competencias.ativos_mes_anterior,
    comparacao_competencias.ativos_3meses_anterior,
    comparacao_competencias.tornandose_inativos_anterior,
    (
        comparacao_competencias.ativos_mes
        - comparacao_competencias.ativos_mes_anterior
    ) AS dif_ativos_mes_anterior,
    (
        comparacao_competencias.ativos_3meses
        - comparacao_competencias.ativos_3meses_anterior
    ) AS dif_ativos_3meses_anterior,
    (
        comparacao_competencias.tornandose_inativos
        - comparacao_competencias.tornandose_inativos_anterior
    ) AS dif_tornandose_inativos_anterior,
    saude_mental.classificar_linha_perfil(
        coalesce(estabelecimento.nome, 'Todos')
    ) AS estabelecimento_linha_perfil
FROM comparacao_competencias
LEFT JOIN listas_de_codigos.sexos sexo
	ON sexo.id_sigtap = sexo_predominante_id_sigtap
LEFT JOIN listas_de_codigos.estabelecimentos estabelecimento
	ON estabelecimento.id_scnes = estabelecimento_id_cnes
LEFT JOIN
    saude_mental._raas_ultima_competencia_disponivel
    ultima_competencia_disponivel
ON
    comparacao_competencias.unidade_geografica_id
    = ultima_competencia_disponivel.unidade_geografica_id
WHERE competencia <= ultima_competencia_disponivel.periodo_data_inicio
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
	usuarios_ativos_por_estabelecimento_resumo_un
ON saude_mental.usuarios_ativos_por_estabelecimento_resumo (
	unidade_geografica_id,
	unidade_geografica_id_sus,
	periodo_id,
	competencia,
	estabelecimento
);



DROP MATERIALIZED VIEW IF EXISTS 
	saude_mental.usuarios_ativos_por_estabelecimento_resumo_ultimo_mes
CASCADE;
CREATE MATERIALIZED VIEW 
	saude_mental.usuarios_ativos_por_estabelecimento_resumo_ultimo_mes
AS
WITH ultima_competencia AS (
    SELECT
        DISTINCT ON (unidade_geografica_id)
        *
    FROM (
        SELECT * FROM saude_mental._raas_ultima_competencia_disponivel
        UNION
        SELECT * FROM saude_mental._bpa_i_caps_ultima_competencia_disponivel
    ) q
    ORDER BY
        unidade_geografica_id,
        -- se o último BPA-i e RAAS forem em competências diferentes, usa a mais
        -- antiga
        periodo_data_inicio ASC
)
SELECT
	unidade_geografica_id,
	unidade_geografica_id_sus,
	periodo_id,
	competencia,
	listas_de_codigos.nome_mes(competencia) AS nome_mes,
	estabelecimento,
	sexo_predominante,
	sexo_predominante_quantidade,
	usuarios_idade_media,
	ativos_mes,
	ativos_3meses,
	tornandose_inativos,
	ativos_mes_anterior,
	ativos_3meses_anterior,
	tornandose_inativos_anterior,
    dif_ativos_mes_anterior,
    dif_ativos_3meses_anterior,
    dif_tornandose_inativos_anterior,
    estabelecimento_linha_perfil
FROM saude_mental.usuarios_ativos_por_estabelecimento_resumo
INNER JOIN ultima_competencia
USING (unidade_geografica_id, periodo_id)
WITH NO DATA;
CREATE UNIQUE INDEX
	usuarios_ativos_por_estabelecimento_resumo_ultimo_mes_un
ON saude_mental.usuarios_ativos_por_estabelecimento_resumo_ultimo_mes (
	unidade_geografica_id,
	unidade_geografica_id_sus,
	estabelecimento
);
