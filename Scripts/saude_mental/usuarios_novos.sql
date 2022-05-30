/******************************************************************************
 *                                                                            *
 *                                                                            *
 *                       PERFIL DOS USUÁRIOS NOVOS EM CAPS                    *
 *                                                                            *
 *                                                                            *
 ******************************************************************************/



DROP MATERIALIZED VIEW saude_mental._usuarios_novos CASCADE;
CREATE MATERIALIZED VIEW saude_mental._usuarios_novos AS 
WITH usuarios_primeira_competencia AS (
	SELECT 
        DISTINCT ON (
	       estabelecimento_id_cnes,
	       usuario_cns_criptografado
	    )
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        estabelecimento_id_cnes,
        realizacao_periodo_data_inicio AS competencia,
        usuario_cns_criptografado,
        cid.cid_grupo_descricao_curta,
        usuario_sexo_id_sigtap,
        usuario_abuso_substancias,
        usuario_situacao_rua,
        usuario_raca_cor_id_siasus,
		saude_mental.classificar_faixa_etaria(
            usuario_data_nascimento,
            realizacao_periodo_data_inicio
		) AS usuario_faixa_etaria
	FROM dados_publicos.siasus_raas_psicossocial_disseminacao 
    -- TODO: trocar por lista de cids do schema `listas_de_codigos`
    LEFT JOIN saude_mental.cids cid
        ON cid.cid_id = condicao_principal_id_cid10
	WHERE quantidade_apresentada > 0
	ORDER BY 
       estabelecimento_id_cnes,
       usuario_cns_criptografado,
       realizacao_periodo_data_inicio asc
)
SELECT 
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    estabelecimento_id_cnes,
    competencia,
    cid_grupo_descricao_curta,
    usuario_sexo_id_sigtap,
    usuario_abuso_substancias,
    usuario_situacao_rua,
    usuario_raca_cor_id_siasus,
    usuario_faixa_etaria,
    count(DISTINCT usuario_cns_criptografado) AS usuarios_novos
FROM usuarios_primeira_competencia
GROUP BY 
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    estabelecimento_id_cnes,
    competencia,
    cid_grupo_descricao_curta,
    usuario_sexo_id_sigtap,
    usuario_abuso_substancias,
    usuario_situacao_rua,
    usuario_raca_cor_id_siasus,
    usuario_faixa_etaria
WITH NO DATA;
CREATE UNIQUE INDEX 
    _usuarios_novos_un
ON saude_mental._usuarios_novos (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    estabelecimento_id_cnes,
    competencia,
    cid_grupo_descricao_curta,
    usuario_sexo_id_sigtap,
    usuario_abuso_substancias,
    usuario_situacao_rua,
    usuario_raca_cor_id_siasus,
    usuario_faixa_etaria
);



DROP MATERIALIZED VIEW IF EXISTS
    saude_mental.usuarios_novos
CASCADE;
CREATE MATERIALIZED VIEW saude_mental.usuarios_novos AS
WITH
competencia_atual AS (
    SELECT 
        _usuarios_novos.unidade_geografica_id,
        _usuarios_novos.unidade_geografica_id_sus,
        sucessao.periodo_id,
        _usuarios_novos.estabelecimento_id_cnes,
        sucessao.periodo_data_inicio AS competencia,
        _usuarios_novos.cid_grupo_descricao_curta,
        _usuarios_novos.usuario_sexo_id_sigtap,
        _usuarios_novos.usuario_abuso_substancias,
        _usuarios_novos.usuario_situacao_rua,
        _usuarios_novos.usuario_raca_cor_id_siasus,
        _usuarios_novos.usuario_faixa_etaria,
        _usuarios_novos.usuarios_novos
    FROM saude_mental._usuarios_novos
    LEFT JOIN listas_de_codigos.periodos_sucessao sucessao
    ON
        _usuarios_novos.periodo_id = sucessao.periodo_id
    AND sucessao.periodo_tipo = 'Mensal'
),
competencia_anterior AS (
    SELECT 
        _usuarios_novos.unidade_geografica_id,
        _usuarios_novos.unidade_geografica_id_sus,
        sucessao.periodo_id AS proximo_periodo_id,
        _usuarios_novos.estabelecimento_id_cnes,
        sucessao.periodo_data_inicio AS proxima_competencia,
        _usuarios_novos.cid_grupo_descricao_curta,
        _usuarios_novos.usuario_sexo_id_sigtap,
        _usuarios_novos.usuario_abuso_substancias,
        _usuarios_novos.usuario_situacao_rua,
        _usuarios_novos.usuario_raca_cor_id_siasus,
        _usuarios_novos.usuario_faixa_etaria,
        _usuarios_novos.usuarios_novos
    FROM saude_mental._usuarios_novos
    LEFT JOIN listas_de_codigos.periodos_sucessao sucessao
    ON 
        _usuarios_novos.periodo_id = sucessao.ultimo_periodo_id
    AND sucessao.periodo_tipo = 'Mensal'
)
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
	listas_de_codigos.nome_mes(
        coalesce(
            competencia_atual.competencia,
            competencia_anterior.proxima_competencia
        )::date
    ) AS nome_mes,
	coalesce(
        estabelecimento.nome_curto,
        estabelecimento.nome
    ) AS estabelecimento,
	saude_mental.classificar_linha_perfil(
	  estabelecimento.nome
	) AS estabelecimento_linha_perfil,
	coalesce(
	  competencia_atual.cid_grupo_descricao_curta,
      competencia_anterior.cid_grupo_descricao_curta
    ) AS cid_grupo_descricao_curta,
	sexo.nome AS usuario_sexo,
	saude_mental.classificar_binarios(
	   coalesce(
    	  competencia_atual.usuario_abuso_substancias,
    	  competencia_anterior.usuario_abuso_substancias
    	)
	) AS usuario_abuso_substancias,
	saude_mental.classificar_binarios(
	   coalesce(
    	  competencia_atual.usuario_situacao_rua,
    	  competencia_anterior.usuario_situacao_rua
	)
	) AS usuario_situacao_rua,
	raca_cor.nome AS usuario_raca_cor,
	coalesce(
	  competencia_atual.usuario_faixa_etaria,
	  competencia_anterior.usuario_faixa_etaria
	) AS usuario_faixa_etaria,
	coalesce(
	  competencia_atual.usuarios_novos,
	  0
	) AS usuarios_novos,
	coalesce(
	  competencia_anterior.usuarios_novos,
	  0
	) AS usuarios_novos_anterior,
	(
	   coalesce(competencia_atual.usuarios_novos, 0)
	   - coalesce(competencia_anterior.usuarios_novos, 0)
	) AS dif_usuarios_novos_anterior
FROM competencia_atual
FULL JOIN competencia_anterior
ON 
    competencia_atual.unidade_geografica_id
    = competencia_anterior.unidade_geografica_id
AND competencia_atual.periodo_id
    = competencia_anterior.proximo_periodo_id
AND	competencia_atual.estabelecimento_id_cnes
    = competencia_anterior.estabelecimento_id_cnes
AND	competencia_atual.cid_grupo_descricao_curta
    = competencia_anterior.cid_grupo_descricao_curta
AND competencia_atual.usuario_sexo_id_sigtap
    = competencia_anterior.usuario_sexo_id_sigtap
AND competencia_atual.usuario_abuso_substancias
    = competencia_anterior.usuario_abuso_substancias
AND competencia_atual.usuario_situacao_rua
    = competencia_anterior.usuario_situacao_rua
AND competencia_atual.usuario_raca_cor_id_siasus
    = competencia_anterior.usuario_raca_cor_id_siasus
AND competencia_atual.usuario_faixa_etaria
    = competencia_anterior.usuario_faixa_etaria
LEFT JOIN listas_de_codigos.estabelecimentos estabelecimento
    ON  estabelecimento.id_scnes = coalesce(
        competencia_atual.estabelecimento_id_cnes,
        competencia_anterior.estabelecimento_id_cnes
    )
LEFT JOIN listas_de_codigos.sexos sexo
    ON sexo.id_sigtap = coalesce(
        competencia_atual.usuario_sexo_id_sigtap,
        competencia_anterior.usuario_sexo_id_sigtap
    )
LEFT JOIN listas_de_codigos.racas_cores raca_cor
    ON raca_cor.id_siasus = coalesce(
        competencia_atual.usuario_raca_cor_id_siasus,
        competencia_anterior.usuario_raca_cor_id_siasus
    )
LEFT JOIN saude_mental._raas_ultima_competencia_disponivel ultima_competencia
    ON ultima_competencia.unidade_geografica_id = coalesce(
        competencia_atual.unidade_geografica_id,
        competencia_anterior.unidade_geografica_id
    )
WHERE coalesce(
    competencia_atual.competencia,
    competencia_anterior.proxima_competencia
)::date <= ultima_competencia.periodo_data_inicio
WITH NO DATA;
CREATE UNIQUE INDEX
    usuarios_novos_un
ON saude_mental.usuarios_novos (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    competencia,
    estabelecimento,
    cid_grupo_descricao_curta,
    usuario_sexo,
    usuario_abuso_substancias,
    usuario_situacao_rua,
    usuario_raca_cor,
    usuario_faixa_etaria
);

DROP MATERIALIZED VIEW IF EXISTS
    saude_mental.usuarios_novos_resumo_ultimo_mes
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.usuarios_novos_resumo_ultimo_mes
AS
WITH resumo_por_estabelecimento AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        competencia,
        nome_mes,
        estabelecimento_linha_perfil,
        estabelecimento,
        sum(usuarios_novos) AS usuarios_novos,
        sum(usuarios_novos_anterior) AS usuarios_novos_anterior,
        sum(dif_usuarios_novos_anterior) AS dif_usuarios_novos_anterior
    FROM saude_mental.usuarios_novos
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        estabelecimento_linha_perfil,
        estabelecimento,
        nome_mes,
        competencia
),
resumo_todos_estabelecimentos AS (
    SELECT 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        competencia,
        nome_mes,
        'Todas' as estabelecimento_linha_perfil,
        'Todos' AS estabelecimento,
        sum(usuarios_novos) AS usuarios_novos,
        sum(usuarios_novos_anterior) AS usuarios_novos_anterior,
        sum(dif_usuarios_novos_anterior) AS dif_usuarios_novos_anterior
    FROM resumo_por_estabelecimento
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        nome_mes,
        competencia
),
incluindo_estabelecimento_todos AS (
    SELECT * FROM resumo_por_estabelecimento
    UNION
    SELECT * FROM resumo_todos_estabelecimentos
)
SELECT 
    usuarios_ativos.unidade_geografica_id,
    usuarios_ativos.unidade_geografica_id_sus,
    usuarios_ativos.periodo_id,
    usuarios_ativos.competencia,
    usuarios_ativos.nome_mes,
    usuarios_ativos.estabelecimento_linha_perfil,
    usuarios_ativos.estabelecimento,
    coalesce(usuarios_novos, 0) AS usuarios_novos,
    coalesce(usuarios_novos_anterior, 0) AS usuarios_novos_anterior,
    coalesce(dif_usuarios_novos_anterior, 0) AS dif_usuarios_novos_anterior
FROM incluindo_estabelecimento_todos resumo_usuarios_novos
-- Junta com usuários ativos para garantir que todos os CAPS com usuários ativos
-- apareçam na consulta final, mesmo que não haja usuários novos no mês nem no
-- anterior
RIGHT JOIN
    saude_mental.usuarios_ativos_por_estabelecimento_resumo_ultimo_mes
    usuarios_ativos
USING (
    unidade_geografica_id,
    periodo_id,
    estabelecimento
)
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    usuarios_novos_resumo_ultimo_mes_un
ON saude_mental.usuarios_novos_resumo_ultimo_mes (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    estabelecimento_linha_perfil,
    estabelecimento,
    periodo_id,
    nome_mes,
    competencia
);
