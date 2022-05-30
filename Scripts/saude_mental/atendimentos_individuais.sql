/******************************************************************************
 *                                                                            *
 *                                                                            *
 *      USUÁRIOS EM CAPS QUE REALIZAM APENAS ATENDIMENTOS INDIVIDUAIS         *
 *                                                                            *
 *                                                                            *
 ******************************************************************************/



DROP MATERIALIZED VIEW IF EXISTS 
	saude_mental._usuarios_atendimentos_individuais
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental._usuarios_atendimentos_individuais
AS
WITH procedimentos_caps AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        realizacao_periodo_data_inicio,
        estabelecimento_id_cnes,
        usuario_cns_criptografado,
        saude_mental.classificar_faixa_etaria(
           usuario_data_nascimento,
           realizacao_periodo_data_inicio
        ) AS usuario_faixa_etaria,
        usuario_sexo_id_sigtap,
        condicao_principal_id_cid10,
        usuario_raca_cor_id_siasus,
        procedimento_id_sigtap NOT IN (
        -- lista de procedimentos BPA-i NÃO considerados atend individual
            '0301080321',  -- Acompanhamento de SRT por CAPS
            -- procedimentos lançados indevidamente como BPA-i
            '0101050011',  -- Praticas corporais em med tradicional chinesa 
            '0101050020',  -- Terapia comunitária
            '0301050023',  -- Assist domiciliar por equipe multiprofissional
            '0101050135',  -- Sessão de dança circular
            '0101050054',  -- Oficina de massagem/ auto-massagem
            '0101050089',  -- Sessão de musicoterapia
            '0301080143',  -- Atendimento em oficina terapeutica
            '0101050062',  -- Sessão de arteterapia
            '0101050070'   -- Sessão de meditação
        ) AS atendimento_individual,
        quantidade_apresentada
    FROM dados_publicos.siasus_bpa_i_disseminacao
    WHERE 
        estabelecimento_tipo_id_sigtap = '70'  -- CAPS
        -- ignorar acolhimentos iniciais
        AND procedimento_id_sigtap NOT IN (
            '0301080232'  -- ACOLHIMENTO INICIAL POR CAPS
            '0301040079'  -- ESCUTA INICIAL/ORIENTAÇÃO (ACOLHIM DEMANDA ESPONT)
        ) 
    UNION
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        realizacao_periodo_data_inicio,
        estabelecimento_id_cnes,
        usuario_cns_criptografado,
        saude_mental.classificar_faixa_etaria(
           usuario_data_nascimento,
           realizacao_periodo_data_inicio
        ) AS usuario_faixa_etaria,
        usuario_sexo_id_sigtap,
        condicao_principal_id_cid10,
        usuario_raca_cor_id_siasus,
        -- ATENDIMENTO INDIVIDUAL DE PACIENTE EM CAPS
        procedimento_id_sigtap = '0301080208' AS atendimento_individual,
        quantidade_apresentada
    FROM dados_publicos.siasus_raas_psicossocial_disseminacao
)
SELECT
    unidade_geografica_id,
	unidade_geografica_id_sus,
	periodo_id,
	realizacao_periodo_data_inicio AS competencia,
	estabelecimento_id_cnes,
	usuario_cns_criptografado,
	usuario_faixa_etaria,
	usuario_sexo_id_sigtap,
	cid.cid_grupo_descricao_curta,
    usuario_raca_cor_id_siasus,
	coalesce(
        sum(quantidade_apresentada) FILTER (WHERE NOT atendimento_individual),
        0
    ) > 0 AS procedimentos_alem_individual,
	(sum(quantidade_apresentada) > 0)::bool AS fez_procedimentos
FROM procedimentos_caps
-- TODO: usar tabela de CIDs do schema `listas_de_codigos`
LEFT JOIN saude_mental.cids cid
ON condicao_principal_id_cid10 = cid_id
-- TODO: adicionar procedimentos registrados em BPAi
GROUP BY 
    unidade_geografica_id,
	unidade_geografica_id_sus,
    periodo_id,
	realizacao_periodo_data_inicio,
	estabelecimento_id_cnes,
	usuario_cns_criptografado,
	usuario_faixa_etaria,
	usuario_sexo_id_sigtap,
	cid_grupo_descricao_curta,
    usuario_raca_cor_id_siasus
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
	_usuarios_atendimentos_individuais_un
ON saude_mental._usuarios_atendimentos_individuais (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    competencia,
    estabelecimento_id_cnes,
    usuario_faixa_etaria,
    usuario_sexo_id_sigtap,
    cid_grupo_descricao_curta,
    usuario_raca_cor_id_siasus,
    usuario_cns_criptografado
);


DROP MATERIALIZED VIEW IF EXISTS 
    saude_mental.usuarios_atendimentos_individuais_perfil
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.usuarios_atendimentos_individuais_perfil
AS
WITH perfil_por_estabelecimento AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        competencia,
        coalesce(
            estabelecimento.nome_curto,
            estabelecimento.nome
        ) AS estabelecimento,
        usuario_faixa_etaria,
        sexo.nome AS usuario_sexo,
        coalesce(
            cid_grupo_descricao_curta,
            'Outras condições'
        ) AS cid_grupo_descricao_curta,
        raca_cor.nome AS usuario_raca_cor,
        count(DISTINCT usuario_cns_criptografado) FILTER (
            WHERE fez_procedimentos AND NOT procedimentos_alem_individual
        ) AS usuarios_apenas_atendimento_individual,
        count(DISTINCT usuario_cns_criptografado) FILTER (
            WHERE fez_procedimentos
        ) AS usuarios_frequentantes
    FROM saude_mental._usuarios_atendimentos_individuais usuario
    LEFT JOIN listas_de_codigos.estabelecimentos estabelecimento
    ON usuario.estabelecimento_id_cnes = estabelecimento.id_scnes
    LEFT JOIN listas_de_codigos.sexos sexo
    ON usuario.usuario_sexo_id_sigtap = sexo.id_sigtap
    LEFT JOIN listas_de_codigos.racas_cores raca_cor
    ON usuario.usuario_raca_cor_id_siasus = raca_cor.id_siasus
    WHERE NOT procedimentos_alem_individual
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        competencia,
        coalesce(estabelecimento.nome_curto, estabelecimento.nome),
        usuario_faixa_etaria,
        sexo.nome,
        cid_grupo_descricao_curta,
        usuario_raca_cor
),
perfil_todos_estabelecimentos AS (
    SELECT 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        competencia,
        'Todos' AS estabelecimento,
        usuario_faixa_etaria,
        usuario_sexo,
        cid_grupo_descricao_curta,
        usuario_raca_cor,
        sum(
            usuarios_apenas_atendimento_individual
        ) AS usuarios_apenas_atendimento_individual,
        sum(usuarios_frequentantes) AS usuarios_frequentantes
    FROM perfil_por_estabelecimento
    GROUP BY 
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        competencia,
        usuario_faixa_etaria,
        usuario_sexo,
        cid_grupo_descricao_curta,
        usuario_raca_cor
)
SELECT * FROM perfil_por_estabelecimento
UNION
SELECT * FROM perfil_todos_estabelecimentos
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    usuarios_atendimentos_individuais_perfil_un
ON saude_mental.usuarios_atendimentos_individuais_perfil (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    estabelecimento,
    usuario_faixa_etaria,
    usuario_sexo,
    cid_grupo_descricao_curta,
    usuario_raca_cor,
    competencia,
    periodo_id
);


DROP MATERIALIZED VIEW IF EXISTS 
    saude_mental.atendimentos_individuais_perfil_resumo_ultimo_mes
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.atendimentos_individuais_perfil_resumo_ultimo_mes
AS
WITH
ultima_competencia_disponivel AS (
    SELECT
        _raas_ultima_competencia_disponivel.unidade_geografica_id,
        least(
            _raas_ultima_competencia_disponivel.periodo_data_inicio,
            _bpa_i_caps_ultima_competencia_disponivel.periodo_data_inicio
        ) AS periodo_data_inicio
    FROM saude_mental._raas_ultima_competencia_disponivel
    LEFT JOIN saude_mental._bpa_i_caps_ultima_competencia_disponivel
    USING (
        unidade_geografica_id
    )
),
perfil_ultimo_mes AS (
    SELECT
        usuarios_atendimentos_individuais_perfil.*
    FROM saude_mental.usuarios_atendimentos_individuais_perfil
    INNER JOIN ultima_competencia_disponivel
    ON 
        usuarios_atendimentos_individuais_perfil.unidade_geografica_id
        = ultima_competencia_disponivel.unidade_geografica_id
    AND usuarios_atendimentos_individuais_perfil.competencia
        = ultima_competencia_disponivel.periodo_data_inicio
),
perfil_sexo AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        usuario_sexo,
        sum(
            usuarios_apenas_atendimento_individual
        ) AS usuarios_apenas_atendimento_individual
--        round(
--            100 * sum(usuarios_apenas_atendimento_individual)::NUMERIC
--            / nullif(sum(usuarios_frequentantes), 0),
--            2
--        ) AS perc_apenas_atendimentos_individuais
    FROM perfil_ultimo_mes
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        usuario_sexo
),
sexo_predominante AS (
    SELECT
        DISTINCT ON (
            unidade_geografica_id
        )
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        usuario_sexo AS sexo_predominante,
        (
            usuarios_apenas_atendimento_individual
        ) AS usuarios_sexo_predominante
    FROM perfil_sexo
    ORDER BY
        unidade_geografica_id,
        usuarios_apenas_atendimento_individual DESC        
),
perfil_faixa_etaria AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        usuario_faixa_etaria,
        sum(
            usuarios_apenas_atendimento_individual
        ) AS usuarios_apenas_atendimento_individual
--        round(
--            100 * sum(usuarios_apenas_atendimento_individual)::NUMERIC
--            / nullif(sum(usuarios_frequentantes), 0),
--            2
--        ) AS perc_apenas_atendimentos_individuais
    FROM perfil_ultimo_mes
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        usuario_faixa_etaria
),
faixa_etaria_predominante AS (
    SELECT
        DISTINCT ON (
            unidade_geografica_id
        )
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        usuario_faixa_etaria AS faixa_etaria_predominante,
        (
            usuarios_apenas_atendimento_individual
        ) AS usuarios_faixa_etaria_predominante
    FROM perfil_faixa_etaria
    ORDER BY
        unidade_geografica_id,
        usuarios_apenas_atendimento_individual DESC        
),
perfil_cid_grupo AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        cid_grupo_descricao_curta,
        sum(
            usuarios_apenas_atendimento_individual
        ) AS usuarios_apenas_atendimento_individual
--        round(
--            100 * sum(usuarios_apenas_atendimento_individual)::NUMERIC
--            / nullif(sum(usuarios_frequentantes), 0),
--            2
--        ) AS perc_apenas_atendimentos_individuais
    FROM perfil_ultimo_mes
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        cid_grupo_descricao_curta
),
cid_grupo_predominante AS (
    SELECT
        DISTINCT ON (
            unidade_geografica_id
        )
        unidade_geografica_id,
        unidade_geografica_id_sus,
        competencia,
        periodo_id,
        cid_grupo_descricao_curta AS cid_grupo_predominante,
        usuarios_apenas_atendimento_individual AS usuarios_cid_predominante
    FROM perfil_cid_grupo
    ORDER BY
        unidade_geografica_id,
        usuarios_apenas_atendimento_individual DESC        
)
SELECT 
    listas_de_codigos.nome_mes(coalesce(
       sexo_predominante.competencia,
       faixa_etaria_predominante.competencia,
       cid_grupo_predominante.competencia
    )) AS nome_mes,
    *
FROM sexo_predominante
FULL JOIN faixa_etaria_predominante
USING (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    competencia,
    periodo_id
)
FULL JOIN cid_grupo_predominante
USING (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    competencia,
    periodo_id
)
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    atendimentos_individuais_perfil_resumo_ultimo_mes 
ON saude_mental.atendimentos_individuais_perfil_resumo_ultimo_mes (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    competencia,
    periodo_id
);


DROP MATERIALIZED VIEW IF EXISTS
	saude_mental._atendimentos_individuais_por_caps
CASCADE;
CREATE MATERIALIZED VIEW 
	saude_mental._atendimentos_individuais_por_caps
AS
WITH _por_estabelecimento AS (
    SELECT
        unidade_geografica_id,
    	unidade_geografica_id_sus,
    	periodo_id,
    	competencia,
    	estabelecimento_id_cnes,
    	count(DISTINCT usuario_cns_criptografado) FILTER (
            WHERE fez_procedimentos AND NOT procedimentos_alem_individual
        ) AS usuarios_apenas_atendimento_individual,
        count(DISTINCT usuario_cns_criptografado) FILTER (
            WHERE fez_procedimentos
        ) AS fizeram_algum_procedimento
    FROM saude_mental._usuarios_atendimentos_individuais
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        competencia,
        estabelecimento_id_cnes
),
por_estabelecimento AS (
    SELECT
        *,
        round(
            100 * usuarios_apenas_atendimento_individual::numeric
            / nullif(fizeram_algum_procedimento, 0),
            1
        ) AS perc_apenas_atendimentos_individuais
    FROM _por_estabelecimento
),
todos_estabelecimentos AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        competencia,
        '0000000' AS estabelecimento_id_cnes,
        sum(
            usuarios_apenas_atendimento_individual
        ) AS usuarios_apenas_atendimento_individual,
        sum(fizeram_algum_procedimento) AS fizeram_algum_procedimento,
        round(
            100 * sum(usuarios_apenas_atendimento_individual)::numeric
            / nullif(sum(fizeram_algum_procedimento), 0),
            1
        ) AS perc_apenas_atendimentos_individuais
    FROM por_estabelecimento
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        competencia
),
por_estabelecimento_com_total AS (
    SELECT *
    FROM por_estabelecimento
    UNION
    SELECT *
    FROM todos_estabelecimentos
),
estabelecimento_maior_taxa AS (
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
        perc_apenas_atendimentos_individuais AS maior_taxa
    FROM por_estabelecimento
    LEFT JOIN listas_de_codigos.estabelecimentos estabelecimento
    ON por_estabelecimento.estabelecimento_id_cnes = estabelecimento.id_scnes
    ORDER BY 
        unidade_geografica_id,
        periodo_id,
        perc_apenas_atendimentos_individuais DESC
)
SELECT *
FROM por_estabelecimento_com_total
LEFT JOIN estabelecimento_maior_taxa
USING (
    unidade_geografica_id,
    periodo_id
)
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
	_atendimentos_individuais_por_caps_un
ON saude_mental._atendimentos_individuais_por_caps (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    competencia,
    estabelecimento_id_cnes
);


DROP MATERIALIZED VIEW IF EXISTS
	saude_mental.atendimentos_individuais_por_caps
CASCADE;
CREATE MATERIALIZED VIEW
	saude_mental.atendimentos_individuais_por_caps
AS
SELECT
    competencia_atual.unidade_geografica_id,
    competencia_atual.unidade_geografica_id_sus,
	sucessao.periodo_id,
    competencia_atual.competencia AS competencia,
    listas_de_codigos.nome_mes(competencia_atual.competencia) AS nome_mes,
    saude_mental.classificar_linha_perfil(
        coalesce(estabelecimento.nome, 'Todos')
    ) AS estabelecimento_linha_perfil,
	coalesce(
        estabelecimento.nome_curto,
        estabelecimento.nome,
        'Todos'
    ) AS estabelecimento,
    coalesce(
        competencia_atual.perc_apenas_atendimentos_individuais,
        0::bigint
    ) AS perc_apenas_atendimentos_individuais,
    coalesce(
        competencia_anterior.perc_apenas_atendimentos_individuais,
        0::bigint
    ) AS perc_apenas_atendimentos_individuais_anterior,
    (
    	coalesce(competencia_atual.perc_apenas_atendimentos_individuais, 0)
    	- coalesce(competencia_anterior.perc_apenas_atendimentos_individuais, 0)
    ) AS dif_perc_apenas_atendimentos_individuais,
    competencia_atual.estabelecimento_maior_taxa,
    competencia_atual.maior_taxa
FROM saude_mental._atendimentos_individuais_por_caps competencia_atual
LEFT JOIN 
	listas_de_codigos.periodos_sucessao sucessao 
ON
	competencia_atual.periodo_id = sucessao.periodo_id
AND sucessao.periodo_tipo::text = 'Mensal'::text
FULL JOIN 
	saude_mental._atendimentos_individuais_por_caps competencia_anterior
ON
    competencia_atual.unidade_geografica_id
    = competencia_anterior.unidade_geografica_id
AND	sucessao.ultimo_periodo_id = competencia_anterior.periodo_id
AND competencia_atual.estabelecimento_id_cnes
    = competencia_anterior.estabelecimento_id_cnes
LEFT JOIN listas_de_codigos.estabelecimentos estabelecimento
ON
	estabelecimento.id_scnes = coalesce(
		competencia_atual.estabelecimento_id_cnes,
		competencia_anterior.estabelecimento_id_cnes
	)
WHERE 
	competencia_atual.fizeram_algum_procedimento IS NOT NULL 
AND competencia_anterior.fizeram_algum_procedimento IS NOT NULL
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
	atendimentos_individuais_por_caps_un
ON saude_mental.atendimentos_individuais_por_caps (
	unidade_geografica_id,
	unidade_geografica_id_sus,
	estabelecimento,
	competencia,
	periodo_id
);

DROP MATERIALIZED VIEW IF EXISTS
	saude_mental.atendimentos_individuais_por_caps_ultimo_mes
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.atendimentos_individuais_por_caps_ultimo_mes
AS
SELECT 
    DISTINCT ON (
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento
    )
	*
  FROM saude_mental.atendimentos_individuais_por_caps
  ORDER BY
    unidade_geografica_id,
    unidade_geografica_id_sus,
    estabelecimento,
  	competencia DESC
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
	atendimentos_individuais_por_caps_ultimo_mes_un
ON saude_mental.atendimentos_individuais_por_caps_ultimo_mes (
	unidade_geografica_id,
	unidade_geografica_id_sus,
	estabelecimento
);
CALL saude_mental.atualizar_atendimentos_individuais();
