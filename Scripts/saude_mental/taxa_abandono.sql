/**************************************************************************

			CÁLCULO DE ABANDONOS ENTRE USUÁRIOS RECENTES EM CAPS


 **************************************************************************/


-- Obter primeito procedimento RAAS por combinação de CAPS e usuário (CNS)
DROP MATERIALIZED VIEW IF EXISTS 
saude_mental._raas_primeiro_procedimento
CASCADE;
CREATE MATERIALIZED VIEW 
    saude_mental._raas_primeiro_procedimento
AS
-- TODO: Combinar procedimentos lançados incorretamente em BPA-i
SELECT
    DISTINCT ON
    (
        estabelecimento_id_cnes,
        usuario_cns_criptografado
    )
	raas.*
FROM
    dados_publicos.siasus_raas_psicossocial_disseminacao raas
ORDER BY
    estabelecimento_id_cnes,
    usuario_cns_criptografado,
    realizacao_periodo_data_inicio ASC
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    _raas_primeiro_procedimento_un
ON saude_mental._raas_primeiro_procedimento (
    estabelecimento_id_cnes,
    usuario_cns_criptografado
);
CREATE INDEX IF NOT EXISTS
    _raas_primeiro_procedimento_periodo_id_idx
ON saude_mental._raas_primeiro_procedimento (
    periodo_id
);


-- Definir os usuários como recentes durante os primeiros seis meses após o
-- primeiro procedimento. Criar uma VIEW replicando os dados cadastrais registrados
-- no primeiro procedimento ao para cada competência ao longo desses seis meses.
DROP MATERIALIZED VIEW IF EXISTS
    saude_mental._usuarios_recentes
CASCADE;
CREATE MATERIALIZED VIEW 
    saude_mental._usuarios_recentes
AS
SELECT
    periodo.id AS periodo_id,
    periodo.data_inicio AS periodo_data_inicio,
    unidade_geografica_id,
    unidade_geografica_id_sus,
    estabelecimento_id_cnes,
    usuario_cns_criptografado,
    condicao_principal_id_cid10,
    usuario_sexo_id_sigtap,
    saude_mental.classificar_faixa_etaria(
        usuario_data_nascimento,
        periodo.data_inicio
    ) AS usuario_faixa_etaria,
    usuario_raca_cor_id_siasus,
    primeiro_procedimento.realizacao_periodo_data_inicio 
    AS primeiro_procedimento_periodo_data_inicio,
    date_trunc(
        'month',
        primeiro_procedimento.realizacao_periodo_data_inicio 
        + '6 mon 2 days'::interval
    ) AS data_deixa_de_ser_recente
FROM
    saude_mental._raas_primeiro_procedimento primeiro_procedimento
LEFT JOIN listas_de_codigos.periodos periodo
ON
    periodo.tipo = 'Mensal'
AND periodo.data_inicio >= primeiro_procedimento.realizacao_periodo_data_inicio
AND periodo.data_fim < date_trunc(
    'month',
    primeiro_procedimento.realizacao_periodo_data_inicio
    + '6 mon 2 days'::interval
)
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    _usuarios_recentes_un_resumido
ON saude_mental._usuarios_recentes (
    estabelecimento_id_cnes,
    usuario_cns_criptografado,
    periodo_id
);
CREATE UNIQUE INDEX IF NOT EXISTS
    _usuarios_recentes_un_completo
ON saude_mental._usuarios_recentes (
    periodo_id,
    periodo_data_inicio,
    unidade_geografica_id,
    unidade_geografica_id_sus,
    estabelecimento_id_cnes,
    usuario_cns_criptografado,
    condicao_principal_id_cid10,
    usuario_sexo_id_sigtap,
    usuario_faixa_etaria,
    usuario_raca_cor_id_siasus,
    primeiro_procedimento_periodo_data_inicio,
    data_deixa_de_ser_recente
);


-- Usuários com perfil ambulatorial - devem ser removidos do cálculo de abandono
DROP MATERIALIZED VIEW IF EXISTS
    saude_mental._usuarios_recentes_perfil_ambulatorial 
CASCADE;
CREATE MATERIALIZED VIEW 
    saude_mental._usuarios_recentes_perfil_ambulatorial
AS
SELECT
    raas.estabelecimento_id_cnes,
    raas.usuario_cns_criptografado,
    -- Nenhum procedimento diferente de atendimento individual em CAPS foi 
    -- registrados na RAAS do usuário recente
    NOT (
        '0301080208' <> ANY(array_agg(raas.procedimento_id_sigtap))
    ) AS tem_perfil_ambulatorial
FROM saude_mental._usuarios_recentes usuarios_recentes 
-- TODO: Considerar procedimentos lançados incorretamente em BPAi
INNER JOIN dados_publicos.siasus_raas_psicossocial_disseminacao raas
ON
    raas.estabelecimento_id_cnes = usuarios_recentes.estabelecimento_id_cnes
AND raas.usuario_cns_criptografado = usuarios_recentes.usuario_cns_criptografado
AND (
    raas.realizacao_periodo_data_inicio 
    < usuarios_recentes.data_deixa_de_ser_recente
)
WHERE raas.quantidade_apresentada > 0 
GROUP BY
    raas.estabelecimento_id_cnes,
    raas.usuario_cns_criptografado
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    _usuarios_recentes_perfil_ambulatorial_un
ON saude_mental._usuarios_recentes_perfil_ambulatorial (
    estabelecimento_id_cnes,
    usuario_cns_criptografado
);


-- Cruzar usuários recentes com usuários que se tornaram inativos
DROP MATERIALIZED VIEW IF EXISTS
    saude_mental._usuarios_recentes_abandono 
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental._usuarios_recentes_abandono 
AS 
WITH inatividade AS (
    SELECT
        estabelecimento_id_cnes,
        usuario_cns_criptografado,
        -- competência seguinte à ultima em que frequentou
        date_trunc(
            'month',
            periodo_data_inicio - '2 mon'::INTERVAL
        ) AS inatividade_periodo_data_inicio
    FROM
        saude_mental._usuarios_ativos
    WHERE
        periodo_data_inicio >= '2013-01-01'::date
    AND ativo_4meses
    AND NOT ativo_3meses
)
SELECT
    usuario.unidade_geografica_id,
    usuario.unidade_geografica_id_sus,
    usuario.periodo_id,
    usuario.periodo_data_inicio,
    usuario.estabelecimento_id_cnes,
    usuario.usuario_cns_criptografado,
    usuario.condicao_principal_id_cid10,
    usuario.usuario_sexo_id_sigtap,
    usuario.usuario_faixa_etaria,
    usuario.usuario_raca_cor_id_siasus,
    usuario.primeiro_procedimento_periodo_data_inicio,
    usuario.data_deixa_de_ser_recente,
    coalesce(
        min(inatividade_periodo_data_inicio) < data_deixa_de_ser_recente,
        FALSE
    ) AS abandonou,
    min(inatividade_periodo_data_inicio) AS inatividade_periodo_data_inicio
FROM saude_mental._usuarios_recentes AS usuario
LEFT JOIN inatividade
ON
    usuario.estabelecimento_id_cnes = inatividade.estabelecimento_id_cnes
    AND usuario.usuario_cns_criptografado = inatividade.usuario_cns_criptografado
    AND inatividade.inatividade_periodo_data_inicio > usuario.primeiro_procedimento_periodo_data_inicio
-- remover usuários com perfil exclusivamente "ambulatorial"
LEFT JOIN saude_mental._usuarios_recentes_perfil_ambulatorial perfil_ambulatorial
ON
    usuario.estabelecimento_id_cnes = perfil_ambulatorial.estabelecimento_id_cnes
AND usuario.usuario_cns_criptografado 
    = perfil_ambulatorial.usuario_cns_criptografado
WHERE 
    perfil_ambulatorial.usuario_cns_criptografado IS NULL
    OR NOT perfil_ambulatorial.tem_perfil_ambulatorial
GROUP BY
    usuario.periodo_id,
    usuario.periodo_data_inicio,
    usuario.unidade_geografica_id,
    usuario.unidade_geografica_id_sus,
    usuario.estabelecimento_id_cnes,
    usuario.usuario_cns_criptografado,
    usuario.condicao_principal_id_cid10,
    usuario.usuario_sexo_id_sigtap,
    usuario.usuario_faixa_etaria,
    usuario.usuario_raca_cor_id_siasus,
    usuario.primeiro_procedimento_periodo_data_inicio,
    usuario.data_deixa_de_ser_recente
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    _usuarios_recentes_abandono_un
ON saude_mental._usuarios_recentes_abandono (
    periodo_id,
    periodo_data_inicio,
    unidade_geografica_id,
    unidade_geografica_id_sus,
    estabelecimento_id_cnes,
    usuario_cns_criptografado,
    condicao_principal_id_cid10,
    usuario_sexo_id_sigtap,
    usuario_faixa_etaria,
    usuario_raca_cor_id_siasus,
    primeiro_procedimento_periodo_data_inicio,
    data_deixa_de_ser_recente
);


-- Caracterização do perfil dos usuários que abandonaram
DROP MATERIALIZED VIEW IF EXISTS
    saude_mental.abandonos_perfil_usuarios
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.abandonos_perfil_usuarios 
AS
SELECT
    usuario_abandonou.unidade_geografica_id,
    usuario_abandonou.unidade_geografica_id_sus,
    periodo.id AS periodo_id,
    usuario_abandonou.inatividade_periodo_data_inicio AS periodo_data_inicio,
    coalesce(
        estabelecimento.nome_mes,
        estabelecimento.nome
    ) AS estabelecimento,
    sexo.nome AS usuario_sexo,
    usuario_abandonou.usuario_faixa_etaria,
    cid.cid_grupo_descricao_curta,
    raca_cor.nome AS usuario_raca_cor,
    count(DISTINCT usuario_abandonou.usuario_cns_criptografado) AS abandonaram
FROM saude_mental._usuarios_recentes_abandono usuario_abandonou
LEFT JOIN listas_de_codigos.estabelecimentos estabelecimento
ON usuario_abandonou.estabelecimento_id_cnes = estabelecimento.id_scnes
LEFT JOIN listas_de_codigos.sexos sexo
ON usuario_abandonou.usuario_sexo_id_sigtap = sexo.id_sigtap
-- TODO: trocar por tabela de CIDs no schema `listas_de_codigos`
LEFT JOIN saude_mental.cids cid
ON usuario_abandonou.condicao_principal_id_cid10 = cid.cid_id 
LEFT JOIN listas_de_codigos.racas_cores raca_cor
ON usuario_abandonou.usuario_raca_cor_id_siasus = raca_cor.id_siasus
LEFT JOIN listas_de_codigos.periodos periodo
ON usuario_abandonou.inatividade_periodo_data_inicio = periodo.data_inicio
LEFT JOIN
    saude_mental._raas_ultima_competencia_disponivel
    ultima_competencia_disponivel
ON 
    usuario_abandonou.unidade_geografica_id
    = ultima_competencia_disponivel.unidade_geografica_id
WHERE
        abandonou
    -- são necessários 2 meses após a competência de inatividade para confirmar
    -- que o vínculo foi interrompido
    AND inatividade_periodo_data_inicio
        <= ultima_competencia_disponivel.periodo_data_inicio - '2 mon'::interval
GROUP BY
    usuario_abandonou.unidade_geografica_id,
    usuario_abandonou.unidade_geografica_id_sus,
    periodo.id,
    usuario_abandonou.inatividade_periodo_data_inicio,
    estabelecimento.nome,
    sexo.nome,
    usuario_abandonou.usuario_faixa_etaria,
    cid.cid_grupo_descricao_curta,
    raca_cor.nome
WITH NO DATA;


-- Taxa mensal de abandonos entre usuários recentes. 
-- Considera o número de abandonos no mês como numerador, e o número de usuários
-- recentes sem histórico prévio de inatividade como denominador. 
DROP MATERIALIZED VIEW IF EXISTS 
    saude_mental._usuarios_recentes_abandono_mensal 
CASCADE;
CREATE MATERIALIZED VIEW 
    saude_mental._usuarios_recentes_abandono_mensal 
AS
WITH abandono_mensal_por_caps AS (
    SELECT
        periodo_id,
        periodo_data_inicio,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_id_cnes,
        count(DISTINCT usuario_cns_criptografado) FILTER (
            WHERE inatividade_periodo_data_inicio::date
            = periodo_data_inicio::date
        ) AS usuarios_recentes_abandonaram_no_mes,
        count(
            DISTINCT usuario_cns_criptografado
        ) AS usuarios_recentes_sem_inatividade_previa
    FROM
        saude_mental._usuarios_recentes_abandono
    GROUP BY
        periodo_id,
        periodo_data_inicio,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento_id_cnes
),
abandono_mensal_todos_caps AS (
    SELECT
        periodo_id,
        periodo_data_inicio,
        unidade_geografica_id,
        unidade_geografica_id_sus,
        '0000000' AS estabelecimento_id_cnes,
        count(DISTINCT usuario_cns_criptografado) FILTER (
            WHERE inatividade_periodo_data_inicio::date
            = periodo_data_inicio::date
        ) AS usuarios_recentes_abandonaram_no_mes,
        count(
            DISTINCT usuario_cns_criptografado
        ) AS usuarios_recentes_sem_inatividade_previa
    FROM
        saude_mental._usuarios_recentes_abandono
    GROUP BY
        periodo_id,
        periodo_data_inicio,
        unidade_geografica_id,
        unidade_geografica_id_sus
),
abandono_mensal_incluindo_total_municipio AS (
    SELECT * FROM abandono_mensal_por_caps
    UNION
    SELECT * FROM abandono_mensal_todos_caps
)
SELECT
    abandono_mensal.periodo_id,
    abandono_mensal.periodo_data_inicio,
    abandono_mensal.unidade_geografica_id,
    abandono_mensal.unidade_geografica_id_sus,
    abandono_mensal.estabelecimento_id_cnes,
	usuarios_recentes_abandonaram_no_mes,
    usuarios_recentes_sem_inatividade_previa,
    round(
		100 * coalesce(usuarios_recentes_abandonaram_no_mes, 0)
        / nullif(usuarios_recentes_sem_inatividade_previa, 0)::numeric,
		1
	) AS abandono_usuarios_recentes_taxa_mensal
FROM abandono_mensal_incluindo_total_municipio abandono_mensal
LEFT JOIN
    saude_mental._raas_ultima_competencia_disponivel
    ultima_competencia_disponivel
ON 
    abandono_mensal.unidade_geografica_id
    = ultima_competencia_disponivel.unidade_geografica_id
WHERE
    -- são necessários 2 meses após a competência de inatividade para confirmar
    -- que o vínculo foi interrompido
    abandono_mensal.periodo_data_inicio
    <= ultima_competencia_disponivel.periodo_data_inicio - '2 mon'::interval
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    _usuarios_recentes_abandono_mensal_un
ON saude_mental._usuarios_recentes_abandono_mensal (
    periodo_id,
    periodo_data_inicio,
    unidade_geografica_id,
    unidade_geografica_id_sus,
    estabelecimento_id_cnes
);



-- Apresentação para uso no banco de produção e no painel, incluindo comparação com o mês anterior
DROP MATERIALIZED VIEW IF EXISTS
    saude_mental.abandono_mensal
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.abandono_mensal
AS
SELECT
    abandono_mensal.unidade_geografica_id,
    abandono_mensal.unidade_geografica_id_sus,
    abandono_mensal.periodo_id,
    abandono_mensal.periodo_data_inicio AS competencia,
    listas_de_codigos.nome_mes(abandono_mensal.periodo_data_inicio) AS nome_mes,
    coalesce(
        estabelecimento.nome_curto,
        estabelecimento.nome,
        'Todos'
    ) AS estabelecimento,
    abandono_mensal.usuarios_recentes_abandonaram_no_mes,
    abandono_mensal.usuarios_recentes_sem_inatividade_previa,
    abandono_mensal.abandono_usuarios_recentes_taxa_mensal
FROM saude_mental._usuarios_recentes_abandono_mensal abandono_mensal
LEFT JOIN listas_de_codigos.estabelecimentos estabelecimento
ON abandono_mensal.estabelecimento_id_cnes = estabelecimento.id_scnes
LEFT JOIN
    saude_mental._raas_ultima_competencia_disponivel
    ultima_competencia_disponivel
ON 
    abandono_mensal.unidade_geografica_id
    = ultima_competencia_disponivel.unidade_geografica_id
WHERE
    -- são necessários 2 meses após a competência de inatividade para confirmar
    -- que o vínculo foi interrompido
    abandono_mensal.periodo_data_inicio
    <= ultima_competencia_disponivel.periodo_data_inicio - '2 mon'::interval
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    abandono_mensal_un
ON saude_mental.abandono_mensal(
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    competencia,
    estabelecimento
);

-- Taxa consolidada de abandonos entre usuários recentes, por coortes de usuários acompanhados durante os primeiros seis 
-- meses de vínculo com o estabelecimento.
-- Considera o número de abandonos no mês como numerador, e o número de usuários recentes sem histórico prévio 
-- de inatividade como denominador. 
DROP MATERIALIZED VIEW IF EXISTS
    saude_mental._usuarios_recentes_abandono_coortes
CASCADE;
CREATE MATERIALIZED VIEW 
    saude_mental._usuarios_recentes_abandono_coortes
AS
WITH
abandonos_por_estabelecimento AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        primeiro_procedimento_periodo_data_inicio,
        data_deixa_de_ser_recente,
        estabelecimento_id_cnes,
        count(DISTINCT usuario_cns_criptografado) FILTER (
            WHERE abandonou
        ) AS coorte_usuarios_abandonaram,
        count(DISTINCT usuario_cns_criptografado) AS coorte_usuarios_total,
        mode() WITHIN GROUP (
            ORDER BY usuario_sexo_id_sigtap
        ) FILTER (WHERE abandonou) AS sexo_predominante_id_sigtap,
        mode() WITHIN GROUP (
            ORDER BY usuario_faixa_etaria
        ) FILTER (WHERE abandonou) AS faixa_etaria_predominante
    FROM saude_mental._usuarios_recentes_abandono
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        primeiro_procedimento_periodo_data_inicio,
        data_deixa_de_ser_recente,
        estabelecimento_id_cnes
),
abandonos_todos_estabelecimentos AS (
    SELECT
        _usuarios_recentes_abandono.unidade_geografica_id,
        _usuarios_recentes_abandono.unidade_geografica_id_sus,
        primeiro_procedimento_periodo_data_inicio,
        data_deixa_de_ser_recente,
        '0000000' AS estabelecimento_id_cnes,
        count(DISTINCT usuario_cns_criptografado) FILTER (
            WHERE abandonou
        ) AS coorte_usuarios_abandonaram,
        count(DISTINCT usuario_cns_criptografado) AS coorte_usuarios_total,
        mode() WITHIN GROUP (
            ORDER BY usuario_sexo_id_sigtap
        ) FILTER (WHERE abandonou) AS sexo_predominante_id_sigtap,
        mode() WITHIN GROUP (
            ORDER BY usuario_faixa_etaria
        ) FILTER (WHERE abandonou) AS faixa_etaria_predominante
    FROM saude_mental._usuarios_recentes_abandono
    LEFT JOIN
        saude_mental._raas_ultima_competencia_disponivel
        ultima_competencia_disponivel
    ON 
        _usuarios_recentes_abandono.unidade_geografica_id
        = ultima_competencia_disponivel.unidade_geografica_id
    WHERE
        -- são necessários 2 meses após a competência de inatividade para
        -- confirmar que o vínculo foi interrompido
        _usuarios_recentes_abandono.periodo_data_inicio
        <= ultima_competencia_disponivel.periodo_data_inicio - '2 mon'::interval
    GROUP BY
        _usuarios_recentes_abandono.unidade_geografica_id,
        _usuarios_recentes_abandono.unidade_geografica_id_sus,
        primeiro_procedimento_periodo_data_inicio,
        data_deixa_de_ser_recente
),
abandonos_por_estabelecimento_com_total AS (
    SELECT * FROM abandonos_por_estabelecimento
    UNION 
    SELECT * FROM abandonos_todos_estabelecimentos
),
estabelecimento_maior_taxa_por_municipio AS (
    SELECT
        DISTINCT ON (
            abandonos.unidade_geografica_id,
            abandonos.primeiro_procedimento_periodo_data_inicio
        )
        abandonos.unidade_geografica_id,
        primeiro_procedimento_periodo_data_inicio,
        estabelecimento_id_cnes AS estabelecimento_maior_taxa_id_cnes,
        round( 
            100 * coorte_usuarios_abandonaram::numeric
            / nullif(coorte_usuarios_total, 0),
            1
        ) AS maior_taxa
    FROM abandonos_por_estabelecimento abandonos
    ORDER BY
        abandonos.unidade_geografica_id,
        abandonos.primeiro_procedimento_periodo_data_inicio,
        (
            coorte_usuarios_abandonaram::NUMERIC
            / nullif(coorte_usuarios_total, 0)
        ) DESC
),
abandonos_por_grupos_condicao_por_estabelecimento AS (
    SELECT
        _usuarios_recentes_abandono.unidade_geografica_id,
        primeiro_procedimento_periodo_data_inicio,
        estabelecimento_id_cnes,
        cid.cid_grupo_descricao_curta,
        count(DISTINCT usuario_cns_criptografado) AS quantidade_usuarios
    FROM saude_mental._usuarios_recentes_abandono
    -- TODO: trocar por tabela de CIDs do schema `listas_de_codigos`
    LEFT JOIN saude_mental.cids cid
    ON _usuarios_recentes_abandono.condicao_principal_id_cid10 = cid.cid_id
    GROUP BY
        _usuarios_recentes_abandono.unidade_geografica_id,
        primeiro_procedimento_periodo_data_inicio,
        estabelecimento_id_cnes,
        cid.cid_grupo_descricao_curta
),
abandonos_por_grupos_condicao_total AS (
    SELECT
        unidade_geografica_id,
        primeiro_procedimento_periodo_data_inicio,
        '0000000' AS estabelecimento_id_cnes,
        cid_grupo_descricao_curta,
        sum(quantidade_usuarios) AS quantidade_usuarios
    FROM abandonos_por_grupos_condicao_por_estabelecimento
    GROUP BY
        unidade_geografica_id,
        primeiro_procedimento_periodo_data_inicio,
        cid_grupo_descricao_curta
),
abandonos_por_grupos_condicao_por_estabelecimento_com_total AS (
    SELECT * FROM abandonos_por_grupos_condicao_por_estabelecimento
    UNION
    SELECT * FROM abandonos_por_grupos_condicao_total
),
condicao_predominante AS (
    SELECT 
        DISTINCT ON (
            unidade_geografica_id,
            primeiro_procedimento_periodo_data_inicio,
            estabelecimento_id_cnes
        )
        unidade_geografica_id,
        primeiro_procedimento_periodo_data_inicio,
        estabelecimento_id_cnes,
        cid_grupo_descricao_curta AS condicao_predominante,
        quantidade_usuarios AS condicao_predominante_usuarios
    FROM abandonos_por_grupos_condicao_por_estabelecimento_com_total
    ORDER BY
        unidade_geografica_id,
        primeiro_procedimento_periodo_data_inicio,
        estabelecimento_id_cnes,
        quantidade_usuarios DESC
)
SELECT 
    abandonos_por_estabelecimento_com_total.*,
    condicao_predominante.condicao_predominante,
    condicao_predominante.condicao_predominante_usuarios,
    estabelecimento_maior_taxa_por_municipio.estabelecimento_maior_taxa_id_cnes,
    estabelecimento_maior_taxa_por_municipio.maior_taxa,
    round(
        100 * coorte_usuarios_abandonaram::numeric 
        / nullif(coorte_usuarios_total, 0),
        1
    ) AS taxa_abandono_coorte
FROM abandonos_por_estabelecimento_com_total
LEFT JOIN condicao_predominante
USING (
    unidade_geografica_id,
    primeiro_procedimento_periodo_data_inicio,
    estabelecimento_id_cnes
)
LEFT JOIN estabelecimento_maior_taxa_por_municipio
USING (
    unidade_geografica_id,
    primeiro_procedimento_periodo_data_inicio
)
LEFT JOIN
    saude_mental._raas_ultima_competencia_disponivel
    ultima_competencia_disponivel
ON 
    abandonos_por_estabelecimento_com_total.unidade_geografica_id
    = ultima_competencia_disponivel.unidade_geografica_id
WHERE
    -- são necessários 2 meses após a competência de inatividade para confirmar
    -- que o vínculo foi interrompido
    abandonos_por_estabelecimento_com_total.data_deixa_de_ser_recente
    <= ultima_competencia_disponivel.periodo_data_inicio - '2 mon'::INTERVAL
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    _usuarios_recentes_abandono_coortes_un
ON saude_mental._usuarios_recentes_abandono_coortes (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    estabelecimento_id_cnes,
    primeiro_procedimento_periodo_data_inicio,
    data_deixa_de_ser_recente
);



DROP MATERIALIZED VIEW IF EXISTS
    saude_mental.abandono_coortes_resumo
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.abandono_coortes_resumo
AS
WITH abandono_coortes AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        primeiro_procedimento_periodo_data_inicio,
        data_deixa_de_ser_recente,
        EXTRACT(
            'year' FROM primeiro_procedimento_periodo_data_inicio
        )::text AS a_partir_do_ano,
        listas_de_codigos.nome_mes(
            (primeiro_procedimento_periodo_data_inicio)::date
        ) AS a_partir_do_mes,
        EXTRACT(
            'year' FROM data_deixa_de_ser_recente - '1 days'::interval
        )::text AS ate_ano,
        listas_de_codigos.nome_mes(
            (
                data_deixa_de_ser_recente - '1 days'::interval
            )::date
        ) AS ate_mes,
        estabelecimento_id_cnes,
        coalesce(
            estabelecimento.nome_curto,
            estabelecimento.nome,
            'Todos'
        ) AS estabelecimento,
        sexo.nome AS sexo_predominante,
        faixa_etaria_predominante,
        condicao_predominante,
        condicao_predominante_usuarios,
        coalesce(
            estabelecimento_maior_taxa.nome_curto,
            estabelecimento_maior_taxa.nome
        ) AS estabelecimento_maior_taxa,
        maior_taxa,
        coorte_usuarios_abandonaram,
        coorte_usuarios_total,
        taxa_abandono_coorte
    FROM saude_mental._usuarios_recentes_abandono_coortes t
    LEFT JOIN listas_de_codigos.sexos sexo
    ON t.sexo_predominante_id_sigtap = sexo.id_sigtap
    LEFT JOIN listas_de_codigos.estabelecimentos estabelecimento
	ON t.estabelecimento_id_cnes = estabelecimento.id_scnes
	LEFT JOIN listas_de_codigos.estabelecimentos estabelecimento_maior_taxa
   ON t.estabelecimento_maior_taxa_id_cnes = estabelecimento_maior_taxa.id_scnes
)
SELECT
    coorte_atual.unidade_geografica_id,
    coorte_atual.unidade_geografica_id_sus,
    coorte_atual.primeiro_procedimento_periodo_data_inicio AS competencia_inicio_coorte,
    date_trunc(
        'month',
        coorte_atual.data_deixa_de_ser_recente - '1 day'::interval
    )::date AS competencia_final_coorte,
    coorte_atual.a_partir_do_ano,
    coorte_atual.a_partir_do_mes,
    coorte_atual.ate_ano,
    coorte_atual.ate_mes,
    coorte_atual.estabelecimento,
    coorte_atual.sexo_predominante,
    coorte_atual.faixa_etaria_predominante,
    coorte_atual.condicao_predominante,
    coorte_atual.condicao_predominante_usuarios,
    coorte_atual.estabelecimento_maior_taxa,
    coorte_atual.maior_taxa,
    coorte_atual.coorte_usuarios_abandonaram AS usuarios_abandonaram,
    coorte_atual.taxa_abandono_coorte AS taxa_abandono,
    (
        coorte_anterior.coorte_usuarios_abandonaram
    ) AS usuarios_abandonaram_6m_anterior,
    coorte_anterior.taxa_abandono_coorte AS taxa_abandono_6m_anterior,
    (
        coorte_atual.taxa_abandono_coorte - coorte_anterior.taxa_abandono_coorte
    ) AS dif_taxa_abandono_anterior
FROM abandono_coortes coorte_atual
LEFT JOIN abandono_coortes coorte_anterior 
ON
    -- para uma dada coorte A, comparar com a coorte B que se encerrou imediatamente quando A se iniciou;
    -- por exemplo, os usuários que tiveram o primeiro procedimento (deixaram de ser recentes no final de junho)
    -- com os usuários que tiveram o primeiro procedimento em julho do mesmo ano
        coorte_atual.estabelecimento_id_cnes
        = coorte_anterior.estabelecimento_id_cnes
    AND coorte_atual.primeiro_procedimento_periodo_data_inicio::date
        = coorte_anterior.data_deixa_de_ser_recente::date
LEFT JOIN saude_mental._raas_ultima_competencia_disponivel ultima_competencia_disponivel
ON
        coorte_atual.unidade_geografica_id 
        = ultima_competencia_disponivel.unidade_geografica_id
    AND coorte_anterior.unidade_geografica_id 
        = ultima_competencia_disponivel.unidade_geografica_id
-- a última coorte deve ser aquela em que já se passaram pelo menos dois meses
-- após os usuários deixarem de ser recentes. Esse é o tempo necessário para
-- caracterizar abandonos ocorridos no 6º mês após o primeiro procedimento.
WHERE ultima_competencia_disponivel.periodo_data_inicio >= date_trunc(
	'month',
	coorte_atual.data_deixa_de_ser_recente + '1 mon'::interval
)
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    abandono_coortes_resumo_un
ON saude_mental.abandono_coortes_resumo (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    competencia_inicio_coorte,
    competencia_final_coorte,
    a_partir_do_ano,
    a_partir_do_mes,
    ate_ano,
    ate_mes,
    estabelecimento
);



DROP MATERIALIZED VIEW IF EXISTS
    saude_mental.abandono_coortes_resumo_ultimo_mes
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.abandono_coortes_resumo_ultimo_mes
AS
SELECT
    DISTINCT ON (
        unidade_geografica_id,
        unidade_geografica_id_sus,
        estabelecimento
    )
    unidade_geografica_id,
    unidade_geografica_id_sus,
    competencia_inicio_coorte,
    competencia_final_coorte,
    a_partir_do_ano,
    a_partir_do_mes,
    ate_ano,
    ate_mes,
    estabelecimento,
    sexo_predominante,
    faixa_etaria_predominante,
    condicao_predominante,
    condicao_predominante_usuarios,
    estabelecimento_maior_taxa,
    maior_taxa,
    usuarios_abandonaram,
    taxa_abandono,
    usuarios_abandonaram_6m_anterior,
    taxa_abandono_6m_anterior,
    dif_taxa_abandono_anterior
FROM saude_mental.abandono_coortes_resumo
ORDER BY
    unidade_geografica_id,
    unidade_geografica_id_sus,
    estabelecimento,
    competencia_inicio_coorte DESC
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    abandono_coortes_resumo_ultimo_mes_un
ON saude_mental.abandono_coortes_resumo_ultimo_mes (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    estabelecimento
);
