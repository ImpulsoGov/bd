/**************************************************************************

          DEFINIÇÃO DAS PRIMEIRAS E ÚLTIMAS COMPETÊNCIAS EM QUE HÁ 
            DADOS PARA OS DIFERENTES INSTRUMENTOS DE REGISTRO

 **************************************************************************/


/* Para cada unidade geográfica, definir a última competência com  *
 * dados de procedimentos ambulatoriais disponíveis                */
CREATE INDEX IF NOT EXISTS
    siasus_procedimentos_ambulatoriais_ug_x_competencia
ON dados_publicos.siasus_procedimentos_ambulatoriais (
    unidade_geografica_id,
    realizacao_periodo_data_inicio DESC
);

DROP MATERIALIZED VIEW IF EXISTS 
    saude_mental._procedimentos_ultima_competencia_disponivel
CASCADE;
CREATE MATERIALIZED VIEW 
    saude_mental._procedimentos_ultima_competencia_disponivel
AS
SELECT 
    DISTINCT ON (
        unidade_geografica_id
    )
    unidade_geografica_id,
    periodo_id,
    realizacao_periodo_data_inicio AS periodo_data_inicio
FROM dados_publicos.siasus_procedimentos_ambulatoriais
ORDER BY
    unidade_geografica_id,
    realizacao_periodo_data_inicio DESC
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS 
    _procedimentos_ultima_competencia_disponivel_un
ON saude_mental._procedimentos_ultima_competencia_disponivel (
    unidade_geografica_id
);


CREATE INDEX IF NOT EXISTS
    raas_psicossocial_ug_x_competencia_idx
ON dados_publicos.siasus_raas_psicossocial_disseminacao (
    unidade_geografica_id,
    realizacao_periodo_data_inicio DESC
)
;

-- Primeiro mês com RAAS disponíveis para cada município
DROP MATERIALIZED VIEW IF EXISTS
    saude_mental._raas_primeira_competencia_disponivel
CASCADE;
CREATE MATERIALIZED VIEW 
    saude_mental._raas_primeira_competencia_disponivel
AS
SELECT 
    DISTINCT ON (
        unidade_geografica_id
    )
    unidade_geografica_id,
    periodo_id,
    processamento_periodo_data_inicio AS periodo_data_inicio
FROM
    dados_publicos.siasus_raas_psicossocial_disseminacao
ORDER BY
    unidade_geografica_id,
    processamento_periodo_data_inicio ASC
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS
    _raas_primeira_competencia_disponivel_un
ON
saude_mental._raas_primeira_competencia_disponivel (unidade_geografica_id);

-- Último mês com RAAS disponíveis para cada município
DROP MATERIALIZED VIEW IF EXISTS
    saude_mental._raas_ultima_competencia_disponivel
CASCADE;
CREATE MATERIALIZED VIEW 
    saude_mental._raas_ultima_competencia_disponivel
AS
SELECT 
    DISTINCT ON (
        unidade_geografica_id
    )
    unidade_geografica_id,
    periodo_id,
    processamento_periodo_data_inicio AS periodo_data_inicio
FROM
    dados_publicos.siasus_raas_psicossocial_disseminacao
ORDER BY
    unidade_geografica_id,
    processamento_periodo_data_inicio DESC
WITH NO DATA;
CREATE UNIQUE INDEX
    _raas_ultima_competencia_disponivel_un
ON
saude_mental._raas_ultima_competencia_disponivel (unidade_geografica_id);



CREATE INDEX IF NOT EXISTS
    _bpa_i_caps_ug_x_competencia_idx
ON dados_publicos.siasus_bpa_i_disseminacao (
    unidade_geografica_id,
    realizacao_periodo_data_inicio DESC
)
WHERE estabelecimento_tipo_id_sigtap = '70' -- CAPS
;

DROP MATERIALIZED VIEW IF EXISTS
    saude_mental._bpa_i_primeira_competencia_disponivel
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental._bpa_i_primeira_competencia_disponivel
AS
SELECT
    DISTINCT ON (
        unidade_geografica_id
    )
    unidade_geografica_id,
    periodo_id,
    processamento_periodo_data_inicio AS periodo_data_inicio
FROM dados_publicos.siasus_bpa_i_disseminacao
ORDER BY 
    unidade_geografica_id,
    processamento_periodo_data_inicio ASC
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    _bpa_i_primeira_competencia_disponivel_un
ON saude_mental._bpa_i_primeira_competencia_disponivel (
    unidade_geografica_id
);

DROP MATERIALIZED VIEW IF EXISTS
    saude_mental._bpa_i_caps_ultima_competencia_disponivel
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental._bpa_i_caps_ultima_competencia_disponivel
AS
SELECT
    DISTINCT ON (
        unidade_geografica_id
    )
    unidade_geografica_id,
    periodo_id,
    processamento_periodo_data_inicio AS periodo_data_inicio
FROM dados_publicos.siasus_bpa_i_disseminacao
WHERE estabelecimento_tipo_id_sigtap = '70' -- CAPS
ORDER BY 
    unidade_geografica_id,
    processamento_periodo_data_inicio DESC
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    _bpa_i_caps_ultima_competencia_disponivel_un
ON saude_mental._bpa_i_caps_ultima_competencia_disponivel (
    unidade_geografica_id
);


/* Último período com dados disponíveis de AIH reduzidas */
DROP MATERIALIZED VIEW IF EXISTS 
    saude_mental._aih_rd_ultima_competencia_disponivel
CASCADE;
CREATE MATERIALIZED VIEW 
    saude_mental._aih_rd_ultima_competencia_disponivel
AS
SELECT
    DISTINCT ON (
        unidade_geografica_id,
        unidade_geografica_id_sus
    )
    unidade_geografica_id,
    periodo_id,
    periodo_data_inicio
FROM dados_publicos.sihsus_aih_reduzida_disseminacao
ORDER BY 
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_data_inicio DESC
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    _aih_rd_ultima_competencia_disponivel_un
ON saude_mental._aih_rd_ultima_competencia_disponivel (
    unidade_geografica_id
);

REFRESH MATERIALIZED VIEW saude_mental._bpa_i_primeira_competencia_disponivel;



