/**************************************************************************

						RELAÇÃO RAPS <> REDE HOSPITALAR


 **************************************************************************/



/* Definir CIDs relacionados à saúde mental */
-- TODO: trocar tabela com CIDs por versão do schema `listas_de_codigos`
DROP TABLE IF EXISTS saude_mental.condicoes_saude_mental CASCADE;
CREATE TABLE saude_mental.condicoes_saude_mental (
    id_cid10 varchar(4) primary key,
    classificacao text NOT NULL,
    observacao text
);
INSERT INTO saude_mental.condicoes_saude_mental (
    id_cid10, 
    classificacao, 
    observacao
)
VALUES
    -- Condutas autolesivas
    ('X6', 'Conduta autolesiva', 'Auto-intoxicação intencional'),
    ('X7', 'Conduta autolesiva', 'Lesão autoprovocada intencionalmente'),
    ('X80', 'Conduta autolesiva', 'Lesão autoprovocada intencionalmente'),
    ('X81', 'Conduta autolesiva', 'Lesão autoprovocada intencionalmente'),
    ('X82', 'Conduta autolesiva', 'Lesão autoprovocada intencionalmente'),
    ('X83', 'Conduta autolesiva', 'Lesão autoprovocada intencionalmente'),
    ('X84', 'Conduta autolesiva', 'Lesão autoprovocada intencionalmente'),
    ('Y870', 'Conduta autolesiva', 'Seqüelas de lesões autoprovocadas intencionalmente'),
    -- Abuso de substâncias
    ('F1', 'Álcool e outras drogas', 'Transtornos mentais e comportamentais devido ao uso de substâncias'),
    ('T40', 'Álcool e outras drogas', 'Intoxicação por narcóticos e psicodislépticos (alucinógenos)'),
    ('R78', 'Álcool e outras drogas', 'Presença de drogas e de outras substâncias normalmente não encontradas no sangue'),
--  ('T43', 'Transtornos', 'Intoxicação por drogas psicotrópicas não classificadas em outra parte'), ???
--  ('Y49', 'Transtornos', 'Efeitos adversos de substâncias psicotrópicas, não classificadas em outra parte'), ???
    ('Y90', 'Álcool e outras drogas', 'Evidência de Alcoolismo Determinada Por Taxas de Alcoolemia'),
    ('Y91', 'Álcool e outras drogas', 'Evidência de Alcoolismo Determinada Pelo Nível da Intoxicação'),
    -- Transtornos mentais
    ('F2', 'Transtornos', 'Esquizofrenia e transtornos esquizotípicos e delirantes'),
    ('F3', 'Transtornos', 'Transtornos do humor'),
    ('F4', 'Transtornos', 'Transtornos neuróticos, relacionados com o stress e somatoformes'),
    ('F5', 'Transtornos', 'Síndromes comportamentais relacionadas a disfunções fisiológicas e fatores físicos'),
    ('F6', 'Transtornos', 'Transtornos de personalidade e do comportamento adulto'),
    ('F7', 'Transtornos', 'Retardo mental'),
    ('F8', 'Transtornos', 'Transtornos do desenvolvimento psicológico'),
    ('F90', 'Transtornos', 'Transtornos hipercinéticos'),
    ('F91', 'Transtornos', 'Distúrbios de conduta'),
    ('F92', 'Transtornos', 'Transtornos mistos de conduta e das emoções'),
    ('F93', 'Transtornos', 'Transtornos emocionais com início na infância'),
    ('F94', 'Transtornos', 'Transtornos do funcionamento social com início na infância ou na adolescência'),
    ('F95', 'Transtornos', 'Tiques'),
    ('F98', 'Transtornos', 'Outros transtornos comportamentais e emocionais com início na infância ou adolescência'),
    ('F99', 'Transtornos', 'Transtorno mental não especificado')
;
CREATE UNIQUE INDEX IF NOT EXISTS 
    condicoes_saude_mental_un
ON saude_mental.condicoes_saude_mental (id_cid10);



/* Discretizar duração das internações */
CREATE OR REPLACE FUNCTION 
    saude_mental.classificar_duracao_internacao(
        entrada_data date,
        desfecho_data date
    )
RETURNS text
LANGUAGE plpgsql
AS $function$
    DECLARE
        duracao_internacao interval;
    BEGIN
        duracao_internacao := AGE(desfecho_data, entrada_data);
        CASE
            WHEN 
                duracao_internacao < '2 days'::INTERVAL
            THEN RETURN 'Até 1 dia';
            WHEN 
                duracao_internacao >= '2 days'::interval 
            AND duracao_internacao < '4 days'::interval 
            THEN RETURN ' 2 a 3 dias';
            WHEN 
                duracao_internacao >= '4 days'::INTERVAL
            AND duracao_internacao < '15 days'::INTERVAL
            THEN RETURN ' 4 a 14 dias';
            WHEN 
                 duracao_internacao >= '15 days'::interval 
            AND duracao_internacao < '30 days'::INTERVAL
            THEN RETURN '15 a 30 dias';
            ELSE RETURN 'Mais de 30 dias';
        END CASE;
    END;
$function$
;


/* Juntar atendimentos em CAPS (registros em RAAS e BPA-i) e  *
 * nas referências ambulatoriais em saúde mental              */
DROP MATERIALIZED VIEW IF EXISTS
    saude_mental._atendimentos_raps
CASCADE;
CREATE MATERIALIZED VIEW IF NOT EXISTS
    saude_mental._atendimentos_raps
AS
WITH
atendimentos_referencias_saude_mental AS (
    -- procedimentos realizados nas referências ambulatoriais 
    -- (reaproveitado da visão definida no arquivo 
    -- `referencias_ambulatoriais.sql`)
    SELECT
    	unidade_geografica_id,
    	unidade_geografica_id_sus,
    	periodo_id,
    	realizacao_periodo_data_inicio,
    	usuario_cns_criptografado,
    	usuario_data_nascimento,
    	usuario_sexo_id_sigtap,
    	usuario_residencia_municipio_id_sus,
        procedimento_id_sigtap,
        quantidade_apresentada
    FROM saude_mental._procedimentos_referencias_ambulatoriais
    WHERE quantidade_apresentada > 0
),
atendimentos_caps_bpa_i AS (
    -- procedimentos em CAPS registrados em BPA-i
    SELECT 
    	unidade_geografica_id,
    	unidade_geografica_id_sus,
    	periodo_id,
    	realizacao_periodo_data_inicio,
    	usuario_cns_criptografado,
    	usuario_data_nascimento,
    	usuario_sexo_id_sigtap,
        usuario_residencia_municipio_id_sus,
        procedimento_id_sigtap,
        quantidade_apresentada
    FROM dados_publicos.siasus_bpa_i_disseminacao
    WHERE 
        estabelecimento_tipo_id_sigtap = '70'  -- CAPS
),
atendimentos_caps_raas AS (
    -- procedimentos em CAPS registrados em RAAS
    SELECT 
    	unidade_geografica_id,
    	unidade_geografica_id_sus,
    	periodo_id,
    	realizacao_periodo_data_inicio,
    	usuario_cns_criptografado,
    	usuario_data_nascimento,
    	usuario_sexo_id_sigtap,
        usuario_residencia_municipio_id_sus,
        procedimento_id_sigtap,
        quantidade_apresentada
    FROM dados_publicos.siasus_raas_psicossocial_disseminacao
),
atendimentos_todos AS (
    SELECT * FROM atendimentos_referencias_saude_mental
    UNION
    SELECT * FROM atendimentos_caps_bpa_i
    UNION
    SELECT * FROM atendimentos_caps_raas
)
SELECT 
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    realizacao_periodo_data_inicio,
    usuario_cns_criptografado,
    usuario_data_nascimento,
    usuario_sexo_id_sigtap,
    usuario_residencia_municipio_id_sus
FROM atendimentos_todos
WHERE
    quantidade_apresentada > 0
-- ignorar procedimentos ambulatoriais relacionados à situação de urgência 
-- ou à gestão de outras situações de crise.
-- O objetivo do indicador é saber do atendimento continuado na atenção
-- psicossocial e/ou especializada
AND procedimento_id_sigtap NOT IN (
    '0301060010', -- DIAGNOSTICO E/OU AT DE URGENCIA EM CLINICA PEDIATRICA
    '0301060029', --  AT DE URGENCIA C/ OBS ATE 24H EM ATENCAO ESPECIALIZADA
    '0301060061', -- ATENDIMENTO DE URGENCIA EM ATENCAO ESPECIALIZADA
    '0301060088', -- DIAGNOSTICO E/OU AT DE URGENCIA EM CLINICA MEDICA
    '0301080291', -- ATENÇÃO ÀS SITUAÇÕES DE CRISE
    '0303170018'  -- DIAGNOSTICO E/OU AT DE URGENCIA EM PSIQUIATRIA
)
WITH NO DATA;
CREATE INDEX IF NOT EXISTS 
    _atendimentos_raps_sexo_x_nascimento_x_competencia_idx
ON saude_mental._atendimentos_raps (
    usuario_sexo_id_sigtap,
    usuario_data_nascimento,
    realizacao_periodo_data_inicio
);
CREATE INDEX IF NOT EXISTS
    sihsus_aih_reduzida_disseminacao_ug_x_competencia_idx
ON dados_publicos.sihsus_aih_reduzida_disseminacao (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_data_inicio
);



/* Definir quais das internações são devidas a questões de saúde mental */
DROP MATERIALIZED VIEW IF EXISTS saude_mental._aih_saude_mental;
CREATE MATERIALIZED VIEW saude_mental._aih_saude_mental AS 
SELECT
	aih.*,
	condicao_saude_mental.classificacao AS condicao_saude_mental_classificao
FROM dados_publicos.sihsus_aih_reduzida_disseminacao aih
LEFT JOIN saude_mental.condicoes_saude_mental condicao_saude_mental
ON 
   aih.condicao_principal_id_cid10 LIKE (condicao_saude_mental.id_cid10||'%')
OR aih.condicao_secundaria_id_cid10 LIKE (condicao_saude_mental.id_cid10||'%')
OR aih.condicao_secundaria_1_id_cid10 LIKE (condicao_saude_mental.id_cid10||'%')
OR aih.condicao_secundaria_2_id_cid10 LIKE (condicao_saude_mental.id_cid10||'%')
OR aih.condicao_secundaria_3_id_cid10 LIKE (condicao_saude_mental.id_cid10||'%')
OR aih.condicao_secundaria_4_id_cid10 LIKE (condicao_saude_mental.id_cid10||'%')
OR aih.condicao_secundaria_5_id_cid10 LIKE (condicao_saude_mental.id_cid10||'%')
OR aih.condicao_secundaria_6_id_cid10 LIKE (condicao_saude_mental.id_cid10||'%')
OR aih.condicao_secundaria_7_id_cid10 LIKE (condicao_saude_mental.id_cid10||'%')
OR aih.condicao_secundaria_8_id_cid10 LIKE (condicao_saude_mental.id_cid10||'%')
OR aih.condicao_secundaria_9_id_cid10 LIKE (condicao_saude_mental.id_cid10||'%')
WHERE
	-- o leito é de especialidade de saúde mental
	aih.leito_especialidade_id_sigtap = ANY (ARRAY[
	    '05',  -- Psiquiatria
	    '14',  -- Leito dia/saude mental
	    '84',  -- Acolhimento Noturno
	    '87'  -- Saude Mental (clinico)
	]::bpchar(2)[])
	-- OU o procedimento principal é diária em saúde mental
OR aih.procedimento_solicitado_id_sigtap = ANY (ARRAY[
    '0802010253',  -- DIARIA DE SAÚDE MENTAL COM PERMANENCIA DE ATÉ SETE DIAS
    '0802010261',  -- DIÁRIA DE SAUDE MENTAL COM PERMANENCIA ENTRE 08 A 15 DIAS
	'0802010270'   -- DIÁRIA DE SAUDE MENTAL COM PERMENENCIA SUPERIOR A 15 DIAS
]::bpchar(10)[])
-- OU o desfecho foi classificado como 'alta de paciente agudo em psiquiatria'
OR aih.desfecho_motivo_id_sihsus = '19'
	-- OU algum dos CIDs do diagnóstico é de saúde mental
OR condicao_saude_mental.id_cid10 IS NOT NULL
WITH NO DATA;
CREATE INDEX
    _aih_saude_mental_aih_x_competencia
ON saude_mental._aih_saude_mental (
    aih_id_sihsus,
    periodo_data_inicio
);


   
/*  internações são devidas a questões de saúde mental */
DROP MATERIALIZED VIEW IF EXISTS
    saude_mental._internacoes_relacao_raps
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental._internacoes_relacao_raps
AS 
WITH
-- Utiliza apenas a última remessa de AIH disponível para cada internação
aih_consolidado AS (
	SELECT
		DISTINCT ON (aih_id_sihsus)
		*
	FROM saude_mental._aih_saude_mental
	ORDER BY 
	   aih_id_sihsus,
	   periodo_data_inicio DESC
),
aih_consolidado_campos_selecionados AS (
    SELECT
        -- ATENÇÃO: A unidade geográfica aqui é a de *moradia* do usuário,
        -- *não* do estabelecimento que atendeu! 
        unidade_geografica.id AS unidade_geografica_id,
        unidade_geografica.id_sus AS unidade_geografica_id_sus,
        internacao.unidade_geografica_id AS estabelecimento_unidade_geografica_id,
        internacao.aih_id_sihsus,
        internacao.aih_data_inicio,
        internacao.aih_data_fim AS desfecho_data,
        internacao.condicao_saude_mental_classificao,
        internacao.desfecho_motivo_id_sihsus,
        internacao.usuario_data_nascimento,
        sexo.id_sigtap AS usuario_sexo_id_sigtap,
        internacao.usuario_residencia_cep
    FROM aih_consolidado internacao
    LEFT JOIN listas_de_codigos.unidades_geograficas unidade_geografica
    ON 
        internacao.usuario_residencia_municipio_id_sus
        = unidade_geografica.id_sus
    LEFT JOIN listas_de_codigos.sexos sexo
    ON 
       internacao.usuario_sexo_id_sigtap  -- TODO: renomear p/ _id_sihsus
       = sexo.id_sihsus  
),
internados_atendimentos_raps_antes AS (
    SELECT
        internacao.*,
        -- checa se houve pelo menos uma correspondência com atendimentos na
        -- RAPS usando os campos na clausula LEFT JOIN
        bool_or(
            atendimento_raps.usuario_cns_criptografado IS NOT NULL
        ) AS atendimento_raps_6m_antes
    FROM aih_consolidado_campos_selecionados internacao
    LEFT JOIN saude_mental._atendimentos_raps atendimento_raps
	ON
        internacao.usuario_sexo_id_sigtap
	    = atendimento_raps.usuario_sexo_id_sigtap
	AND internacao.usuario_data_nascimento
	    = atendimento_raps.usuario_data_nascimento
	AND internacao.unidade_geografica_id_sus
	    = atendimento_raps.usuario_residencia_municipio_id_sus
	-- procedimento realizado na RAPS até meses antes do início da AIH
	AND atendimento_raps.realizacao_periodo_data_inicio
	    >= (
	       date_trunc('month', internacao.aih_data_inicio) - '6 mon'::INTERVAL
	   )::date
	AND atendimento_raps.realizacao_periodo_data_inicio
	   < date_trunc('month', internacao.aih_data_inicio)
    LEFT JOIN
        saude_mental._raas_primeira_competencia_disponivel
        raas_primeira_competencia
    ON 
        internacao.unidade_geografica_id
        = raas_primeira_competencia.unidade_geografica_id
    LEFT JOIN 
        saude_mental._bpa_i_primeira_competencia_disponivel
        bpa_i_primeira_competencia
    ON
        internacao.unidade_geografica_id
        = bpa_i_primeira_competencia.unidade_geografica_id
    -- Ignorar (i.e., tratar como nulos) os registros de internação iniciados
    -- até o sexto mês do início dos registros de RAAS e BPA-i, já que esse é o
    -- periodo levado em consideração para o registro dos atendimentos antes
    -- da internação
    WHERE 
        date_trunc('month', internacao.aih_data_inicio) >= (
            greatest(
                raas_primeira_competencia.periodo_data_inicio,
                bpa_i_primeira_competencia.periodo_data_inicio
            ) + '6 mon'::INTERVAL
        )::date
	GROUP BY
	    internacao.unidade_geografica_id,
        internacao.unidade_geografica_id_sus,
        internacao.estabelecimento_unidade_geografica_id,
        internacao.aih_id_sihsus,
        internacao.aih_data_inicio,
        internacao.desfecho_data,
        internacao.condicao_saude_mental_classificao,
        internacao.desfecho_motivo_id_sihsus,
        internacao.usuario_data_nascimento,
        internacao.usuario_sexo_id_sigtap,
        internacao.usuario_residencia_cep
),
internados_atendimentos_raps_apos AS (
    SELECT
        internacao.*,
        -- checa se houve pelo menos uma correspondência com atendimentos na
        -- RAPS usando as condições na clausula LEFT JOIN
        bool_or(
            atendimento_raps.usuario_cns_criptografado IS NOT NULL
        ) AS atendimento_raps_1m_apos
    FROM aih_consolidado_campos_selecionados internacao
    LEFT JOIN saude_mental._atendimentos_raps atendimento_raps
    ON
        internacao.usuario_sexo_id_sigtap
        = atendimento_raps.usuario_sexo_id_sigtap
    AND internacao.usuario_data_nascimento
        = atendimento_raps.usuario_data_nascimento
    AND internacao.unidade_geografica_id_sus
        = atendimento_raps.usuario_residencia_municipio_id_sus
	AND
	   atendimento_raps.realizacao_periodo_data_inicio
        >= date_trunc('month', internacao.desfecho_data)
       -- procedimento realizado na RAPS até a competência seguinte à alta da AIH
    AND atendimento_raps.realizacao_periodo_data_inicio
        <= (
            date_trunc('month', internacao.desfecho_data) + '1 mon'::interval
        )::date
    -- Só faz sentido avaliar a ida à RAPS após o desfecho da internação se o
    -- desfecho for alta
    WHERE internacao.desfecho_motivo_id_sihsus IN (
        '11',  -- Alta curado
        '12',  -- Alta melhorado
        '14',  -- Alta a pedido
        '15',  -- Alta com previsão de retorno p/acomp do paciente
        '16',  -- Alta por evasão
        '18',  -- Alta por outros motivos
        '19',  -- Alta de paciente agudo em psiquiatria
        '29',  -- Transferência para internação domiciliar
        '32',  -- Transferência para internação domiciliar
        '51'   -- Encerramento administrativo
    )
    GROUP BY
        internacao.unidade_geografica_id,
        internacao.unidade_geografica_id_sus,
        internacao.estabelecimento_unidade_geografica_id,
        internacao.aih_id_sihsus,
        internacao.aih_data_inicio,
        internacao.desfecho_data,
        internacao.condicao_saude_mental_classificao,
        internacao.desfecho_motivo_id_sihsus,
        internacao.usuario_data_nascimento,
        internacao.usuario_sexo_id_sigtap,
        internacao.usuario_residencia_cep
)
SELECT
    internacao.*,
    internados_atendimentos_raps_antes.atendimento_raps_6m_antes,
    internados_atendimentos_raps_apos.atendimento_raps_1m_apos
FROM aih_consolidado_campos_selecionados internacao
LEFT JOIN internados_atendimentos_raps_antes
USING (
    aih_id_sihsus
)
LEFT JOIN internados_atendimentos_raps_apos
USING (
    aih_id_sihsus
)
WITH NO DATA;
CREATE UNIQUE INDEX _internacoes_relacao_raps_un
ON saude_mental._internacoes_relacao_raps (
    aih_id_sihsus
);
CREATE INDEX IF NOT EXISTS
    _internacoes_relacao_raps_ug_idx
ON saude_mental._internacoes_relacao_raps (
    unidade_geografica_id,
    unidade_geografica_id_sus
);


/* Resumo de altas e da relação dos egressos com a RAPS nos últimos 12 meses */
DROP MATERIALIZED VIEW IF EXISTS
    saude_mental.relacao_raps_reue_altas_resumo_12m
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.relacao_raps_reue_altas_resumo_12m
AS
WITH altas AS (
	SELECT
		internacao.unidade_geografica_id,
		internacao.unidade_geografica_id_sus,
		EXTRACT(
            YEAR FROM min(internacao.desfecho_data)
        )::text AS a_partir_de_ano,
		listas_de_codigos.nome_mes(
		  min(internacao.desfecho_data)
		) AS a_partir_de_mes,
		EXTRACT(
		  YEAR FROM max(internacao.desfecho_data)
		)::text AS ate_ano,
		listas_de_codigos.nome_mes(max(internacao.desfecho_data)) AS ate_mes,
		count(DISTINCT internacao.aih_id_sihsus) FILTER (
		  WHERE 
		      NOT atendimento_raps_6m_antes 
		  AND NOT atendimento_raps_1m_apos
		) AS altas_atendimento_raps_antes_nao_apos_nao,
		count(DISTINCT internacao.aih_id_sihsus) FILTER (
            WHERE 
                    atendimento_raps_6m_antes
            AND NOT atendimento_raps_1m_apos
        ) AS altas_atendimento_raps_antes_sim_apos_nao,
		count(DISTINCT internacao.aih_id_sihsus) FILTER (
            WHERE 
                atendimento_raps_6m_antes
            AND atendimento_raps_1m_apos
        ) AS altas_atendimento_raps_antes_sim_apos_sim,
		count(DISTINCT internacao.aih_id_sihsus) FILTER (
            WHERE
                NOT atendimento_raps_6m_antes
            AND atendimento_raps_1m_apos
        ) AS altas_atendimento_raps_antes_nao_apos_sim
	FROM saude_mental._internacoes_relacao_raps internacao
	LEFT JOIN
	   saude_mental._aih_rd_ultima_competencia_disponivel
	   ultima_competencia
	ON 
	   internacao.estabelecimento_unidade_geografica_id
	   = ultima_competencia.unidade_geografica_id
    WHERE
        internacao.desfecho_motivo_id_sihsus = ANY(ARRAY[
            '11',  -- Alta curado
            '12',  -- Alta melhorado
            '14',  -- Alta a pedido
            '15',  -- Alta com previsão de retorno p/acomp do paciente
            '16',  -- Alta por evasão
            '18',  -- Alta por outros motivos
            '19',  -- Alta de paciente agudo em psiquiatria
            '29',  -- Transferência para internação domiciliar
            '32',  -- Transferência para internação domiciliar
            '51'   -- Encerramento administrativo
        ]::bpchar(2)[])
        -- Datas dos últimos 12 meses
	AND date_trunc('month', internacao.desfecho_data) 
	    >= date_trunc(
	       'month',
	       ultima_competencia.periodo_data_inicio - '12 mon'::interval
	   )
		-- Eliminar altas da última competência disponível; é necessário que
		-- haja um mês após para verificar se houve acompanhamento pela RAPS
		-- após a saída
	AND date_trunc(
        'month',
        internacao.desfecho_data
    ) < ultima_competencia.periodo_data_inicio
    AND atendimento_raps_1m_apos IS NOT NULL
	GROUP BY
		internacao.unidade_geografica_id,
		internacao.unidade_geografica_id_sus
),
altas_subtotais AS (
	SELECT
		*,
		(
            altas_atendimento_raps_antes_sim_apos_nao 
            + altas_atendimento_raps_antes_sim_apos_sim
        ) AS altas_atendimento_raps_6m_antes,
		(
            altas_atendimento_raps_antes_nao_apos_sim 
            + altas_atendimento_raps_antes_sim_apos_sim
        ) AS altas_atendimento_raps_1m_apos,
		(
			altas_atendimento_raps_antes_nao_apos_nao
			+ altas_atendimento_raps_antes_sim_apos_nao
			+ altas_atendimento_raps_antes_sim_apos_sim
			+ altas_atendimento_raps_antes_nao_apos_sim
		) AS altas_total
	FROM altas
)
SELECT
	*,
	round(
	   100 * altas_atendimento_raps_6m_antes::numeric / nullif(altas_total, 0),
	   1
	) AS perc_altas_atendimento_raps_6m_antes,
	round(
	   100 * altas_atendimento_raps_1m_apos::numeric / nullif(altas_total, 0),
	   1
	) AS perc_altas_atendimento_raps_1m_apos
FROM altas_subtotais
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    relacao_raps_reue_altas_resumo_12m_un
ON saude_mental.relacao_raps_reue_altas_resumo_12m (
    unidade_geografica_id,
    unidade_geografica_id_sus
);


-- Criar versão "verticalizada" do resumo de relação das altas com a RAPS, i.e.,
-- onde as categorias de atendido ou não pela RAPS até a competência seguinte à
-- alta seja disposto em diferentes linhas, em vez de colunas
DROP MATERIALIZED VIEW IF EXISTS
    saude_mental.relacao_raps_reue_altas_resumo_12m_vertical
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.relacao_raps_reue_altas_resumo_12m_vertical
AS
SELECT
    unidade_geografica_id,
    unidade_geografica_id_sus,
    a_partir_de_ano,
    a_partir_de_mes,
    ate_ano,
    ate_mes,
    'Sim' AS atendimento_raps_1m_apos,
    (perc_altas_atendimento_raps_1m_apos) / 100 AS prop_altas
FROM saude_mental.relacao_raps_reue_altas_resumo_12m
UNION
SELECT
    unidade_geografica_id,
    unidade_geografica_id_sus,
    a_partir_de_ano,
    a_partir_de_mes,
    ate_ano,
    ate_mes,
    'Não' AS atendimento_raps_1m_apos,
    (100 - perc_altas_atendimento_raps_1m_apos) / 100 AS prop_altas
FROM saude_mental.relacao_raps_reue_altas_resumo_12m
WITH NO DATA;
CREATE INDEX IF NOT EXISTS
    relacao_raps_reue_altas_resumo_12m_vertical_ug_ix
ON saude_mental.relacao_raps_reue_altas_resumo_12m_vertical (
    unidade_geografica_id,
    unidade_geografica_id_sus
);


/* Resumo de entradas e da relação dos usuários com a RAPS com a RAPS nos últimos 12 meses */
DROP MATERIALIZED VIEW IF EXISTS
    saude_mental.relacao_raps_reue_internacoes_resumo_12m
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.relacao_raps_reue_internacoes_resumo_12m
AS
WITH
_relacao_raps_reue_internacoes_resumo_12m AS (
    SELECT 
    	internacao.unidade_geografica_id,
    	internacao.unidade_geografica_id_sus,
    	EXTRACT(
    	   YEAR FROM min(internacao.aih_data_inicio)
    	)::text AS a_partir_de_ano,
    	listas_de_codigos.nome_mes(
    	   min(internacao.aih_data_inicio)
    	) AS a_partir_de_mes,
    	EXTRACT(
    	   YEAR FROM max(internacao.aih_data_inicio)
    	)::text AS ate_ano,
    	listas_de_codigos.nome_mes(max(internacao.aih_data_inicio)) AS ate_mes,
    	count(DISTINCT internacao.aih_id_sihsus) FILTER (
    	   WHERE atendimento_raps_6m_antes
    	) AS internacoes_atendimento_raps_antes,
    	count(DISTINCT internacao.aih_id_sihsus) FILTER (
    	   WHERE 
    	       internacao.condicao_saude_mental_classificao 
    	       = 'Álcool e outras drogas'
    	) AS internacoes_alcool_drogas,
    	count(DISTINCT internacao.aih_id_sihsus) FILTER (
    	   WHERE
    	       internacao.condicao_saude_mental_classificao 
    	       != 'Álcool e outras drogas'
    	   ) AS internacoes_transtornos,
    	count(DISTINCT internacao.aih_id_sihsus) AS internacoes_total
    FROM saude_mental._internacoes_relacao_raps internacao
    LEFT JOIN saude_mental._aih_rd_ultima_competencia_disponivel ultima_competencia
    ON
        internacao.estabelecimento_unidade_geografica_id
        = ultima_competencia.unidade_geografica_id
    -- Datas dos últimos 12 meses
    WHERE
        date_trunc('month', internacao.aih_data_inicio)
    	>= date_trunc(
    	   'month',
    	   ultima_competencia.periodo_data_inicio - '12 mon'::INTERVAL
    	)
	-- Eliminar altas da última competência disponível; é necessário que
	-- haja um mês após para verificar se houve acompanhamento pela RAPS
	-- após a saída
    AND date_trunc(
    	    'month', 
    	    internacao.aih_data_inicio
    	) < ultima_competencia.periodo_data_inicio
    GROUP BY
    	internacao.unidade_geografica_id,
    	internacao.unidade_geografica_id_sus
)
SELECT
    *,
    round(
        100 * internacoes_atendimento_raps_antes::numeric
        / nullif(internacoes_total, 0),
        1
    ) AS perc_internacoes_atendimento_raps_antes
FROM _relacao_raps_reue_internacoes_resumo_12m
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    relacao_raps_reue_internacoes_resumo_12m_un
ON saude_mental.relacao_raps_reue_internacoes_resumo_12m (
    unidade_geografica_id,
    unidade_geografica_id_sus
);


-- Criar versão "verticalizada" do resumo de relação das altas com a RAPS, i.e.,
-- onde as categorias de atendido ou não pela RAPS até a competência seguinte à
-- alta seja disposto em diferentes linhas, em vez de colunas
DROP MATERIALIZED VIEW IF EXISTS
    saude_mental.relacao_raps_reue_internacoes_resumo_12m_vertical
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.relacao_raps_reue_internacoes_resumo_12m_vertical
AS
SELECT
    unidade_geografica_id,
    unidade_geografica_id_sus,
    a_partir_de_ano,
    a_partir_de_mes,
    ate_ano,
    ate_mes,
    'Sim' AS atendimento_raps_6m_antes,
    (perc_internacoes_atendimento_raps_antes) / 100 AS prop_internacoes
FROM saude_mental.relacao_raps_reue_internacoes_resumo_12m
UNION
SELECT
    unidade_geografica_id,
    unidade_geografica_id_sus,
    a_partir_de_ano,
    a_partir_de_mes,
    ate_ano,
    ate_mes,
    'Não' AS atendimento_raps_6m_antes,
    (100 - perc_internacoes_atendimento_raps_antes) / 100 AS prop_internacoes
FROM saude_mental.relacao_raps_reue_internacoes_resumo_12m
WITH NO DATA;
CREATE INDEX IF NOT EXISTS
    relacao_raps_reue_internacoes_resumo_12m_vertical_ug_ix
ON saude_mental.relacao_raps_reue_internacoes_resumo_12m_vertical (
    unidade_geografica_id,
    unidade_geografica_id_sus
);


/* Resumo do n. de pessoas que passaram por acolhimentos noturnos em CAPS nos */ 
/* últimos 12 meses                                                           */
DROP MATERIALIZED VIEW IF EXISTS saude_mental.acolhimentos_noturnos_12m CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.acolhimentos_noturnos_12m
AS
SELECT 
	raas.unidade_geografica_id,
	raas.unidade_geografica_id_sus,
	EXTRACT(
	   YEAR FROM min(raas.realizacao_periodo_data_inicio)
	)::text AS a_partir_de_ano,
	listas_de_codigos.nome_mes(
	   min(raas.realizacao_periodo_data_inicio)
	) AS a_partir_de_mes,
	EXTRACT(
	   YEAR FROM max(raas.realizacao_periodo_data_inicio)
	)::text AS ate_ano,
	listas_de_codigos.nome_mes(
	   max(raas.realizacao_periodo_data_inicio)
	) AS ate_mes,
	count(DISTINCT raas.usuario_cns_criptografado) AS acolhimentos_noturnos
FROM dados_publicos.siasus_raas_psicossocial_disseminacao raas
LEFT JOIN
    saude_mental._aih_rd_ultima_competencia_disponivel
    internacoes_ultima_competencia
ON
    raas.unidade_geografica_id
    = internacoes_ultima_competencia.unidade_geografica_id
-- Datas compatíveis com os últimos 12 meses de *internacões hospitalares*
WHERE
    date_trunc('month', raas.realizacao_periodo_data_inicio)
    >= date_trunc(
		'month',
		internacoes_ultima_competencia.periodo_data_inicio - '12 mon'::interval
	)
-- Eliminar altas da última competência disponível; é necessário que
-- haja um mês após para verificar se houve acompanhamento pela RAPS
-- após a saída
AND date_trunc(
   'month',
   raas.realizacao_periodo_data_inicio
) < internacoes_ultima_competencia.periodo_data_inicio
AND raas.quantidade_apresentada > 0
AND (
    -- acolhimento noturno em CAPS transtornos
        raas.procedimento_id_sigtap = '0301080020'
    -- acolhimento noturno em CAPS AD
    OR  raas.procedimento_id_sigtap = '0301080186'
)
GROUP BY
	raas.unidade_geografica_id,
	raas.unidade_geografica_id_sus
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    acolhimentos_noturnos_12m_un
ON saude_mental.acolhimentos_noturnos_12m (
    unidade_geografica_id,
    unidade_geografica_id_sus
);


/* Internações geolocalizadas */
DROP MATERIALIZED VIEW IF EXISTS
    saude_mental.internacoes_geolocalizadas
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.internacoes_geolocalizadas
AS
WITH internacoes AS (
	SELECT
		internacao.unidade_geografica_id,
		internacao.unidade_geografica_id_sus,
		internacao.aih_data_inicio,
		internacao.desfecho_data,
		internacao.usuario_data_nascimento,
		internacao.usuario_sexo_id_sigtap,
		internacao.usuario_residencia_cep,
		saude_mental.classificar_duracao_internacao(
			internacao.aih_data_inicio,
			internacao.desfecho_data
		) AS internacao_duracao_faixa,
		desfecho_grupo.desfecho_grupo_descricao,
		internacao.atendimento_raps_6m_antes,
		internacao.atendimento_raps_1m_apos,
		internacao.condicao_saude_mental_classificao
	FROM saude_mental._internacoes_relacao_raps internacao
	LEFT JOIN saude_mental.internacao_desfechos_grupos desfecho_grupo
		ON internacao.desfecho_motivo_id_sihsus = desfecho_grupo.desfecho_id_sihsus
),
internacoes_provavel_mesmo_usuario AS (
	SELECT 
		internacao_posterior.*,
		coalesce(
			age(
				internacao_posterior.aih_data_inicio,
				internacao_anterior.desfecho_data
			) < '6 mon'::interval,
			FALSE
		) AS menos_6m_ultima_internacao
	FROM internacoes internacao_anterior
	RIGHT JOIN internacoes internacao_posterior
	ON
		internacao_anterior.usuario_data_nascimento
		= internacao_posterior.usuario_data_nascimento
	AND internacao_anterior.usuario_sexo_id_sigtap
	    = internacao_posterior.usuario_sexo_id_sigtap
	AND internacao_anterior.usuario_residencia_cep 
	    = internacao_posterior.usuario_residencia_cep
	AND internacao_anterior.desfecho_data 
	    < internacao_posterior.aih_data_inicio
),
ceps_por_bairro AS (
    SELECT 
        unidade_geografica_id,
        municipio_id_sus,
        bairro_nome,
        array_agg(id_cep) AS ceps,
        min(latitude) + (max(latitude) - min(latitude))/2 AS latitude,
        min(longitude) + (max(longitude) - min(longitude))/2 AS longitude
    FROM listas_de_codigos.ceps
    INNER JOIN listas_de_codigos.ceps_por_municipio
    USING (id_cep)
    GROUP BY unidade_geografica_id, municipio_id_sus, bairro_nome
),
internacoes_com_cep AS (
    SELECT
    	ceps_por_municipio.unidade_geografica_id AS unidade_geografica_id,
    	ceps_por_municipio.municipio_id_sus AS unidade_geografica_id_sus,
    	internacao.internacao_duracao_faixa,
    	internacao.desfecho_grupo_descricao,
        saude_mental.classificar_binarios(
            internacao.atendimento_raps_6m_antes
        ) AS atendimento_raps_6m_antes,
        saude_mental.classificar_binarios(
            internacao.atendimento_raps_1m_apos
        ) AS atendimento_raps_1m_apos,
        saude_mental.classificar_binarios(
            internacao.menos_6m_ultima_internacao
        ) AS menos_6m_ultima_internacao,
    	cep.latitude AS usuario_residencia_cep_latitude,
    	cep.longitude AS usuario_residencia_cep_longitude,
    	(
    	   cep.latitude::text || ',' || cep.longitude::TEXT
    	) AS usuario_residencia_cep_latlong,
        coalesce(
            cep.bairro_nome,
            'Sem informação'
        ) AS usuario_residencia_bairro,
        internacao.aih_data_inicio AS internacao_data_inicio,
        internacao.condicao_saude_mental_classificao,
        internacao.usuario_residencia_cep
    FROM internacoes_provavel_mesmo_usuario internacao
    INNER JOIN listas_de_codigos.ceps_por_municipio
    ON internacao.usuario_residencia_cep = ceps_por_municipio.id_cep
    LEFT JOIN listas_de_codigos.ceps cep
    ON ceps_por_municipio.id_cep = cep.id_cep
)
SELECT
    internacao.unidade_geografica_id,
    internacao.unidade_geografica_id_sus,
    internacao.internacao_duracao_faixa,
    internacao.desfecho_grupo_descricao,
    internacao.atendimento_raps_6m_antes,
    internacao.atendimento_raps_1m_apos,
    internacao.menos_6m_ultima_internacao,
    internacao.usuario_residencia_cep_latitude,
    internacao.usuario_residencia_cep_longitude,
    internacao.usuario_residencia_cep_latlong,
    internacao.usuario_residencia_bairro,
    internacao.internacao_data_inicio,
    internacao.condicao_saude_mental_classificao,
    ceps_por_bairro.latitude AS usuario_residencia_bairro_latitude,
    ceps_por_bairro.longitude AS usuario_residencia_bairro_longitude,
    (
        ceps_por_bairro.latitude::text || ',' || ceps_por_bairro.longitude::text
    ) AS usuario_residencia_bairro_latlong
FROM internacoes_com_cep internacao
LEFT JOIN ceps_por_bairro
ON  
    internacao.unidade_geografica_id = ceps_por_bairro.unidade_geografica_id
AND internacao.unidade_geografica_id_sus = ceps_por_bairro.municipio_id_sus
AND internacao.usuario_residencia_bairro = ceps_por_bairro.bairro_nome
AND internacao.usuario_residencia_cep = ANY(ceps_por_bairro.ceps)
WITH NO DATA;
CREATE INDEX IF NOT EXISTS
    internacoes_geolocalizadas_cruzamentos_idx
ON saude_mental.internacoes_geolocalizadas(
    unidade_geografica_id,
    unidade_geografica_id_sus,
    internacao_duracao_faixa,
    desfecho_grupo_descricao,
    atendimento_raps_6m_antes,
    atendimento_raps_1m_apos,
    menos_6m_ultima_internacao,
    internacao_data_inicio,
    condicao_saude_mental_classificao
);

