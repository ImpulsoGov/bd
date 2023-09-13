-- impulso_previne_dados_nominais.lista_nominal_gestantes_unificada source

CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.lista_nominal_gestantes_unificada
TABLESPACE pg_default
AS  WITH base_atendimentos_pre_natal AS (
    WITH base AS (
        SELECT 
            DISTINCT
                b.municipio_id_sus,
                b.id_registro,
                b.data_registro AS data_atendimento,
                b.chave_gestante,
                b.profissional_nome_atendimento,
                -- Se a data de DUM é inválida ('3000-12-31'), procuramos o registro de idade_gestacional (casos de ficha CDS)
                CASE
                    WHEN COALESCE(b.data_dum,'3000-12-31'::date) <> '3000-12-31'::date
                        THEN b.data_dum
                    WHEN b.idade_gestacional_atendimento IS NOT NULL 
                        THEN (b.data_registro - '7 days'::interval * b.idade_gestacional_atendimento::double precision)::date
                    ELSE NULL::date
                END AS data_dum_atendimento,
                CASE
                    WHEN COALESCE(b.data_dum,'3000-12-31'::date) <> '3000-12-31'::date
                        THEN (b.data_dum + '294 days'::INTERVAL)::date
                    WHEN b.idade_gestacional_atendimento IS NOT NULL 
                        THEN (b.data_registro - '7 days'::interval * b.idade_gestacional_atendimento::double precision + '294 days'::INTERVAL)::date
                    ELSE NULL::date
                END AS data_dpp_atendimento,
                CASE
                    WHEN COALESCE(b.data_dum,'3000-12-31'::date) <> '3000-12-31'::date
                        THEN (CURRENT_DATE - b.data_dum) / 7
                    WHEN b.idade_gestacional_atendimento IS NOT NULL 
                        THEN (CURRENT_DATE - (b.data_registro - '7 days'::interval * b.idade_gestacional_atendimento::double precision)::date) / 7
                    ELSE NULL::integer
                END AS gestante_idade_gestacional,
                CASE 
                    WHEN COALESCE(b.data_dum,'3000-12-31'::date) <> '3000-12-31'::date
                        THEN (b.data_registro - b.data_dum) / 7
                    WHEN b.idade_gestacional_atendimento IS NOT NULL
                        THEN b.idade_gestacional_atendimento
                    ELSE NULL::integer
                END AS gestante_idade_gestacional_atendimento
            FROM impulso_previne_dados_nominais.eventos_pre_natal b
            WHERE b.tipo_registro = 'consulta_pre_natal'
            )       
        SELECT 
            b.*,
            -- Pelas regras do SISAB a data de DUM considerada na gestação é a primeira data com registro válido 
            (array_agg(b.data_atendimento) FILTER (WHERE b.data_dum_atendimento IS NOT NULL) OVER (PARTITION BY b.chave_gestante ORDER BY b.id_registro ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS primeira_data_consulta_pre_natal_com_DUM,
            (array_agg(b.data_dum_atendimento) FILTER (WHERE b.data_dum_atendimento IS NOT NULL) OVER (PARTITION BY b.chave_gestante ORDER BY b.id_registro ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS primeira_data_dum_valida
        FROM base b
        --select * from base_atendimentos_pre_natal
)
, validacao_dum AS (
-- Análise da data de DUM que é a base para definição do fim e início das gestacoes
    SELECT 
        apn.municipio_id_sus,
        apn.chave_gestante,
        COUNT(DISTINCT apn.id_registro) AS consultas_pre_natal,
        COUNT(DISTINCT CASE WHEN apn.data_dum_atendimento IS NOT NULL THEN apn.data_dum_atendimento END) AS cont_data_dum_validas,
        COUNT(DISTINCT CASE WHEN apn.data_dum_atendimento IS NULL THEN apn.id_registro END) AS atend_dum_invalida,
        MAX(apn.primeira_data_dum_valida) AS primeira_data_dum_valida,
        MAX(CASE WHEN apn.data_dum_atendimento IS NOT NULL THEN data_dum_atendimento END) AS maior_data_dum,
        MIN(CASE WHEN apn.data_dum_atendimento IS NOT NULL THEN data_dum_atendimento END) AS menor_data_dum,
        MAX(CASE WHEN apn.data_dum_atendimento IS NOT NULL THEN data_dum_atendimento END)- MIN(CASE WHEN apn.data_dum_atendimento IS NOT NULL THEN apn.data_dum_atendimento END) AS diff_maior_menor_data_dum, 
        MAX(apn.primeira_data_consulta_pre_natal_com_DUM) AS primeira_data_consulta_pre_natal_com_DUM,
        MAX(apn.data_atendimento) AS maior_data_consulta_pre_natal,
        MIN(apn.data_atendimento) AS menor_data_consulta_pre_natal,
        MAX(apn.data_atendimento)- MIN(apn.data_atendimento) AS diff_maior_menor_data_consulta_pre_natal,
        MAX(apn.primeira_data_dum_valida) + '294 days'::INTERVAL AS primeira_data_dpp,
        MAX(apn.data_dpp_atendimento)::date AS maior_data_dpp,
        MIN(apn.data_dpp_atendimento)::date AS menor_data_dpp,
        MAX(apn.data_dpp_atendimento)::date - MIN(apn.data_dpp_atendimento)::date AS diff_maior_menor_data_dpp
    FROM base_atendimentos_pre_natal apn
    GROUP BY 1, 2
) 
, validacao_registros_parto AS (
    SELECT
        b.municipio_id_sus,
        b.chave_gestante,
        COUNT(DISTINCT b.id_registro) AS cont_partos,
        MAX(b.data_registro) AS maior_data_registro_parto,
        MIN(b.data_registro) AS menor_data_registro_parto,
        MAX(b.data_registro) - MIN(b.data_registro) AS diff_dias_primeio_ultimo_parto       
    FROM impulso_previne_dados_nominais.eventos_pre_natal b
    WHERE b.tipo_registro = 'registro_de_parto'
    GROUP BY 1, 2
        )   
, validacao_registros_aborto AS (
    SELECT
        b.municipio_id_sus,
        b.chave_gestante,
        COUNT(DISTINCT b.id_registro) AS cont_abortos,
        MAX(b.data_registro) AS maior_data_registro_aborto,
        MIN(b.data_registro) AS menor_data_registro_aborto,
        MAX(b.data_registro) - MIN(b.data_registro) AS diff_dias_primeio_ultimo_aborto
    FROM impulso_previne_dados_nominais.eventos_pre_natal b
    WHERE b.tipo_registro = 'registro_de_aborto'
    GROUP BY 1, 2
)
, analise_gestante AS (
-- Criação de variáveis para entendimento do histórico de gestações por gestante e possíveis erros/falhas de registro
    SELECT
        vd.municipio_id_sus,
        vd.chave_gestante,
        vd.cont_data_dum_validas,
        vd.primeira_data_dum_valida,
        vd.primeira_data_dpp,
        -- Definição da data de fim da gestacao (somente eventos identificados e considerados pelo SISAB)
        LEAST(ra.menor_data_registro_aborto,vd.primeira_data_dpp)::date AS data_fim_primeira_gestacao,
        CASE 
            WHEN LEAST(ra.menor_data_registro_aborto,vd.primeira_data_dpp) = ra.menor_data_registro_aborto 
                THEN 'primeira_gestacao_encerrada_registro_aborto'
            WHEN LEAST(ra.menor_data_registro_aborto,vd.primeira_data_dpp,'2300-01-01'::date) > CURRENT_DATE 
                THEN 'primeira_gestacao_nao_encerrada'
            WHEN LEAST(ra.menor_data_registro_aborto,vd.primeira_data_dpp) = vd.primeira_data_dpp 
                THEN 'primeira_gestacao_encerrada_DPP'
        END AS tipo_encerramento_primeira_gestacao,
        -- Nos casos de gestantes com um histórico de atendimentos numa janela maior que 9 meses ou mais de um registro de data de DUM com intervalos maiores que
        -- 3 meses sem identificação correta de fim de gestação, há possibilidade de erros de registros ou segunda gestação não sinalizada
        -- Nesses casos não inferimos datas de fim e de início, porém sinalizamos possivel erro no registro da gestante
        CASE
            WHEN vd.maior_data_consulta_pre_natal >= LEAST(ra.menor_data_registro_aborto,vd.primeira_data_dpp, '2300-01-01'::date)
                THEN NULL 
            WHEN LEAST(ra.menor_data_registro_aborto,vd.primeira_data_dpp)::date IS NOT NULL  
                THEN NULL 
            WHEN vd.diff_maior_menor_data_dum > 90 OR vd.diff_maior_menor_data_consulta_pre_natal > 294
                THEN 'possivel_gestante_com_duas_gestacoes_ou_erro_registro_DUM'
            WHEN CURRENT_DATE - vd.maior_data_consulta_pre_natal > 294
                THEN 'possivel_gestante_com_gestacao_encerrada'
        END AS quant_gestacoes,
        -- Temos casos de gestante com mais de um parto - datas próximas, ou possível indicação de registros duplicados
        CASE 
            WHEN diff_dias_primeio_ultimo_parto > 180 
                THEN 'possibilidade_dois_partos_ou_erro_registro'
            WHEN diff_dias_primeio_ultimo_parto = 0
                THEN 'apenas_um_parto'
            ELSE 'possibilidade_apenas_um_parto_ou_erro_registro'
        END AS tipo_registro_parto,
        -- Temos casos de gestante com mais de um aborto - datas próximas, ou possível indicação de registros duplicados
        CASE 
            WHEN diff_dias_primeio_ultimo_aborto > 60 
                THEN 'possibilidade_dois_abortos_ou_erro_registro'
            WHEN diff_dias_primeio_ultimo_aborto = 0
                THEN 'apenas_um_aborto'
            ELSE 'possibilidade_apenas_um_aborto_ou_erro_registro'
        END AS tipo_registro_aborto 
    FROM validacao_dum vd 
    LEFT JOIN validacao_registros_parto rp 
        ON rp.chave_gestante = vd.chave_gestante
        AND rp.municipio_id_sus = vd.municipio_id_sus
    LEFT JOIN validacao_registros_aborto ra 
        ON ra.chave_gestante = vd.chave_gestante
        AND ra.municipio_id_sus = vd.municipio_id_sus
)
, base_atendimentos_por_gestacao AS (
/*
    As gestacoes sao identificadas a partir da data_fim_primeira_gestacao. 
    Todos os registros que ocorrem ANTES dessa data entram para a PRIMEIRA GESTAÇÃO
    Todos os registros que ocorrem DEPOIS dessa data entram para a SEGUNDA GESTAÇÃO
    Nos casos de gestantes com apenas DUMs invalidas - todos os registros entram para a PRIMEIRA GESTAÇÃO
*/
-- PRIMEIRA GESTACAO IDENTIFICADA
    SELECT
        apn.municipio_id_sus,
        apn.chave_gestante||'_1' AS chave_gestacao,
        'primeira_gestacao_identificada' AS ordem_gestacao,
        apn.id_registro,
        apn.chave_gestante,
        apn.data_atendimento,
        apn.profissional_nome_atendimento,
        apn.data_dum_atendimento,
        apn.data_dpp_atendimento,
        apn.gestante_idade_gestacional,
        apn.gestante_idade_gestacional_atendimento,
        row_number() OVER (PARTITION BY apn.chave_gestante ORDER BY apn.data_atendimento, apn.id_registro) AS ordem_consulta_pre_natal_gestacao,
        FIRST_VALUE(apn.data_atendimento) OVER (PARTITION BY apn.chave_gestante ORDER BY apn.data_atendimento) AS data_primeiro_atendimento,
        FIRST_VALUE(apn.data_atendimento) OVER (PARTITION BY apn.chave_gestante ORDER BY apn.data_atendimento desc) AS data_ultimo_atendimento,
        (array_agg(apn.data_dum_atendimento) FILTER (WHERE apn.data_dum_atendimento IS NOT NULL) OVER (PARTITION BY apn.chave_gestante ORDER BY apn.data_atendimento ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS data_primeira_DUM_valida,
        (array_agg(apn.data_atendimento) FILTER (WHERE apn.data_dum_atendimento IS NOT NULL) OVER (PARTITION BY apn.chave_gestante ORDER BY apn.data_atendimento ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS data_atendimento_com_primeira_DUM_valida,
        (array_agg(apn.gestante_idade_gestacional_atendimento) FILTER (WHERE apn.data_dum_atendimento IS NOT NULL) OVER (PARTITION BY apn.chave_gestante ORDER BY apn.data_atendimento ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS idade_gestacional_atendimento_com_primeira_DUM_valida,
        (array_agg(apn.gestante_idade_gestacional) FILTER (WHERE apn.data_dum_atendimento IS NOT NULL) OVER (PARTITION BY apn.chave_gestante ORDER BY apn.data_atendimento ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS idade_gestacional_atual_com_primeira_DUM_valida,
        cg.data_fim_primeira_gestacao,
        cg.tipo_encerramento_primeira_gestacao,
        cg.quant_gestacoes,
        cg.tipo_registro_parto, 
        cg.tipo_registro_aborto,    
        CASE 
            WHEN apn.data_dpp_atendimento IS NOT NULL AND apn.data_dpp_atendimento < apn.data_atendimento
                THEN 'registro_de_pre_natal_com_dpp_no_passado'
        END AS registro_com_dpp_passado,
        CASE 
            WHEN apn.data_atendimento - cg.data_fim_primeira_gestacao BETWEEN 0 AND 30 
                THEN 'possivel_consulta_pos_parto_ou_parto_tardio_ou_erro_DUM'
        END AS consulta_proxima_fim_gestacao 
    FROM base_atendimentos_pre_natal apn
    JOIN analise_gestante cg 
        ON cg.chave_gestante = apn.chave_gestante
        AND cg.municipio_id_sus = apn.municipio_id_sus
    WHERE apn.data_atendimento < cg.data_fim_primeira_gestacao
        OR cg.data_fim_primeira_gestacao IS NULL -- Gestantes com DUM inválidas sem registro de fim de gestacao
UNION ALL 
-- SEGUNDA GESTACAO IDENTIFICADA
    SELECT
        apn.municipio_id_sus,
        apn.chave_gestante||'_2' AS chave_gestacao,
        'segunda_gestacao_identificada' AS ordem_gestacao,
        apn.id_registro,
        apn.chave_gestante,
        apn.data_atendimento,
        apn.profissional_nome_atendimento,
        apn.data_dum_atendimento,
        apn.data_dpp_atendimento,
        apn.gestante_idade_gestacional,
        apn.gestante_idade_gestacional_atendimento,
        row_number() OVER (PARTITION BY apn.chave_gestante ORDER BY apn.data_atendimento, apn.id_registro) AS ordem_consulta_pre_natal_gestacao,
        FIRST_VALUE(apn.data_atendimento) OVER (PARTITION BY apn.chave_gestante ORDER BY apn.data_atendimento) AS data_primeiro_atendimento,
        FIRST_VALUE(apn.data_atendimento) OVER (PARTITION BY apn.chave_gestante ORDER BY apn.data_atendimento desc) AS data_ultimo_atendimento,
        (array_agg(apn.data_dum_atendimento) FILTER (WHERE apn.data_dum_atendimento IS NOT NULL) OVER (PARTITION BY apn.chave_gestante ORDER BY apn.data_atendimento ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS data_primeira_DUM_valida,
        (array_agg(apn.data_atendimento) FILTER (WHERE apn.data_dum_atendimento IS NOT NULL) OVER (PARTITION BY apn.chave_gestante ORDER BY apn.data_atendimento ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS data_atendimento_com_primeira_DUM_valida,
        (array_agg(apn.gestante_idade_gestacional_atendimento) FILTER (WHERE apn.data_dum_atendimento IS NOT NULL) OVER (PARTITION BY apn.chave_gestante ORDER BY apn.data_atendimento ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS idade_gestacional_atendimento_com_primeira_DUM_valida,
        (array_agg(apn.gestante_idade_gestacional) FILTER (WHERE apn.data_dum_atendimento IS NOT NULL) OVER (PARTITION BY apn.chave_gestante ORDER BY apn.data_atendimento ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS idade_gestacional_atual_com_primeira_DUM_valida,
        cg.data_fim_primeira_gestacao,
        cg.tipo_encerramento_primeira_gestacao,
        cg.quant_gestacoes,
        cg.tipo_registro_parto, 
        cg.tipo_registro_aborto,
        -- Há casos de registros de DUM que acarretam em datas de DPP anteriores a data de atendimento do pré-natal. 
        -- Nesse caso o pré-natal registrado indica uma gestacao que já ocorreu, nao sendo possivel contabilizar nos indicadores de pré-natal 
        -- OBS: gestantes que fizeram uso de metodos contraceptivos que interropem a menstruacao por muito tempo podem entrar nesses casos.
        CASE 
            WHEN apn.data_dpp_atendimento IS NOT NULL AND apn.data_dpp_atendimento < apn.data_atendimento
                THEN 'registro_de_pre_natal_com_dpp_no_passado'
        END AS registro_com_dpp_passado,
        -- Há casos de consultas de pré natal logo após o encerramento da gestacao. 
        -- Nesse caso o SISAB contabiliza como uma nova gestacao. Mas existem casos de parto tardio ou consulta de pos-parto mal registrada ou erro de DUM
        CASE 
            WHEN apn.data_atendimento - cg.data_fim_primeira_gestacao BETWEEN 0 AND 30 
                THEN 'possivel_consulta_pos_parto_ou_parto_tardio_ou_erro_DUM'
        END AS consulta_proxima_fim_gestacao
    FROM base_atendimentos_pre_natal apn
    JOIN analise_gestante cg 
        ON cg.chave_gestante = apn.chave_gestante
        AND cg.municipio_id_sus = apn.municipio_id_sus
    WHERE apn.data_atendimento >= cg.data_fim_primeira_gestacao
)
, infos_gestante_atendimento_individual_recente AS (
    WITH base AS (
        SELECT 
            b.municipio_id_sus,
            b.chave_gestante,
            b.gestante_nome,
            b.gestante_data_de_nascimento,
            (array_agg(b.gestante_documento_cpf) FILTER (WHERE b.gestante_documento_cpf IS NOT NULL) OVER (PARTITION BY b.chave_gestante ORDER BY b.data_registro DESC))[1] AS gestante_documento_cpf,
            (array_agg(b.gestante_documento_cns) FILTER (WHERE b.gestante_documento_cns IS NOT NULL) OVER (PARTITION BY b.chave_gestante ORDER BY b.data_registro DESC))[1] AS gestante_documento_cns,
            b.gestante_telefone,
            b.estabelecimento_cnes_atendimento,
            b.estabelecimento_nome_atendimento,
            b.equipe_ine_atendimento,
            b.equipe_nome_atendimento,
            b.data_ultimo_cadastro_individual,
            b.estabelecimento_cnes_cad_indivual,
            b.estabelecimento_nome_cad_individual,
            b.equipe_ine_cad_individual,
            b.equipe_nome_cad_individual,
            b.acs_cad_individual,
            b.data_ultima_visita_acs,
            b.acs_visita_domiciliar,
            --b.data_ultimo_cadastro_dom_familia,
            --b.micro_area_domicilio,
            --b.cnes_estabelecimento_cad_dom_familia,
            --b.estabelecimento_cad_dom_familia,
            --b.ine_equipe_cad_dom_familia,
            --b.equipe_cad_dom_familia,
            b.acs_cad_dom_familia,
            row_number() OVER (PARTITION BY b.chave_gestante ORDER BY b.id_registro DESC) = 1 AS ultimo_atendimento_individual
        FROM impulso_previne_dados_nominais.eventos_pre_natal b
        WHERE tipo_registro = 'consulta_pre_natal'
    )
SELECT * FROM base WHERE ultimo_atendimento_individual IS TRUE 
) 
, base_final_gestacoes AS (
    SELECT 
        bag.municipio_id_sus,
        bag.chave_gestacao,
        bag.ordem_gestacao,
        bag.chave_gestante,
        ig.gestante_telefone,
        ig.gestante_nome,
        ig.gestante_data_de_nascimento,
        COALESCE(NULLIF(ig.estabelecimento_cnes_cad_indivual::text, '-'::text), ig.estabelecimento_cnes_atendimento::text) AS estabelecimento_cnes,
        UPPER(COALESCE(NULLIF(ig.estabelecimento_nome_cad_individual::text, 'Não informado'::text), ig.estabelecimento_nome_atendimento::text)) AS estabelecimento_nome,
        COALESCE(NULLIF(ig.equipe_ine_cad_individual::text, '-'::text), ig.equipe_ine_atendimento::text) AS equipe_ine,
        UPPER(COALESCE(NULLIF(ig.equipe_nome_cad_individual::text, 'SEM EQUIPE'::text), ig.equipe_nome_atendimento::text)) AS equipe_nome,
        UPPER(COALESCE(ig.acs_visita_domiciliar, ig.acs_cad_individual, 'SEM ACS')) AS acs_nome,
        ig.data_ultima_visita_acs AS acs_data_ultima_visita,
        bag.data_primeira_DUM_valida AS gestacao_data_dum,
        (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE AS gestacao_data_dpp,
        (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE - CURRENT_DATE AS gestacao_dpp_dias_para,
        CASE
            WHEN (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE >= '2022-01-01'::date AND (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE <= '2022-04-30'::date THEN '2022.Q1'::text
            WHEN (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE >= '2022-05-01'::date AND (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE <= '2022-08-31'::date THEN '2022.Q2'::text
            WHEN (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE >= '2022-09-01'::date AND (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE <= '2022-12-31'::date THEN '2022.Q3'::text
            WHEN (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE >= '2023-01-01'::date AND (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE <= '2023-04-30'::date THEN '2023.Q1'::text
            WHEN (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE >= '2023-05-01'::date AND (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE <= '2023-08-31'::date THEN '2023.Q2'::text
            WHEN (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE >= '2023-09-01'::date AND (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE <= '2023-12-31'::date THEN '2023.Q3'::text
            WHEN (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE >= '2024-01-01'::date AND (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE <= '2024-04-30'::date THEN '2024.Q1'::text
            WHEN (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE >= '2024-05-01'::date AND (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE <= '2024-08-31'::date THEN '2024.Q2'::text
            WHEN (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE >= '2024-09-01'::date AND (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE <= '2024-12-31'::date THEN '2024.Q3'::text
            WHEN (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE >= '2025-01-01'::date AND (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE <= '2025-04-30'::date THEN '2025.Q1'::text
            WHEN (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE >= '2025-05-01'::date AND (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE <= '2025-08-31'::date THEN '2025.Q2'::text
            WHEN (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE >= '2025-09-01 '::date AND (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE <= '2025-08-31'::date THEN '2025.Q3'::text
            ELSE 'SEM QUADRI'::text
        END AS gestacao_quadrimestre,
        bag.idade_gestacional_atendimento_com_primeira_DUM_valida AS gestacao_idade_gestacional_primeiro_atendimento,
        bag.data_primeiro_atendimento AS consulta_prenatal_primeira_data,
        bag.data_ultimo_atendimento AS consulta_prenatal_ultima_data,
        CURRENT_DATE - bag.data_ultimo_atendimento AS consulta_prenatal_ultima_dias_desde,
        bag.data_fim_primeira_gestacao,
        bag.tipo_encerramento_primeira_gestacao,
        ig.gestante_documento_cpf,
        ig.gestante_documento_cns,
        bag.idade_gestacional_atual_com_primeira_DUM_valida AS gestacao_idade_gestacional_atual,
        CONCAT(max(bag.quant_gestacoes),', ',max(bag.tipo_registro_parto),', ', max(bag.tipo_registro_aborto),', ',max(bag.registro_com_dpp_passado), ', ',max(consulta_proxima_fim_gestacao)) AS sinalizacao_erro_registro,
        -- No caso de somente DUM inváidas, não inferimos datas de início e fim de gestação
        CASE 
            WHEN COUNT(DISTINCT bag.data_dum_atendimento)= 0
                THEN 'somente_DUMs_invalidas'
            WHEN COUNT(DISTINCT bag.data_dum_atendimento) > 1
                THEN 'mais_de_uma_DUM_valida'
            WHEN COUNT(DISTINCT bag.data_dum_atendimento) = 1
                THEN 'uma_DUM_valida'
        END AS gestacao_qtde_dums,
        min(CASE
                    WHEN bag.data_dum_atendimento IS NOT NULL THEN bag.ordem_consulta_pre_natal_gestacao
                    ELSE NULL::bigint
                END) AS ordem_primeira_consulta_com_dum,       
        COUNT(DISTINCT bag.id_registro) AS consultas_prenatal_total,
        -- O SISAB só contabiliza consultas de pré-natal a partir da consulta com a primeira DUM válida e com realizadas por profissional corretamente cadastrado
        COUNT(DISTINCT CASE 
                            WHEN bag.data_atendimento >= data_atendimento_com_primeira_DUM_valida 
                                    AND bag.profissional_nome_atendimento NOT IN ('Não informado', 'PROFISSIONAL NÃO CADASTRADO') 
                                THEN bag.id_registro 
              END) AS consultas_pre_natal_validas,
        -- Quando só há DUMs inválida, não há data_fim_primeira_gestacao. Nesse caso contabilizamos exames e consultas em uma gestacao apenas
        COUNT(CASE 
                        WHEN bag.ordem_gestacao = 'primeira_gestacao_identificada' AND odonto.data_registro BETWEEN bag.data_primeira_DUM_valida AND bag.data_fim_primeira_gestacao
                            THEN odonto.data_registro
                        WHEN bag.ordem_gestacao = 'segunda_gestacao_identificada' AND odonto.data_registro BETWEEN bag.data_primeira_DUM_valida AND (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE
                            THEN odonto.data_registro
                        WHEN bag.data_fim_primeira_gestacao IS NULL AND odonto.data_registro >= bag.data_primeiro_atendimento
                            THEN odonto.data_registro
              END) > 0 
        AS atendimento_odontologico_realizado,
        COUNT(CASE 
                    WHEN bag.ordem_gestacao = 'primeira_gestacao_identificada' AND hiv.data_registro BETWEEN bag.data_primeira_DUM_valida AND bag.data_fim_primeira_gestacao
                        THEN hiv.data_registro
                    WHEN bag.ordem_gestacao = 'segunda_gestacao_identificada' AND hiv.data_registro BETWEEN bag.data_primeira_DUM_valida AND (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE
                        THEN hiv.data_registro
                    WHEN bag.data_fim_primeira_gestacao IS NULL AND hiv.data_registro >= bag.data_primeiro_atendimento
                        THEN hiv.data_registro
              END) > 0 
        AS exame_hiv_realizado,
        COUNT(CASE 
                    WHEN bag.ordem_gestacao = 'primeira_gestacao_identificada' AND sifilis.data_registro BETWEEN bag.data_primeira_DUM_valida AND bag.data_fim_primeira_gestacao
                        THEN sifilis.data_registro
                    WHEN bag.ordem_gestacao = 'segunda_gestacao_identificada' AND sifilis.data_registro BETWEEN bag.data_primeira_DUM_valida AND (bag.data_primeira_DUM_valida + '294 days'::INTERVAL)::DATE
                        THEN sifilis.data_registro
                    WHEN bag.data_fim_primeira_gestacao IS NULL AND sifilis.data_registro >= bag.data_primeiro_atendimento
                        THEN sifilis.data_registro
              END) > 0 
        AS exame_sifilis_realizado,
        CASE
            WHEN COUNT(CASE 
                    WHEN bag.ordem_gestacao = 'primeira_gestacao_identificada' AND aborto.data_registro <= bag.data_fim_primeira_gestacao
                        THEN aborto.data_registro
                    WHEN bag.ordem_gestacao = 'segunda_gestacao_identificada' AND aborto.data_registro > bag.data_fim_primeira_gestacao
                        THEN aborto.data_registro
                    END) > 0 
            THEN 'Sim'
            ELSE 'Não'
        END AS possui_registro_aborto,
        CASE
            WHEN COUNT(CASE 
                    WHEN bag.ordem_gestacao = 'primeira_gestacao_identificada' AND (parto.data_registro <= bag.data_fim_primeira_gestacao + INTERVAL '180 days') 
                        THEN parto.data_registro
                    WHEN bag.ordem_gestacao = 'segunda_gestacao_identificada' AND parto.data_registro > bag.data_fim_primeira_gestacao + INTERVAL '180 days'
                        THEN parto.data_registro
                    END) > 0 
            THEN 'Sim'
            ELSE 'Não'
        END AS possui_registro_parto
    FROM base_atendimentos_por_gestacao bag
    LEFT JOIN infos_gestante_atendimento_individual_recente ig
        ON bag.chave_gestante = ig.chave_gestante
        AND bag.municipio_id_sus = ig.municipio_id_sus
    LEFT JOIN impulso_previne_dados_nominais.eventos_pre_natal odonto 
        ON bag.chave_gestante = odonto.chave_gestante
        AND bag.municipio_id_sus = odonto.municipio_id_sus
        AND odonto.tipo_registro = 'atendimento_odontologico'
    LEFT JOIN impulso_previne_dados_nominais.eventos_pre_natal sifilis 
        ON bag.chave_gestante = sifilis.chave_gestante
        AND bag.municipio_id_sus = sifilis.municipio_id_sus
        AND sifilis.tipo_registro IN ('teste_rapido_exame_sifilis','exame_sifilis_avaliado')
    LEFT JOIN impulso_previne_dados_nominais.eventos_pre_natal hiv 
        ON bag.chave_gestante = hiv.chave_gestante
        AND bag.municipio_id_sus = hiv.municipio_id_sus
        AND hiv.tipo_registro IN ('teste_rapido_exame_hiv','exame_hiv_avaliado')
    LEFT JOIN impulso_previne_dados_nominais.eventos_pre_natal parto 
        ON bag.chave_gestante = parto.chave_gestante
        AND bag.municipio_id_sus = parto.municipio_id_sus
        AND parto.tipo_registro = 'registro_de_parto'
    LEFT JOIN impulso_previne_dados_nominais.eventos_pre_natal aborto 
        ON bag.chave_gestante = aborto.chave_gestante
        AND bag.municipio_id_sus = aborto.municipio_id_sus
        AND aborto.tipo_registro = 'registro_de_aborto'
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26
)
, aux AS (
SELECT 
    *, 
    CASE
        WHEN exame_sifilis_realizado IS TRUE  AND exame_hiv_realizado IS TRUE 
            THEN TRUE
        ELSE FALSE
    END AS exame_sifilis_hiv_realizado,
    now() AS atualizacao_data,
    now() AS criacao_data 
FROM base_final_gestacoes
WHERE 
    gestacao_data_dpp >= (CASE
                                WHEN date_part('month', CURRENT_DATE) >= 1::double precision AND date_part('month', CURRENT_DATE) <= 4::double precision THEN concat(date_part('year', (CURRENT_DATE - '365 days'::interval)::date), '-09-01')
                                WHEN date_part('month', CURRENT_DATE) >= 5::double precision AND date_part('month', CURRENT_DATE) <= 8::double precision THEN concat(date_part('year', CURRENT_DATE), '-01-01')
                                WHEN date_part('month', CURRENT_DATE) >= 9::double precision AND date_part('month', CURRENT_DATE) <= 12::double precision THEN concat(date_part('year', CURRENT_DATE), '-05-01')
                                ELSE NULL::text
                            END::date)
    OR consulta_prenatal_ultima_data >= CASE
                                            WHEN date_part('month', CURRENT_DATE) >= 1::double precision AND date_part('month', CURRENT_DATE) <= 4::double precision THEN concat(date_part('year', (CURRENT_DATE - '365 days'::interval)::date), '-09-01')
                                            WHEN date_part('month', CURRENT_DATE) >= 5::double precision AND date_part('month', CURRENT_DATE) <= 8::double precision THEN concat(date_part('year', CURRENT_DATE), '-01-01')
                                            WHEN date_part('month', CURRENT_DATE) >= 9::double precision AND date_part('month', CURRENT_DATE) <= 12::double precision THEN concat(date_part('year', CURRENT_DATE), '-05-01')
                                            ELSE NULL::text
                                        END::date
)
SELECT *
FROM aux
-- Filtro de INEs de equipes fora do município de Santa Luz - PI (o centralizador do município transmite dados do município vizinho Palmeira do Piauí - PI que não é parceiro da ImpulsoGov)
WHERE  equipe_ine NOT IN ('0000071722', '0000071730', '0001511912', '0001846892', '0001847236', '0002275872')
WITH DATA;

-- View indexes:
CREATE INDEX lista_nominal_gestantes_consulta_prenatal_primeira_data_idx ON impulso_previne_dados_nominais.lista_nominal_gestantes_unificada USING btree (consulta_prenatal_primeira_data, consulta_prenatal_ultima_data, ordem_gestacao);
CREATE INDEX lista_nominal_gestantes_gestacao_data_dpp_idx ON impulso_previne_dados_nominais.lista_nominal_gestantes_unificada USING btree (gestacao_data_dpp);
