-- impulso_previne.alertas_dados_atualizacoes source

CREATE OR REPLACE VIEW impulso_previne.alertas_dados_atualizacoes
AS WITH unifica_etl_nome AS (
         SELECT
                CASE
                    WHEN tb1_1.etl_nome ~~ 'SISAB - Indicadores de Desempenho%'::text THEN 'Indicadores de Desempenho'::text
                    WHEN tb1_1.etl_nome ~~ 'SISAB - Cadastros Individuais%'::text THEN 'Capitação Ponderada - Cadastros Individuais'::text
                    WHEN tb1_1.etl_nome ~~ 'Egestor - Relatório de Financiamento APS%'::text THEN 'Incentivos para Ações Estratégicas'::text
                    WHEN tb1_1.etl_nome ~~ 'SISAB - Relatório de Validação%'::text THEN 'Capitação Ponderada - Validação da Produção'::text
                    ELSE NULL::text
                END AS painel_nome,
            tb1_1.periodo_codigo
           FROM impulso_previne.alertas_dados_agendamentos_etl tb1_1
        ), ultima_competencia_em_agenda AS (
         SELECT tb1_1.painel_nome,
            max(tb1_1.periodo_codigo::text) AS ultimo_periodo_agendado
           FROM unifica_etl_nome tb1_1
          WHERE tb1_1.painel_nome IS NOT NULL
          GROUP BY tb1_1.painel_nome
        )
 SELECT tb1.painel_nome,
    tb1.ultimo_periodo_disponivel,
        CASE
            WHEN tb1.painel_nome = 'Acessos na Área Logada'::text THEN (CURRENT_DATE - '1 day'::interval)::date::text
            WHEN tb2.ultimo_periodo_agendado IS NULL THEN tb1.ultimo_periodo_disponivel::text
            ELSE tb2.ultimo_periodo_agendado
        END AS periodo_esperado,
    tb1.ultima_atualizacao
   FROM impulso_previne.alertas_dados_ultimas_atualizacoes tb1
     LEFT JOIN ultima_competencia_em_agenda tb2 ON tb1.painel_nome = tb2.painel_nome;