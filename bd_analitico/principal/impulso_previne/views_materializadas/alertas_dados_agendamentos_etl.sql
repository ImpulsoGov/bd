-- impulso_previne.alertas_dados_agendamentos_etl source

CREATE MATERIALIZED VIEW impulso_previne.alertas_dados_agendamentos_etl
TABLESPACE pg_default
AS WITH etl_validacao_producao AS (
         SELECT 'SISAB - Relatório de Validação'::text AS etl_nome,
            ca.tabela_destino,
            ca.periodo_codigo,
            ca.atualizacao_retroativa
           FROM configuracoes.capturas_agendamentos ca
          WHERE ca.operacao_id = 'c577c9fd-6a8e-43e3-9d65-042ad2268cf0'::uuid
        ), etl_indicadores_desempenho_equipes_validas AS (
         SELECT 'SISAB - Indicadores de Desempenho Equipes Válidas'::text AS etl_nome,
            ca.tabela_destino,
            ca.periodo_codigo,
            ca.atualizacao_retroativa
           FROM configuracoes.capturas_agendamentos ca
          WHERE ca.operacao_id = '133e8b75-f801-42f5-88de-611c3a1d0aa7'::uuid
        ), etl_indicadores_desempenho_equipes_homologadas AS (
         SELECT 'SISAB - Indicadores de Desempenho Equipes Homologadas'::text AS etl_nome,
            ca.tabela_destino,
            ca.periodo_codigo,
            ca.atualizacao_retroativa
           FROM configuracoes.capturas_agendamentos ca
          WHERE ca.operacao_id = '584b190b-7a4c-4577-b617-1d847655affc'::uuid
        ), etl_indicadores_desempenho_equipes_todas AS (
         SELECT 'SISAB - Indicadores de Desempenho Todas as Equipes'::text AS etl_nome,
            ca.tabela_destino,
            ca.periodo_codigo,
            ca.atualizacao_retroativa
           FROM configuracoes.capturas_agendamentos ca
          WHERE ca.operacao_id = '9d6b0b5d-bae7-4785-8c7b-ff55dc4386e0'::uuid
        ), etl_cadastros_individuais_equipes_todas AS (
         SELECT 'SISAB - Cadastros Individuais Todas as Equipes'::text AS etl_nome,
            ca.tabela_destino,
            ca.periodo_codigo,
            ca.atualizacao_retroativa
           FROM configuracoes.capturas_agendamentos ca
          WHERE ca.operacao_id = '180ae562-2e34-4ae7-bff4-31ded6f0b418'::uuid
        ), etl_cadastros_individuais_equipes_validas AS (
         SELECT 'SISAB - Cadastros Individuais Equipes Válidas'::text AS etl_nome,
            ca.tabela_destino,
            ca.periodo_codigo,
            ca.atualizacao_retroativa
           FROM configuracoes.capturas_agendamentos ca
          WHERE ca.operacao_id = 'da6bf13a-2acd-44c1-a3e2-21ab071fc8a3'::uuid
        ), etl_cadastros_individuais_equipes_homologadas AS (
         SELECT 'SISAB - Cadastros Individuais Equipes Homologadas'::text AS etl_nome,
            ca.tabela_destino,
            ca.periodo_codigo,
            ca.atualizacao_retroativa
           FROM configuracoes.capturas_agendamentos ca
          WHERE ca.operacao_id = 'c668a75e-9eeb-4176-874b-98d7553222f2'::uuid
        ), etl_egestor_financiamento AS (
         SELECT 'Egestor - Relatório de Financiamento APS'::text AS etl_nome,
            ca.tabela_destino,
            ca.periodo_codigo,
            ca.atualizacao_retroativa
           FROM configuracoes.capturas_agendamentos ca
          WHERE ca.operacao_id = '0635c38b-df79-7b13-865e-db5334aab8c6'::uuid
        ), etl_sisab_producao_prof_reduzidos AS (
         SELECT 'SISAB - Relatório de Saúde/Produção'::text AS etl_nome,
            ca.tabela_destino,
            ca.periodo_codigo,
            ca.atualizacao_retroativa
           FROM configuracoes.capturas_agendamentos ca
          WHERE ca.operacao_id = '063e2878-3247-78a7-83dd-1d291156cdf6'::uuid
        ), etl_sisab_producao_prof_outros AS (
         SELECT 'SISAB - Relatório de Saúde/Produção'::text AS etl_nome,
            ca.tabela_destino,
            ca.periodo_codigo,
            ca.atualizacao_retroativa
           FROM configuracoes.capturas_agendamentos ca
          WHERE ca.operacao_id = '06423293-7fac-7493-b209-e5aa489879fb'::uuid
        ), une_tabelas AS (
         SELECT etl_validacao_producao.etl_nome,
            etl_validacao_producao.tabela_destino,
            etl_validacao_producao.periodo_codigo,
            etl_validacao_producao.atualizacao_retroativa
           FROM etl_validacao_producao
        UNION ALL
         SELECT etl_indicadores_desempenho_equipes_validas.etl_nome,
            etl_indicadores_desempenho_equipes_validas.tabela_destino,
            etl_indicadores_desempenho_equipes_validas.periodo_codigo,
            etl_indicadores_desempenho_equipes_validas.atualizacao_retroativa
           FROM etl_indicadores_desempenho_equipes_validas
        UNION ALL
         SELECT etl_indicadores_desempenho_equipes_homologadas.etl_nome,
            etl_indicadores_desempenho_equipes_homologadas.tabela_destino,
            etl_indicadores_desempenho_equipes_homologadas.periodo_codigo,
            etl_indicadores_desempenho_equipes_homologadas.atualizacao_retroativa
           FROM etl_indicadores_desempenho_equipes_homologadas
        UNION ALL
         SELECT etl_indicadores_desempenho_equipes_todas.etl_nome,
            etl_indicadores_desempenho_equipes_todas.tabela_destino,
            etl_indicadores_desempenho_equipes_todas.periodo_codigo,
            etl_indicadores_desempenho_equipes_todas.atualizacao_retroativa
           FROM etl_indicadores_desempenho_equipes_todas
        UNION ALL
         SELECT etl_cadastros_individuais_equipes_todas.etl_nome,
            etl_cadastros_individuais_equipes_todas.tabela_destino,
            etl_cadastros_individuais_equipes_todas.periodo_codigo,
            etl_cadastros_individuais_equipes_todas.atualizacao_retroativa
           FROM etl_cadastros_individuais_equipes_todas
        UNION ALL
         SELECT etl_cadastros_individuais_equipes_validas.etl_nome,
            etl_cadastros_individuais_equipes_validas.tabela_destino,
            etl_cadastros_individuais_equipes_validas.periodo_codigo,
            etl_cadastros_individuais_equipes_validas.atualizacao_retroativa
           FROM etl_cadastros_individuais_equipes_validas
        UNION ALL
         SELECT etl_cadastros_individuais_equipes_homologadas.etl_nome,
            etl_cadastros_individuais_equipes_homologadas.tabela_destino,
            etl_cadastros_individuais_equipes_homologadas.periodo_codigo,
            etl_cadastros_individuais_equipes_homologadas.atualizacao_retroativa
           FROM etl_cadastros_individuais_equipes_homologadas
        UNION ALL
         SELECT etl_egestor_financiamento.etl_nome,
            etl_egestor_financiamento.tabela_destino,
            etl_egestor_financiamento.periodo_codigo,
            etl_egestor_financiamento.atualizacao_retroativa
           FROM etl_egestor_financiamento
        UNION ALL
         SELECT etl_sisab_producao_prof_reduzidos.etl_nome,
            etl_sisab_producao_prof_reduzidos.tabela_destino,
            etl_sisab_producao_prof_reduzidos.periodo_codigo,
            etl_sisab_producao_prof_reduzidos.atualizacao_retroativa
           FROM etl_sisab_producao_prof_reduzidos
        UNION ALL
         SELECT etl_sisab_producao_prof_outros.etl_nome,
            etl_sisab_producao_prof_outros.tabela_destino,
            etl_sisab_producao_prof_outros.periodo_codigo,
            etl_sisab_producao_prof_outros.atualizacao_retroativa
           FROM etl_sisab_producao_prof_outros
        )
 SELECT tb1.etl_nome,
    tb1.tabela_destino,
    tb1.periodo_codigo,
    tb1.atualizacao_retroativa
   FROM une_tabelas tb1
     LEFT JOIN impulso_previne.alertas_dados_agendamentos_etl_notificados tb2 ON tb1.etl_nome = tb2.etl_nome AND tb1.tabela_destino = tb2.tabela_destino AND tb1.periodo_codigo::text = tb2.periodo_codigo::text AND tb1.atualizacao_retroativa = tb2.atualizacao_retroativa
  WHERE tb2.etl_nome IS NULL
WITH DATA;