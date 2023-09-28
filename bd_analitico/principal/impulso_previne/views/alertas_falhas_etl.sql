-- impulso_previne.alertas_falhas_etl source

CREATE OR REPLACE VIEW impulso_previne.alertas_falhas_etl
AS WITH captura_erros AS (
         SELECT DISTINCT
                CASE
                    WHEN tb2.tabela_destino ~~ 'dados_publicos.sisab_cadastros_municipios%'::text THEN 'Capitação Ponderada - Cadastros Individuais'::text
                    WHEN tb2.tabela_destino ~~ 'dados_publicos.sisab_validacao_municipios%'::text THEN 'Capitação Ponderada - Validação da Produção'::text
                    WHEN tb2.tabela_destino ~~ 'dados_publicos.sisab_indicadores_municipios%'::text THEN 'Indicadores de Desempenho'::text
                    WHEN tb2.tabela_destino ~~ 'dados_publicos.egestor_financiamento_acoes_estrategicas%'::text THEN 'Incentivos para Ações Estratégicas'::text
                    WHEN tb2.tabela_destino ~~ 'dados_publicos._scnes_%'::text THEN 'Relatório do SCNES'::text
                    ELSE NULL::text
                END AS etl_nome,
            p.codigo AS periodo_codigo,
            tb1.erro_mensagem,
            tb1.erro_traceback
           FROM configuracoes.capturas_erros_etl tb1
             LEFT JOIN configuracoes.capturas_operacoes tb2 ON tb1.operacao_id = tb2.id
             JOIN listas_de_codigos.periodos p ON tb1.periodo_id = p.id
          WHERE tb2.projuto = 'Impulso Previne'::text
        ), identifica_notificados AS (
         SELECT DISTINCT tb1.etl_nome,
            tb1.periodo_codigo,
            tb1.erro_mensagem,
            tb1.erro_traceback
           FROM captura_erros tb1
             LEFT JOIN impulso_previne.alertas_erros_etl_notificados tb3 ON tb1.etl_nome = tb3.etl_nome AND tb1.erro_mensagem = tb3.erro_mensagem AND tb1.periodo_codigo::text = tb3.periodo_codigo::text
          WHERE tb3.etl_nome IS NULL
        )
 SELECT identifica_notificados.etl_nome,
    identifica_notificados.periodo_codigo,
    identifica_notificados.erro_mensagem,
    identifica_notificados.erro_traceback
   FROM identifica_notificados;