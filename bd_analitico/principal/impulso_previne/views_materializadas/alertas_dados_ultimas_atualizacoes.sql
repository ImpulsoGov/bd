-- impulso_previne.alertas_dados_ultimas_atualizacoes source

CREATE MATERIALIZED VIEW impulso_previne.alertas_dados_ultimas_atualizacoes
TABLESPACE pg_default
AS WITH capitacao_ponderada_cadastros AS (
         SELECT 'Capitação Ponderada - Cadastros Individuais'::text AS painel_nome,
            cp.periodo_codigo AS ultimo_periodo_disponivel,
            cp.atualizacao_data AS ultima_atualizacao
           FROM impulso_previne.capitacao_ponderada_cadastros_por_equipes cp
          ORDER BY cp.data_inicio DESC
         LIMIT 1
        ), capitacao_ponderada_validacao AS (
         SELECT 'Capitação Ponderada - Validação da Produção'::text AS painel_nome,
            cp.periodo_codigo AS ultimo_periodo_disponivel,
            cp.atualizacao_data AS ultima_atualizacao
           FROM impulso_previne.capitacao_ponderada_validacao_por_producao_por_aplicacao cp
          ORDER BY cp.periodo_data_inicio DESC
         LIMIT 1
        ), indicadores_desempenho AS (
         SELECT 'Indicadores de Desempenho'::text AS painel_nome,
            cp.periodo_codigo AS ultimo_periodo_disponivel,
            cp.atualizacao_data AS ultima_atualizacao
           FROM impulso_previne.indicadores_desempenho_score_equipes_validas cp
          ORDER BY cp.periodo_data_inicio DESC
         LIMIT 1
        ), acoes_estrategicas AS (
         SELECT 'Incentivos para Ações Estratégicas'::text AS painel_nome,
            cp.codigo AS ultimo_periodo_disponivel,
            cp.atualizacao_data AS ultima_atualizacao
           FROM impulso_previne.acoes_estrategicas_repasses cp
          ORDER BY cp.data_inicio DESC
         LIMIT 1
        ), acesso_area_logada AS (
         SELECT 'Acessos na Área Logada'::text AS painel_nome,
            "substring"(cp.periodo_data_hora::text, 1, 8)::date::text AS ultimo_periodo_disponivel,
            cp.atualizacao_data AS ultima_atualizacao
           FROM impulso_previne.usuarios_acessos_ga4 cp
          ORDER BY ("substring"(cp.periodo_data_hora::text, 1, 8)::date) DESC
         LIMIT 1
        )
 SELECT capitacao_ponderada_cadastros.painel_nome,
    capitacao_ponderada_cadastros.ultimo_periodo_disponivel,
    capitacao_ponderada_cadastros.ultima_atualizacao
   FROM capitacao_ponderada_cadastros
UNION ALL
 SELECT capitacao_ponderada_validacao.painel_nome,
    capitacao_ponderada_validacao.ultimo_periodo_disponivel,
    capitacao_ponderada_validacao.ultima_atualizacao
   FROM capitacao_ponderada_validacao
UNION ALL
 SELECT indicadores_desempenho.painel_nome,
    indicadores_desempenho.ultimo_periodo_disponivel,
    indicadores_desempenho.ultima_atualizacao
   FROM indicadores_desempenho
UNION ALL
 SELECT acoes_estrategicas.painel_nome,
    acoes_estrategicas.ultimo_periodo_disponivel,
    acoes_estrategicas.ultima_atualizacao
   FROM acoes_estrategicas
UNION ALL
 SELECT acesso_area_logada.painel_nome,
    acesso_area_logada.ultimo_periodo_disponivel,
    acesso_area_logada.ultima_atualizacao
   FROM acesso_area_logada
WITH DATA;