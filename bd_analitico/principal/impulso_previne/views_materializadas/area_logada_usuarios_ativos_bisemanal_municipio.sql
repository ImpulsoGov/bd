--- impulso_previne.area_logada_usuarios_ativos_bisemanal_municipio source

CREATE MATERIALIZED VIEW impulso_previne.area_logada_usuarios_ativos_bisemanal_municipio
TABLESPACE pg_default
AS WITH area_logada_eventos_atividade_retencao_usuarios AS (
         WITH area_logada_funil_cadastros_acessos_usuarios AS (
                 WITH usuarios_acessos AS (
                         SELECT uag.usuario_id,
                            min("substring"(uag.periodo_data_hora::text, 1, 8)::date) AS data_primeiro_acesso,
                            max("substring"(uag.periodo_data_hora::text, 1, 8)::date) AS data_ultimo_acesso,
                            min(
                                CASE
                                    WHEN uag.pagina_path = ANY (ARRAY['/conteudo-programatico'::text, '/capacitacao'::text, '/capacitacoes'::text, '/conteudo'::text, '/duvidas'::text, '/grupo-whatsapp'::text]) THEN "substring"(uag.periodo_data_hora::text, 1, 8)::date
                                    ELSE NULL::date
                                END) AS data_primeiro_acesso_trilha_ga4,
                            min(
                                CASE
                                    WHEN uag.pagina_path = '/busca-ativa'::text AND "substring"(uag.periodo_data_hora::text, 1, 8)::date <= '2023-03-27'::date OR uag.pagina_path = '/busca-ativa/gestantes'::text THEN "substring"(uag.periodo_data_hora::text, 1, 8)::date
                                    ELSE NULL::date
                                END) AS data_primeiro_acesso_lista_gestantes,
                            min(
                                CASE
                                    WHEN uag.pagina_path = '/busca-ativa/diabeticos'::text THEN "substring"(uag.periodo_data_hora::text, 1, 8)::date
                                    ELSE NULL::date
                                END) AS data_primeiro_acesso_lista_diabeticos,
                            min(
                                CASE
                                    WHEN uag.pagina_path = '/busca-ativa/hipertensos'::text THEN "substring"(uag.periodo_data_hora::text, 1, 8)::date
                                    ELSE NULL::date
                                END) AS data_primeiro_acesso_lista_hipertensos,
                            count(DISTINCT "substring"(uag.periodo_data_hora::text, 1, 8)::date) AS total_dias_ativo,
                            sum(uag.eventos) AS total_eventos,
                            sum(
                                CASE
                                    WHEN "substring"(uag.periodo_data_hora::text, 1, 8)::date > (CURRENT_DATE - '30 days'::interval) THEN uag.eventos
                                    ELSE 0
                                END) AS total_eventos_ultimos30d,
                            sum(uag.sessoes) AS total_sessoes,
                            sum(uag.sessao_duracao_media * uag.sessoes::double precision) AS tempo_total_atividade
                           FROM impulso_previne.usuarios_acessos_ga4_ajustada uag
                          WHERE 1 = 1 AND uag.usuarios_ativos > 0 AND uag.usuario_id <> '(not set)'::text AND (uag.cidade_acesso <> ALL (ARRAY['Sao Paulo'::text, 'Santo Andre'::text, 'Rio de Janeiro'::text, 'Brasilia'::text, 'Praia Grande'::text, 'Ribeirao Preto'::text, 'Santos'::text, 'Sao Bernardo do Campo'::text, 'Sao Caetano do Sul'::text, 'Santos'::text])) AND
                                CASE
                                    WHEN uag.cidade_acesso = ALL (ARRAY['Sao Roque'::text]) THEN "substring"(uag.periodo_data_hora::text, 1, 8)::date >= '2023-03-05'::date
                                    ELSE "substring"(uag.periodo_data_hora::text, 1, 8)::date >= '2019-01-01'::date
                                END
                          GROUP BY uag.usuario_id
                        ), primeira_data_prod_trilha AS (
                         SELECT tcac.usuario_id,
                            min(tcac.criacao_data)::date AS data_primeiro_evento_prod_trilha,
                            max(tcac.atualizacao_data)::date AS data_ultimo_evento_prod_trilha,
                            count(DISTINCT tcac.criacao_data::date) AS dias_ativo_trilha
                           FROM impulso_previne.trilha_conteudo_avaliacao_conclusao tcac
                          GROUP BY tcac.usuario_id
                        )
                 SELECT uip.id_usuario AS usuario_id,
                    u.nome_usuario,
                    u.mail AS email_usuario,
                    uip.municipio,
                    uip.cargo,
                    uip.criacao_data::date AS data_criacao_cadastro,
                    date_trunc('WEEK'::text, uip.criacao_data::date::timestamp with time zone)::date AS semana_criacao_cadastro,
                    date_trunc('MONTH'::text, uip.criacao_data::date::timestamp with time zone)::date AS mes_criacao_cadastro,
                    LEAST(ua.data_primeiro_acesso, ut.data_primeiro_evento_prod_trilha) AS data_primeiro_acesso,
                    date_trunc('WEEK'::text, LEAST(ua.data_primeiro_acesso, ut.data_primeiro_evento_prod_trilha)::timestamp with time zone)::date AS semana_primeiro_acesso,
                    (date_trunc('month'::text, LEAST(ua.data_primeiro_acesso, ut.data_primeiro_evento_prod_trilha)::timestamp with time zone)::date + floor((date_part('day'::text, LEAST(ua.data_primeiro_acesso, ut.data_primeiro_evento_prod_trilha)) - 1::double precision) / 15::double precision) * '15 days'::interval)::date AS bisemanal_primeiro_acesso,
                    date_trunc('MONTH'::text, LEAST(ua.data_primeiro_acesso, ut.data_primeiro_evento_prod_trilha)::timestamp with time zone)::date AS mes_primeiro_acesso,
                    LEAST(ua.data_primeiro_acesso_trilha_ga4, ut.data_primeiro_evento_prod_trilha) AS data_primeiro_acesso_trilha_hiperdia,
                    date_trunc('WEEK'::text, LEAST(ua.data_primeiro_acesso_trilha_ga4, ut.data_primeiro_evento_prod_trilha)::timestamp with time zone)::date AS semana_primeiro_acesso_trilha_hiperdia,
                    date_trunc('MONTH'::text, LEAST(ua.data_primeiro_acesso_trilha_ga4, ut.data_primeiro_evento_prod_trilha)::timestamp with time zone)::date AS mes_primeiro_acesso_trilha_hiperdia,
                    CURRENT_DATE - LEAST(ua.data_primeiro_acesso, ut.data_primeiro_evento_prod_trilha) AS dias_desde_primeiro_acesso,
                    round(((CURRENT_DATE - LEAST(ua.data_primeiro_acesso, ut.data_primeiro_evento_prod_trilha)) / 7)::numeric, 0) AS semanas_desde_primeiro_acesso,
                    trunc((LEAST(ua.data_primeiro_acesso, ut.data_primeiro_evento_prod_trilha) - uip.criacao_data::date)::double precision) AS dias_entre_cadastro_e_primeiro_acesso,
                    GREATEST(ua.data_ultimo_acesso, ut.data_ultimo_evento_prod_trilha) AS data_ultimo_acesso,
                    date_trunc('WEEK'::text, GREATEST(ua.data_ultimo_acesso, ut.data_ultimo_evento_prod_trilha)::timestamp with time zone)::date AS semana_ultimo_acesso,
                    date_trunc('MONTH'::text, GREATEST(ua.data_ultimo_acesso, ut.data_ultimo_evento_prod_trilha)::timestamp with time zone)::date AS mes_ultimo_acesso,
                    CURRENT_DATE - GREATEST(ua.data_ultimo_acesso, ut.data_ultimo_evento_prod_trilha) AS dias_desde_ultimo_acesso,
                    round(((CURRENT_DATE - GREATEST(ua.data_ultimo_acesso, ut.data_ultimo_evento_prod_trilha)) / 7)::numeric, 0) AS semanas_desde_ultimo_acesso,
                    COALESCE(ua.total_dias_ativo, ut.dias_ativo_trilha) AS total_dias_ativo,
                    ua.total_eventos,
                    ua.total_eventos_ultimos30d,
                    ua.total_sessoes,
                    ua.tempo_total_atividade,
                    ua.data_primeiro_acesso_lista_gestantes,
                    date_trunc('WEEK'::text, ua.data_primeiro_acesso_lista_gestantes::timestamp with time zone)::date AS semana_primeiro_acesso_lista_gestantes,
                    date_trunc('MONTH'::text, ua.data_primeiro_acesso_lista_gestantes::timestamp with time zone)::date AS mes_primeiro_acesso_lista_gestantes,
                    ua.data_primeiro_acesso_lista_diabeticos,
                    date_trunc('WEEK'::text, ua.data_primeiro_acesso_lista_diabeticos::timestamp with time zone)::date AS semana_primeiro_acesso_lista_diabeticos,
                    date_trunc('MONTH'::text, ua.data_primeiro_acesso_lista_diabeticos::timestamp with time zone)::date AS mes_primeiro_acesso_lista_diabeticos,
                    ua.data_primeiro_acesso_lista_hipertensos,
                    date_trunc('WEEK'::text, ua.data_primeiro_acesso_lista_hipertensos::timestamp with time zone)::date AS semana_primeiro_acesso_lista_hipertensos,
                    date_trunc('MONTH'::text, ua.data_primeiro_acesso_lista_hipertensos::timestamp with time zone)::date AS mes_primeiro_acesso_lista_hipertensos,
                    CURRENT_DATE AS criacao_data
                   FROM impulso_previne.usuarios_ip uip
                     LEFT JOIN usuarios_acessos ua ON ua.usuario_id = uip.id_usuario::text
                     LEFT JOIN impulso_previne.usuarios u ON u.id::text = uip.id_usuario::text
                     LEFT JOIN primeira_data_prod_trilha ut ON ut.usuario_id = uip.id_usuario
                  WHERE uip.cargo::text <> 'Impulser'::text
                ), dias_ativo_ga4_trilha AS (
                 SELECT tcac.usuario_id::text AS usuario_id,
                    tcac.criacao_data::date AS data_ativo,
                    count(DISTINCT tcac.codigo_conteudo) AS eventos,
                    max(0) AS sessoes,
                    max(0) AS tempo_atividade,
                    false AS acessou_lista_gestantes,
                    false AS acessou_lista_diabeticos,
                    false AS acessou_lista_hipertensos
                   FROM impulso_previne.trilha_conteudo_avaliacao_conclusao tcac
                  GROUP BY (tcac.usuario_id::text), (tcac.criacao_data::date), (date_trunc('WEEK'::text, tcac.criacao_data::date::timestamp with time zone)::date), (date_trunc('MONTH'::text, tcac.criacao_data::date::timestamp with time zone)::date)
                UNION ALL
                 SELECT ua.usuario_id,
                    "substring"(ua.periodo_data_hora::text, 1, 8)::date AS data_ativo,
                    sum(ua.eventos) AS eventos,
                    sum(ua.sessoes) AS sessoes,
                    sum(ua.sessao_duracao_media * ua.sessoes::double precision) AS tempo_atividade,
                    count(
                        CASE
                            WHEN ua.pagina_path = '/busca-ativa'::text AND "substring"(ua.periodo_data_hora::text, 1, 8)::date <= '2023-03-27'::date OR ua.pagina_path = '/busca-ativa/gestantes'::text THEN 1
                            ELSE NULL::integer
                        END) > 0 AS acessou_lista_gestantes,
                    count(
                        CASE
                            WHEN ua.pagina_path = '/busca-ativa/diabeticos'::text THEN 1
                            ELSE NULL::integer
                        END) > 0 AS acessou_lista_diabeticos,
                    count(
                        CASE
                            WHEN ua.pagina_path = '/busca-ativa/hipertensos'::text THEN 1
                            ELSE NULL::integer
                        END) > 0 AS acessou_lista_hipertensos
                   FROM impulso_previne.usuarios_acessos_ga4_ajustada ua
                  WHERE ua.usuario_id <> '(not set)'::text AND ua.usuarios_ativos > 0 AND (ua.cidade_acesso <> ALL (ARRAY['Sao Paulo'::text, 'Santo Andre'::text, 'Rio de Janeiro'::text, 'Brasilia'::text, 'Praia Grande'::text, 'Ribeirao Preto'::text, 'Santos'::text, 'Sao Bernardo do Campo'::text, 'Sao Caetano do Sul'::text, 'Santos'::text])) AND
                        CASE
                            WHEN ua.cidade_acesso = ALL (ARRAY['Sao Roque'::text]) THEN "substring"(ua.periodo_data_hora::text, 1, 8)::date >= '2023-03-05'::date
                            ELSE "substring"(ua.periodo_data_hora::text, 1, 8)::date >= '2019-01-01'::date
                        END
                  GROUP BY ua.usuario_id, ("substring"(ua.periodo_data_hora::text, 1, 8)::date), (date_trunc('WEEK'::text, "substring"(ua.periodo_data_hora::text, 1, 8)::date::timestamp with time zone)::date), (date_trunc('MONTH'::text, "substring"(ua.periodo_data_hora::text, 1, 8)::date::timestamp with time zone)::date)
                )
         SELECT du.usuario_id,
            du.nome_usuario,
            du.municipio,
            du.cargo,
            da.data_ativo,
            date_trunc('WEEK'::text, da.data_ativo::timestamp with time zone)::date AS semana_ativo,
            (date_trunc('month'::text, da.data_ativo::timestamp with time zone)::date + LEAST(floor((date_part('day'::text, da.data_ativo) - 1::double precision) / 15::double precision), 1::double precision) * '15 days'::interval)::date AS bisemanal_ativo,
            date_trunc('MONTH'::text, da.data_ativo::timestamp with time zone)::date AS mes_ativo,
            du.data_primeiro_acesso,
            du.semana_primeiro_acesso,
            du.bisemanal_primeiro_acesso,
            du.mes_primeiro_acesso,
            du.dias_desde_primeiro_acesso,
            du.semanas_desde_primeiro_acesso,
            du.dias_entre_cadastro_e_primeiro_acesso,
            du.data_ultimo_acesso,
            du.semana_ultimo_acesso,
            du.mes_ultimo_acesso,
            du.dias_desde_ultimo_acesso,
            du.semanas_desde_ultimo_acesso,
            da.data_ativo - du.data_primeiro_acesso AS dias_entre_primeiro_acesso_e_user_ativo,
            round(((da.data_ativo - du.data_primeiro_acesso) / 7)::numeric, 0) AS semanas_entre_primeiro_acesso_e_user_ativo,
                CASE
                    WHEN da.data_ativo = du.data_primeiro_acesso THEN true
                    ELSE false
                END AS e_primeira_data_de_acesso,
            sum(da.eventos) AS eventos,
            sum(da.sessoes) AS sessoes,
            sum(da.tempo_atividade) AS tempo_atividade,
            bool_or(da.acessou_lista_gestantes) AS acessou_lista_gestantes,
            bool_or(da.acessou_lista_diabeticos) AS acessou_lista_diabeticos,
            bool_or(da.acessou_lista_hipertensos) AS acessou_lista_hipertensos,
            CURRENT_DATE AS criacao_data
           FROM dias_ativo_ga4_trilha da
             JOIN area_logada_funil_cadastros_acessos_usuarios du ON da.usuario_id = du.usuario_id::text
          GROUP BY du.usuario_id, du.nome_usuario, du.municipio, du.cargo, da.data_ativo, (date_trunc('WEEK'::text, da.data_ativo::timestamp with time zone)::date), (date_trunc('MONTH'::text, da.data_ativo::timestamp with time zone)::date), du.data_primeiro_acesso, du.semana_primeiro_acesso, du.bisemanal_primeiro_acesso, du.mes_primeiro_acesso, du.dias_desde_primeiro_acesso, du.semanas_desde_primeiro_acesso, du.dias_entre_cadastro_e_primeiro_acesso, du.data_ultimo_acesso, du.semana_ultimo_acesso, du.mes_ultimo_acesso, du.dias_desde_ultimo_acesso, du.semanas_desde_ultimo_acesso, (da.data_ativo - du.data_primeiro_acesso), (round(((da.data_ativo - du.data_primeiro_acesso) / 7)::numeric, 0))
        ), quinzenas AS (
         SELECT DISTINCT ru.bisemanal_ativo
           FROM area_logada_eventos_atividade_retencao_usuarios ru
        ), municipios AS (
         SELECT DISTINCT ru.municipio
           FROM area_logada_eventos_atividade_retencao_usuarios ru
        ), base_data_municipio AS (
         SELECT quinzenas.bisemanal_ativo AS data_periodo,
            municipios.municipio
           FROM quinzenas,
            municipios
        ), base_atividade AS (
         SELECT 'quinzena'::text AS periodo_referencia,
            bdm.data_periodo,
            bdm.municipio,
            count(DISTINCT
                CASE
                    WHEN ear.e_primeira_data_de_acesso IS TRUE THEN ear.usuario_id
                    ELSE NULL::uuid
                END) AS novos_usuarios,
            count(DISTINCT ear.usuario_id) AS usuarios_ativos,
            count(DISTINCT
                CASE
                    WHEN ear.e_primeira_data_de_acesso IS TRUE AND (ear.cargo::text ~~ '%Equipe%'::text OR ear.cargo::text ~~ '%APS%'::text) THEN ear.usuario_id
                    ELSE NULL::uuid
                END) AS novos_usuarios_coordenacao,
            count(DISTINCT
                CASE
                    WHEN ear.cargo::text ~~ '%Equipe%'::text OR ear.cargo::text ~~ '%APS%'::text THEN ear.usuario_id
                    ELSE NULL::uuid
                END) AS usuarios_ativos_coordenacao
           FROM base_data_municipio bdm
             LEFT JOIN area_logada_eventos_atividade_retencao_usuarios ear ON bdm.data_periodo = ear.bisemanal_ativo AND bdm.municipio::text = ear.municipio::text
          GROUP BY 'quinzena'::text, bdm.data_periodo, bdm.municipio
        ), aux1 AS (
         SELECT ba.periodo_referencia,
            ba.data_periodo,
            ba.municipio,
            ba.novos_usuarios,
            ba.usuarios_ativos,
            sum(ba.novos_usuarios) OVER (PARTITION BY ba.municipio ORDER BY ba.data_periodo ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_usuarios_acc,
            ba.usuarios_ativos::numeric * 1.00 / NULLIF(sum(ba.novos_usuarios) OVER (PARTITION BY ba.municipio ORDER BY ba.data_periodo ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0::numeric) AS perc_usuarios_ativos,
            ba.novos_usuarios_coordenacao,
            ba.usuarios_ativos_coordenacao,
            sum(ba.novos_usuarios_coordenacao) OVER (PARTITION BY ba.municipio ORDER BY ba.data_periodo ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_usuarios_coordenacao_acc,
            ba.usuarios_ativos_coordenacao::numeric * 1.00 / NULLIF(sum(ba.novos_usuarios_coordenacao) OVER (PARTITION BY ba.municipio ORDER BY ba.data_periodo ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), 0::numeric) AS perc_usuarios_ativos_coordenacao,
            first_value(ba.data_periodo) OVER (PARTITION BY ba.municipio ORDER BY ba.usuarios_ativos_coordenacao DESC, ba.data_periodo DESC) AS quizena_max_usuarios_ativos_coordenacao,
            first_value(ba.usuarios_ativos_coordenacao) OVER (PARTITION BY ba.municipio ORDER BY ba.data_periodo DESC) AS ultima_quizena_usuarios_ativos_coordenacao,
            first_value(ba.data_periodo) OVER (PARTITION BY ba.municipio ORDER BY ba.data_periodo DESC) AS ultima_quizena,
            CURRENT_DATE AS criacao_data
           FROM base_atividade ba
        )
 SELECT a.periodo_referencia,
    a.data_periodo,
    a.municipio,
    a.novos_usuarios,
    a.usuarios_ativos,
    a.total_usuarios_acc,
    a.perc_usuarios_ativos,
    a.novos_usuarios_coordenacao,
    a.usuarios_ativos_coordenacao,
    a.total_usuarios_coordenacao_acc,
    a.perc_usuarios_ativos_coordenacao,
    a.quizena_max_usuarios_ativos_coordenacao,
    a.ultima_quizena_usuarios_ativos_coordenacao,
    a.ultima_quizena,
    lag(a.data_periodo) OVER (PARTITION BY a.municipio ORDER BY a.data_periodo) AS quinzena_lw,
    a.criacao_data,
    lag(a.usuarios_ativos_coordenacao) OVER (PARTITION BY a.municipio ORDER BY a.data_periodo) AS usuarios_ativos_coordenacao_lw,
    lag(a.perc_usuarios_ativos_coordenacao) OVER (PARTITION BY a.municipio ORDER BY a.data_periodo) AS perc_usuarios_ativos_coordenacao_lw,
    (a.usuarios_ativos_coordenacao - lag(a.usuarios_ativos_coordenacao) OVER (PARTITION BY a.municipio ORDER BY a.data_periodo))::numeric * 1.00 / NULLIF(lag(a.usuarios_ativos_coordenacao) OVER (PARTITION BY a.municipio ORDER BY a.data_periodo), 0)::numeric AS var_ultima_quizena_usuarios_ativos_coordenacao,
    max(a.usuarios_ativos_coordenacao) OVER (PARTITION BY a.municipio) AS max_usuarios_ativos_coordenacao,
    max(
        CASE
            WHEN a.data_periodo >= (date_trunc('month'::text, CURRENT_DATE::timestamp with time zone)::date - '3 mons'::interval) THEN a.usuarios_ativos_coordenacao
            ELSE NULL::bigint
        END) OVER (PARTITION BY a.municipio) AS max_usuarios_ativos_coordenacao_ultimos3meses,
    (a.usuarios_ativos_coordenacao - max(
        CASE
            WHEN a.data_periodo >= (date_trunc('month'::text, CURRENT_DATE::timestamp with time zone)::date - '3 mons'::interval) THEN a.usuarios_ativos_coordenacao
            ELSE NULL::bigint
        END) OVER (PARTITION BY a.municipio))::numeric * 1.00 / NULLIF(max(
        CASE
            WHEN a.data_periodo >= (date_trunc('month'::text, CURRENT_DATE::timestamp with time zone)::date - '3 mons'::interval) THEN a.usuarios_ativos_coordenacao
            ELSE NULL::bigint
        END) OVER (PARTITION BY a.municipio), 0)::numeric AS var_max_usuarios_ativos_coordenacao,
    max(a.perc_usuarios_ativos_coordenacao) OVER (PARTITION BY a.municipio) AS max_perc_usuarios_ativos_coordenacao,
    max(
        CASE
            WHEN a.data_periodo >= (date_trunc('month'::text, CURRENT_DATE::timestamp with time zone)::date - '3 mons'::interval) THEN a.perc_usuarios_ativos_coordenacao
            ELSE NULL::numeric
        END) OVER (PARTITION BY a.municipio) AS max_perc_usuarios_ativos_coordenacao_ultimos3meses,
    avg(
        CASE
            WHEN a.data_periodo >= (date_trunc('month'::text, CURRENT_DATE::timestamp with time zone)::date - '3 mons'::interval) THEN a.perc_usuarios_ativos_coordenacao
            ELSE NULL::numeric
        END) OVER (PARTITION BY a.municipio) AS avg_perc_usuarios_ativos_coordenacao_ultimos3meses,
        CASE
            WHEN a.data_periodo = (date_trunc('month'::text, CURRENT_DATE::timestamp with time zone)::date + floor((date_part('day'::text, CURRENT_DATE) - 1::double precision) / 15::double precision) * '15 days'::interval)::date THEN true
            ELSE false
        END AS e_ultima_quinzena
   FROM aux1 a
WITH DATA;

-- View indexes:
CREATE INDEX area_logada_usuarios_ativos_bisemanal_municipio_idx ON impulso_previne.area_logada_usuarios_ativos_bisemanal_municipio USING btree (municipio, data_periodo);