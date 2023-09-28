-- impulso_previne.area_logada_funil_cadastros_acessos_usuarios source

CREATE MATERIALIZED VIEW impulso_previne.area_logada_funil_cadastros_acessos_usuarios
TABLESPACE pg_default
AS WITH usuarios_acessos AS (
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
WITH DATA;

-- View indexes:
CREATE INDEX area_logada_funil_cadastros_acessos_usuarios_usuario_id_idx ON impulso_previne.area_logada_funil_cadastros_acessos_usuarios USING btree (usuario_id, municipio);