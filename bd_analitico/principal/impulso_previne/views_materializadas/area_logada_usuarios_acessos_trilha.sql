-- impulso_previne.area_logada_usuarios_acessos_trilha source

CREATE MATERIALIZED VIEW impulso_previne.area_logada_usuarios_acessos_trilha
TABLESPACE pg_default
AS WITH data_conteudos_trilha AS (
         SELECT tcac.usuario_id,
            min(
                CASE
                    WHEN (tcac.codigo_conteudo::text <> ANY (ARRAY['HD-MOD0-C0'::character varying::text])) AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_primeiro_conteudo_concluido,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD0-C0'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod0_c0,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD0-C1'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod0_c1,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD1-C1'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod1_c1,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD1-C2'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod1_c2,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD1-C3'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod1_c3,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD1-C4'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod1_c4,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD2-C1'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod2_c1,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD2-C2'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod2_c2,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD2-C3'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod2_c3,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD2-C4'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod2_c4,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD2-C5'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod2_c5,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD2-C6'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod2_c6,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD2-C7'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod2_c7,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD2-C8'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod2_c8,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD2-C9'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod2_c9,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD3-C1'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod3_c1,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD3-C2'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod3_c2,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD3-C3'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod3_c3,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD3-C4'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod3_c4,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD3-C5'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod3_c5,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD3-C6'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod3_c6,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD3-C7'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod3_c7,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD3-C8'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod3_c8,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD3-C9'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod3_c9,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD3-C10'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod3_c10,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD4-C1'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod4_c1,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD4-C2'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod4_c2,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD4-C3'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod4_c3,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD4-C4'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod4_c4,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD5-C1'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod5_c1,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD5-C2'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod5_c2,
            max(
                CASE
                    WHEN tcac.codigo_conteudo::text = 'HD-MOD5-C3'::text AND tcac.concluido IS TRUE THEN tcac.criacao_data::date
                    ELSE NULL::date
                END) AS data_conclusao_mod5_c3,
            min(tcac.criacao_data)::date AS data_primeiro_evento_prod_trilha,
            max(tcac.atualizacao_data)::date AS data_ultimo_evento_prod_trilha,
            count(DISTINCT tcac.criacao_data::date) AS dias_ativo_trilha,
            count(DISTINCT tcac.codigo_conteudo) AS conteudos_concluidos_ou_avaliados,
            count(DISTINCT
                CASE
                    WHEN tcac.concluido IS TRUE THEN tcac.codigo_conteudo
                    ELSE NULL::character varying
                END) AS conteudos_concluidos,
            count(DISTINCT
                CASE
                    WHEN tcac.avaliacao IS NOT NULL THEN tcac.codigo_conteudo
                    ELSE NULL::character varying
                END) AS conteudos_avaliados,
            avg(tcac.avaliacao) AS media_avaliacao_conteudos
           FROM impulso_previne.trilha_conteudo_avaliacao_conclusao tcac
          GROUP BY tcac.usuario_id
        ), dados_usuarios_ga4_trilha AS (
         SELECT uag.usuario_id,
            min("substring"(uag.periodo_data_hora::text, 1, 8)::date) AS data_primeiro_acesso,
            min(
                CASE
                    WHEN uag.pagina_path = ANY (ARRAY['/conteudo-programatico'::text, '/capacitacao'::text, '/capacitacoes'::text, '/conteudo'::text, '/duvidas'::text, '/grupo-whatsapp'::text]) THEN "substring"(uag.periodo_data_hora::text, 1, 8)::date
                    ELSE NULL::date
                END) AS data_primeiro_acesso_trilha_ga4,
            min(
                CASE
                    WHEN uag.pagina_path = '/capacitacoes'::text THEN "substring"(uag.periodo_data_hora::text, 1, 8)::date
                    ELSE NULL::date
                END) AS data_primeiro_acesso_entrada_trilha,
            min(
                CASE
                    WHEN uag.pagina_path = '/conteudo-programatico'::text THEN "substring"(uag.periodo_data_hora::text, 1, 8)::date
                    ELSE NULL::date
                END) AS data_primeiro_acesso_intro_modulo_trilha_hiperdia,
            min(
                CASE
                    WHEN uag.pagina_path = '/conteudo'::text THEN "substring"(uag.periodo_data_hora::text, 1, 8)::date
                    ELSE NULL::date
                END) AS data_primeiro_acesso_conteudos_trilha_hiperdia,
            min(
                CASE
                    WHEN uag.pagina_path = '/duvidas'::text THEN "substring"(uag.periodo_data_hora::text, 1, 8)::date
                    ELSE NULL::date
                END) AS data_primeiro_acesso_duvidas_trilha_hiperdia,
            min(
                CASE
                    WHEN uag.pagina_path = '/grupo-whatsapp'::text THEN "substring"(uag.periodo_data_hora::text, 1, 8)::date
                    ELSE NULL::date
                END) AS data_primeiro_acesso_whatsapp_trilha_hiperdia,
            sum(
                CASE
                    WHEN uag.pagina_path = ANY (ARRAY['/conteudo-programatico'::text, '/capacitacao'::text, '/capacitacoes'::text, '/conteudo'::text, '/duvidas'::text, '/grupo-whatsapp'::text]) THEN uag.eventos
                    ELSE 0
                END) AS total_eventos_trilha,
            sum(
                CASE
                    WHEN "substring"(uag.periodo_data_hora::text, 1, 8)::date > (CURRENT_DATE - '30 days'::interval) AND (uag.pagina_path = ANY (ARRAY['/conteudo-programatico'::text, '/capacitacao'::text, '/capacitacoes'::text, '/conteudo'::text, '/duvidas'::text, '/grupo-whatsapp'::text])) THEN uag.eventos
                    ELSE 0
                END) AS total_eventos_ultimos30d_trilha,
            sum(
                CASE
                    WHEN uag.pagina_path = ANY (ARRAY['/conteudo-programatico'::text, '/capacitacao'::text, '/capacitacoes'::text, '/conteudo'::text, '/duvidas'::text, '/grupo-whatsapp'::text]) THEN uag.sessoes
                    ELSE 0
                END) AS total_sessoes_trilha,
            sum(
                CASE
                    WHEN uag.pagina_path = ANY (ARRAY['/conteudo-programatico'::text, '/capacitacao'::text, '/capacitacoes'::text, '/conteudo'::text, '/duvidas'::text, '/grupo-whatsapp'::text]) THEN uag.sessao_duracao_media * uag.sessoes::double precision
                    ELSE 0::bigint::double precision
                END) AS tempo_total_atividade_trilha,
            count(DISTINCT
                CASE
                    WHEN uag.pagina_path = ANY (ARRAY['/conteudo-programatico'::text, '/capacitacao'::text, '/capacitacoes'::text, '/conteudo'::text, '/duvidas'::text, '/grupo-whatsapp'::text]) THEN "substring"(uag.periodo_data_hora::text, 1, 8)::date
                    ELSE NULL::date
                END) AS dias_ativo_trilha_hiperdia
           FROM impulso_previne.usuarios_acessos_ga4_ajustada uag
          WHERE 1 = 1 AND uag.usuarios_ativos > 0 AND uag.usuario_id <> '(not set)'::text AND (uag.cidade_acesso <> ALL (ARRAY['Sao Paulo'::text, 'Santo Andre'::text, 'Rio de Janeiro'::text, 'Brasilia'::text, 'Praia Grande'::text, 'Ribeirao Preto'::text, 'Santos'::text, 'Sao Bernardo do Campo'::text, 'Sao Caetano do Sul'::text, 'Santos'::text])) AND
                CASE
                    WHEN uag.cidade_acesso = ALL (ARRAY['Sao Roque'::text]) THEN "substring"(uag.periodo_data_hora::text, 1, 8)::date >= '2023-03-05'::date
                    ELSE "substring"(uag.periodo_data_hora::text, 1, 8)::date >= '2019-01-01'::date
                END
          GROUP BY uag.usuario_id
        )
 SELECT ui.id_usuario,
    u.nome_usuario,
    u.mail AS email_usuario,
    ui.municipio,
    ui.cargo,
    ui.criacao_data::date AS data_criacao_cadastro,
    LEAST(dct.data_primeiro_evento_prod_trilha, dugt.data_primeiro_acesso_trilha_ga4) AS data_primeiro_acesso_trilha_hiperdia,
    date_trunc('WEEK'::text, LEAST(dct.data_primeiro_evento_prod_trilha, dugt.data_primeiro_acesso_trilha_ga4)::timestamp with time zone)::date AS semana_primeiro_acesso_trilha_hiperdia,
    date_trunc('MONTH'::text, LEAST(dct.data_primeiro_evento_prod_trilha, dugt.data_primeiro_acesso_trilha_ga4)::timestamp with time zone)::date AS mes_primeiro_acesso_trilha_hiperdia,
    dct.data_primeiro_conteudo_concluido,
        CASE
            WHEN dct.data_conclusao_mod0_c0 IS NOT NULL AND dct.data_conclusao_mod0_c1 IS NOT NULL THEN GREATEST(dct.data_conclusao_mod0_c0, dct.data_conclusao_mod0_c1)
            ELSE NULL::date
        END AS data_conclusao_mod0,
        CASE
            WHEN dct.data_conclusao_mod1_c1 IS NOT NULL AND dct.data_conclusao_mod1_c2 IS NOT NULL AND dct.data_conclusao_mod1_c3 IS NOT NULL AND dct.data_conclusao_mod1_c4 IS NOT NULL THEN GREATEST(dct.data_conclusao_mod1_c1, dct.data_conclusao_mod1_c2, dct.data_conclusao_mod1_c3, dct.data_conclusao_mod1_c4)
            ELSE NULL::date
        END AS data_conclusao_mod1,
        CASE
            WHEN dct.data_conclusao_mod2_c1 IS NOT NULL AND dct.data_conclusao_mod2_c2 IS NOT NULL AND dct.data_conclusao_mod2_c3 IS NOT NULL AND dct.data_conclusao_mod2_c4 IS NOT NULL AND dct.data_conclusao_mod2_c5 IS NOT NULL AND dct.data_conclusao_mod2_c6 IS NOT NULL AND dct.data_conclusao_mod2_c7 IS NOT NULL AND dct.data_conclusao_mod2_c8 IS NOT NULL AND dct.data_conclusao_mod2_c9 IS NOT NULL THEN GREATEST(dct.data_conclusao_mod2_c1, dct.data_conclusao_mod2_c2, dct.data_conclusao_mod2_c3, dct.data_conclusao_mod2_c4, dct.data_conclusao_mod2_c5, dct.data_conclusao_mod2_c6, dct.data_conclusao_mod2_c7, dct.data_conclusao_mod2_c8, dct.data_conclusao_mod2_c9)
            ELSE NULL::date
        END AS data_conclusao_mod2,
        CASE
            WHEN dct.data_conclusao_mod3_c1 IS NOT NULL AND dct.data_conclusao_mod3_c2 IS NOT NULL AND dct.data_conclusao_mod3_c3 IS NOT NULL AND dct.data_conclusao_mod3_c4 IS NOT NULL AND dct.data_conclusao_mod3_c5 IS NOT NULL AND dct.data_conclusao_mod3_c6 IS NOT NULL AND dct.data_conclusao_mod3_c7 IS NOT NULL AND dct.data_conclusao_mod3_c8 IS NOT NULL AND dct.data_conclusao_mod3_c9 IS NOT NULL AND dct.data_conclusao_mod3_c10 IS NOT NULL THEN GREATEST(dct.data_conclusao_mod3_c1, dct.data_conclusao_mod3_c2, dct.data_conclusao_mod3_c3, dct.data_conclusao_mod3_c4, dct.data_conclusao_mod3_c5, dct.data_conclusao_mod3_c6, dct.data_conclusao_mod3_c7, dct.data_conclusao_mod3_c8, dct.data_conclusao_mod3_c9, dct.data_conclusao_mod3_c10)
            ELSE NULL::date
        END AS data_conclusao_mod3,
        CASE
            WHEN dct.data_conclusao_mod4_c1 IS NOT NULL AND dct.data_conclusao_mod4_c2 IS NOT NULL AND dct.data_conclusao_mod4_c3 IS NOT NULL AND dct.data_conclusao_mod4_c4 IS NOT NULL THEN GREATEST(dct.data_conclusao_mod4_c1, dct.data_conclusao_mod4_c2, dct.data_conclusao_mod4_c3, dct.data_conclusao_mod4_c4)
            ELSE NULL::date
        END AS data_conclusao_mod4,
        CASE
            WHEN dct.data_conclusao_mod5_c1 IS NOT NULL AND dct.data_conclusao_mod5_c2 IS NOT NULL AND dct.data_conclusao_mod5_c3 IS NOT NULL THEN GREATEST(dct.data_conclusao_mod5_c1, dct.data_conclusao_mod5_c2, dct.data_conclusao_mod5_c3)
            ELSE NULL::date
        END AS data_conclusao_mod5,
        CASE
            WHEN dct.data_conclusao_mod0_c0 IS NOT NULL AND dct.data_conclusao_mod0_c1 IS NOT NULL AND dct.data_conclusao_mod1_c1 IS NOT NULL AND dct.data_conclusao_mod1_c2 IS NOT NULL AND dct.data_conclusao_mod1_c3 IS NOT NULL AND dct.data_conclusao_mod1_c4 IS NOT NULL AND dct.data_conclusao_mod2_c1 IS NOT NULL AND dct.data_conclusao_mod2_c2 IS NOT NULL AND dct.data_conclusao_mod2_c3 IS NOT NULL AND dct.data_conclusao_mod2_c4 IS NOT NULL AND dct.data_conclusao_mod2_c5 IS NOT NULL AND dct.data_conclusao_mod2_c6 IS NOT NULL AND dct.data_conclusao_mod2_c7 IS NOT NULL AND dct.data_conclusao_mod2_c8 IS NOT NULL AND dct.data_conclusao_mod2_c9 IS NOT NULL AND dct.data_conclusao_mod3_c1 IS NOT NULL AND dct.data_conclusao_mod3_c2 IS NOT NULL AND dct.data_conclusao_mod3_c3 IS NOT NULL AND dct.data_conclusao_mod3_c4 IS NOT NULL AND dct.data_conclusao_mod3_c5 IS NOT NULL AND dct.data_conclusao_mod3_c6 IS NOT NULL AND dct.data_conclusao_mod3_c7 IS NOT NULL AND dct.data_conclusao_mod3_c8 IS NOT NULL AND dct.data_conclusao_mod3_c9 IS NOT NULL AND dct.data_conclusao_mod3_c10 IS NOT NULL AND dct.data_conclusao_mod4_c1 IS NOT NULL AND dct.data_conclusao_mod4_c2 IS NOT NULL AND dct.data_conclusao_mod4_c3 IS NOT NULL AND dct.data_conclusao_mod4_c4 IS NOT NULL AND dct.data_conclusao_mod5_c1 IS NOT NULL AND dct.data_conclusao_mod5_c2 IS NOT NULL AND dct.data_conclusao_mod5_c3 IS NOT NULL THEN GREATEST(dct.data_conclusao_mod0_c0, dct.data_conclusao_mod0_c1, dct.data_conclusao_mod1_c1, dct.data_conclusao_mod1_c2, dct.data_conclusao_mod1_c3, dct.data_conclusao_mod1_c4, dct.data_conclusao_mod2_c1, dct.data_conclusao_mod2_c2, dct.data_conclusao_mod2_c3, dct.data_conclusao_mod2_c4, dct.data_conclusao_mod2_c5, dct.data_conclusao_mod2_c6, dct.data_conclusao_mod2_c7, dct.data_conclusao_mod2_c8, dct.data_conclusao_mod2_c9, dct.data_conclusao_mod3_c1, dct.data_conclusao_mod3_c2, dct.data_conclusao_mod3_c3, dct.data_conclusao_mod3_c4, dct.data_conclusao_mod3_c5, dct.data_conclusao_mod3_c6, dct.data_conclusao_mod3_c7, dct.data_conclusao_mod3_c8, dct.data_conclusao_mod3_c9, dct.data_conclusao_mod3_c10, dct.data_conclusao_mod4_c1, dct.data_conclusao_mod4_c2, dct.data_conclusao_mod4_c3, dct.data_conclusao_mod4_c4, dct.data_conclusao_mod5_c1, dct.data_conclusao_mod5_c2, dct.data_conclusao_mod5_c3)
            ELSE NULL::date
        END AS data_conclusao_trilha,
    dct.data_conclusao_mod0_c0,
    dct.data_conclusao_mod0_c1,
    dct.data_conclusao_mod1_c1,
    dct.data_conclusao_mod1_c2,
    dct.data_conclusao_mod1_c3,
    dct.data_conclusao_mod1_c4,
    dct.data_conclusao_mod2_c1,
    dct.data_conclusao_mod2_c2,
    dct.data_conclusao_mod2_c3,
    dct.data_conclusao_mod2_c4,
    dct.data_conclusao_mod2_c5,
    dct.data_conclusao_mod2_c6,
    dct.data_conclusao_mod2_c7,
    dct.data_conclusao_mod2_c8,
    dct.data_conclusao_mod2_c9,
    dct.data_conclusao_mod3_c1,
    dct.data_conclusao_mod3_c2,
    dct.data_conclusao_mod3_c3,
    dct.data_conclusao_mod3_c4,
    dct.data_conclusao_mod3_c5,
    dct.data_conclusao_mod3_c6,
    dct.data_conclusao_mod3_c7,
    dct.data_conclusao_mod3_c8,
    dct.data_conclusao_mod3_c9,
    dct.data_conclusao_mod3_c10,
    dct.data_conclusao_mod4_c1,
    dct.data_conclusao_mod4_c2,
    dct.data_conclusao_mod4_c3,
    dct.data_conclusao_mod4_c4,
    dct.data_conclusao_mod5_c1,
    dct.data_conclusao_mod5_c2,
    dct.data_conclusao_mod5_c3,
    LEAST(dugt.data_primeiro_acesso, dct.data_primeiro_evento_prod_trilha) AS data_primeiro_acesso,
    dugt.data_primeiro_acesso_whatsapp_trilha_hiperdia,
    dugt.data_primeiro_acesso_duvidas_trilha_hiperdia,
    dugt.total_eventos_trilha,
    dugt.total_eventos_ultimos30d_trilha,
    dugt.total_sessoes_trilha,
    dugt.tempo_total_atividade_trilha,
    COALESCE(dugt.dias_ativo_trilha_hiperdia, dct.dias_ativo_trilha) AS dias_ativo_trilha_hiperdia,
    dct.conteudos_concluidos_ou_avaliados,
    dct.conteudos_concluidos,
        CASE
            WHEN dct.data_conclusao_mod0_c0 IS NOT NULL AND dct.data_conclusao_mod0_c1 IS NOT NULL THEN 1
            ELSE 0
        END +
        CASE
            WHEN dct.data_conclusao_mod1_c1 IS NOT NULL AND dct.data_conclusao_mod1_c2 IS NOT NULL AND dct.data_conclusao_mod1_c3 IS NOT NULL AND dct.data_conclusao_mod1_c4 IS NOT NULL THEN 1
            ELSE 0
        END +
        CASE
            WHEN dct.data_conclusao_mod2_c1 IS NOT NULL AND dct.data_conclusao_mod2_c2 IS NOT NULL AND dct.data_conclusao_mod2_c3 IS NOT NULL AND dct.data_conclusao_mod2_c4 IS NOT NULL AND dct.data_conclusao_mod2_c5 IS NOT NULL AND dct.data_conclusao_mod2_c6 IS NOT NULL AND dct.data_conclusao_mod2_c7 IS NOT NULL AND dct.data_conclusao_mod2_c8 IS NOT NULL AND dct.data_conclusao_mod2_c9 IS NOT NULL THEN 1
            ELSE 0
        END +
        CASE
            WHEN dct.data_conclusao_mod3_c1 IS NOT NULL AND dct.data_conclusao_mod3_c2 IS NOT NULL AND dct.data_conclusao_mod3_c3 IS NOT NULL AND dct.data_conclusao_mod3_c4 IS NOT NULL AND dct.data_conclusao_mod3_c5 IS NOT NULL AND dct.data_conclusao_mod3_c6 IS NOT NULL AND dct.data_conclusao_mod3_c7 IS NOT NULL AND dct.data_conclusao_mod3_c8 IS NOT NULL AND dct.data_conclusao_mod3_c9 IS NOT NULL AND dct.data_conclusao_mod3_c10 IS NOT NULL THEN 1
            ELSE 0
        END +
        CASE
            WHEN dct.data_conclusao_mod4_c1 IS NOT NULL AND dct.data_conclusao_mod4_c2 IS NOT NULL AND dct.data_conclusao_mod4_c3 IS NOT NULL AND dct.data_conclusao_mod4_c4 IS NOT NULL THEN 1
            ELSE 0
        END +
        CASE
            WHEN dct.data_conclusao_mod5_c1 IS NOT NULL AND dct.data_conclusao_mod5_c2 IS NOT NULL AND dct.data_conclusao_mod5_c3 IS NOT NULL THEN 1
            ELSE 0
        END AS modulos_concluidos,
    dct.conteudos_avaliados,
    dct.media_avaliacao_conteudos,
    CURRENT_DATE AS criacao_data
   FROM impulso_previne.usuarios_ip ui
     LEFT JOIN impulso_previne.usuarios u ON u.id = ui.id_usuario
     LEFT JOIN data_conteudos_trilha dct ON ui.id_usuario = dct.usuario_id
     LEFT JOIN dados_usuarios_ga4_trilha dugt ON dugt.usuario_id = ui.id_usuario::text
  WHERE ui.cargo::text <> 'Impulser'::text
WITH DATA;

-- View indexes:
CREATE INDEX area_logada_usuarios_acessos_trilha_id_usuario_idx ON impulso_previne.area_logada_usuarios_acessos_trilha USING btree (id_usuario);