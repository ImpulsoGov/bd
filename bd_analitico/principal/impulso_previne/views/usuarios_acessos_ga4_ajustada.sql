-- impulso_previne.usuarios_acessos_ga4_ajustada source

CREATE OR REPLACE VIEW impulso_previne.usuarios_acessos_ga4_ajustada
AS SELECT ga.id,
    ga.periodo_data_hora,
        CASE
            WHEN ga.usuario_id = '(not set)'::text THEN ga.usuario_id_alternativo
            ELSE ga.usuario_id
        END AS usuario_id,
    ga.cidade_acesso,
    ga.pagina_path,
    ga.pagina_url,
    ga.usuarios_ativos,
    ga.eventos,
    ga.sessoes,
    ga.sessoes_engajadas,
    ga.sessao_duracao,
    ga.sessao_duracao_media,
    ga.data_inicio_relatorio,
    ga.criacao_data,
    ga.atualizacao_data
   FROM impulso_previne.usuarios_acessos_ga4 ga;