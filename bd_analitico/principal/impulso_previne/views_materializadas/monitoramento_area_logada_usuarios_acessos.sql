-- impulso_previne.monitoramento_area_logada_usuarios_acessos source

CREATE MATERIALIZED VIEW impulso_previne.monitoramento_area_logada_usuarios_acessos
TABLESPACE pg_default
AS SELECT "substring"(tb1.periodo_data_hora::text, 1, 8)::date AS periodo_data,
    "substring"(tb1.periodo_data_hora::text, 9, 10)::integer AS periodo_hora,
    tb1.usuario_id,
    tb3.nome_usuario AS usuario_nome,
    ( SELECT count(DISTINCT usuarios_ip.id_usuario) AS count
           FROM impulso_previne.usuarios_ip
          WHERE usuarios_ip.cargo::text <> 'Impulser'::text AND usuarios_ip.municipio::text = tb1.usuario_municipio AND usuarios_ip.cargo::text = tb1.usuario_cargo) AS usuarios_cadastrados,
    tb2.telefone,
    tb2.whatsapp,
    tb1.usuario_municipio,
    ( SELECT count(DISTINCT usuarios_ip.municipio) AS count
           FROM impulso_previne.usuarios_ip
          WHERE usuarios_ip.cargo::text <> 'Impulser'::text AND usuarios_ip.municipio::text = tb1.usuario_municipio AND usuarios_ip.cargo::text = tb1.usuario_cargo) AS municipios_cadastrados,
    tb2.equipe AS usuario_equipe_ine,
    ( SELECT count(DISTINCT usuarios_ip.equipe) AS count
           FROM impulso_previne.usuarios_ip
          WHERE usuarios_ip.cargo::text <> 'Impulser'::text AND usuarios_ip.municipio::text = tb1.usuario_municipio AND usuarios_ip.cargo::text = tb1.usuario_cargo) AS equipes_cadastrados,
    tb2.cargo AS usuario_cargo,
    tb1.cidade_acesso,
    tb1.pagina_path,
        CASE
            WHEN tb1.usuarios_ativos > 0 THEN true
            ELSE false
        END AS usuario_ativo,
        CASE
            WHEN tb1.novos_usuarios > 0 THEN true
            ELSE false
        END AS novo_usuario,
    tb1.periodo_data_primeira_sessao::date AS periodo_data_primeira_sessao,
    tb1.eventos,
    tb1.sessoes_engajadas,
    tb1.taxa_engajamento,
    tb1.visualizacoes,
    tb1.sessao_duracao,
    tb1.sessao_duracao_media,
    tb1.dau_per_mau,
    tb1.dau_per_wau
   FROM impulso_previne.monitoramento_area_logada tb1
     LEFT JOIN impulso_previne.usuarios_ip tb2 ON tb1.usuario_id = tb2.id_usuario::text
     LEFT JOIN impulso_previne.usuarios tb3 ON tb1.usuario_id = tb3.id::text
  WHERE tb1.usuario_id <> '(not set)'::text AND tb2.cargo::text <> 'Impulser'::text AND (tb1.cidade_acesso <> ALL (ARRAY['Sao Paulo'::text, 'Santo Andre'::text, 'Rio de Janeiro'::text, 'Brasilia'::text, 'Praia Grande'::text, 'Ribeirao Preto'::text, 'Santos'::text, 'Sao Bernardo do Campo'::text, 'Sao Caetano do Sul'::text, 'Santos'::text, 'Sao Roque'::text]))
WITH DATA;

-- View indexes:
CREATE INDEX monitoramento_area_logada_usuarios_acessos_periodo_data_idx ON impulso_previne.monitoramento_area_logada_usuarios_acessos USING btree (periodo_data, usuario_municipio, pagina_path, usuario_cargo);