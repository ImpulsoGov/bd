-- impulso_previne.monitoramento_area_logada_usuarios_nao_logados source

CREATE MATERIALIZED VIEW impulso_previne.monitoramento_area_logada_usuarios_nao_logados
TABLESPACE pg_default
AS SELECT res.id_usuario AS usuario_id,
    u.nome_usuario,
    res.municipio AS usuario_municipio,
    res.cargo AS usuario_cargo,
    res.equipe AS usuario_equipe_ine,
    res.telefone,
    res.whatsapp
   FROM ( SELECT tb1.id_usuario,
            tb1.municipio,
            tb1.cargo,
            tb1.equipe,
            tb1.telefone,
            tb1.whatsapp,
            ( SELECT sum(tb2.usuarios_ativos) > 0
                   FROM impulso_previne.monitoramento_area_logada tb2
                  WHERE tb2.usuario_id <> '(not set)'::text AND tb2.usuario_cargo <> 'Impulser'::text AND tb2.usuario_id = tb1.id_usuario::text AND (tb2.cidade_acesso <> ALL (ARRAY['Sao Paulo'::text, 'Santo Andre'::text, 'Rio de Janeiro'::text, 'Brasilia'::text, 'Praia Grande'::text, 'Ribeirao Preto'::text, 'Santos'::text, 'Sao Bernardo do Campo'::text, 'Sao Caetano do Sul'::text, 'Santos'::text, 'Sao Roque'::text]))) AS usuario_ativo
           FROM impulso_previne.usuarios_ip tb1
          WHERE tb1.cargo::text <> 'Impulser'::text) res
     JOIN impulso_previne.usuarios u ON u.id = res.id_usuario
  WHERE res.usuario_ativo IS NOT TRUE
WITH DATA;

-- View indexes:
CREATE INDEX monitoramento_area_logada_usuarios_nao_logados_usuario_equipe_i ON impulso_previne.monitoramento_area_logada_usuarios_nao_logados USING btree (usuario_equipe_ine);
CREATE INDEX monitoramento_area_logada_usuarios_nao_logados_usuario_municipi ON impulso_previne.monitoramento_area_logada_usuarios_nao_logados USING btree (usuario_municipio, usuario_cargo);