-- impulso_previne.trilha_hiperdia_primeiro_acesso_por_municipio source

CREATE OR REPLACE VIEW impulso_previne.trilha_hiperdia_primeiro_acesso_por_municipio
AS SELECT municipios.id_sus AS municipio_id_sus,
    municipios.nome AS municipio_nome,
    municipios.uf_sigla,
    to_timestamp(min(ga.periodo_data_hora::text), 'YYYYMMDDHH24'::text)::date AS primeiro_acesso
   FROM impulso_previne.usuarios_acessos_ga4 ga
     LEFT JOIN impulso_previne.usuarios usuario ON usuario.id = COALESCE(NULLIF(ga.usuario_id, '(not set)'::text), NULLIF(ga.usuario_id_alternativo, '(not set)'::text))::uuid
     LEFT JOIN impulso_previne.usuarios_ip usuario_detalhes ON usuario.id = usuario_detalhes.id_usuario
     LEFT JOIN listas_de_codigos.municipios ON usuario_detalhes.municipio::text = ((municipios.nome || ' - '::text) || municipios.uf_sigla::text)
  WHERE ga.pagina_url ~~ '%trilhaID=cldxqzjw80okq0bkm2we9n1c%'::text AND municipios.id_sus IS NOT NULL
  GROUP BY municipios.id_sus, municipios.nome, municipios.uf_sigla;