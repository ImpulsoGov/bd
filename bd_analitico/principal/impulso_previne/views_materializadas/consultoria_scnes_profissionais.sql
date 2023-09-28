-- impulso_previne.consultoria_scnes_profissionais source

CREATE MATERIALIZED VIEW impulso_previne.consultoria_scnes_profissionais
TABLESPACE pg_default
AS WITH cnes AS (
         SELECT "substring"(p.data_inicio::text, 1, 10)::date AS ultima_competencia,
            concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
            prof.municipio_id_sus,
            prof.equipe_id_ine,
            prof.profissional_nome,
            prof.profissional_cns,
            prof.profissional_cbo,
            prof.profissional_ocupacao,
            prof.periodo_data_entrada,
            prof.periodo_data_desligamento,
            equipes.equipe_nome,
            equipes.equipe_tipo,
            equipes.estabelecimento_cnes_id,
            estab.estabelecimento_nome,
            estab.estabelecimento_natureza_juridica,
            estab.estabelecimento_tipo,
            row_number() OVER (PARTITION BY equipes.municipio_id_sus, (concat(m.nome, ' - ', m.uf_sigla)), equipes.equipe_id_ine ORDER BY p.data_inicio DESC) = 1 AS ultima_atualizacao_equipe
           FROM dados_publicos.scnes_profissionais_com_ine prof
             LEFT JOIN dados_publicos.scnes_equipes equipes ON equipes.equipe_id_ine::text = prof.equipe_id_ine::text AND equipes.estabelecimento_cnes_id = prof.estabelecimento_cnes_id::bpchar AND equipes.municipio_id_sus = prof.municipio_id_sus
             LEFT JOIN dados_publicos.scnes_estabelecimentos_identificados estab ON estab.estabelecimento_cnes_id = prof.estabelecimento_cnes_id::bpchar AND estab.municipio_id_sus = prof.municipio_id_sus
             LEFT JOIN listas_de_codigos.municipios m ON prof.municipio_id_sus::bpchar = m.id_sus
             LEFT JOIN listas_de_codigos.periodos p ON p.id = prof.periodo_id
          WHERE (equipes.equipe_tipo = ANY (ARRAY['ESF1      - ESTRATEGIA DE SAUDE DA FAMILIA TIPO I'::text, 'ESFF - EQUIPE DE SAUDE DA FAMILIA FLUVIAL'::text, 'ESF - EQUIPE DE SAUDE DA FAMILIA'::text, 'ESF4      - ESTRATEGIA DE SAUDE DA FAMILIA TIPO IV'::text, 'ESFR - EQUIPE DE SAUDE DA FAMILIA RIBEIRINHA'::text, 'eCR MIII - EQUIPE DOS CONSULTORIOS NA RUA MODALIDADE III'::text, 'eCR MII  - EQUIPE DOS CONSULTORIOS NA RUA MODALIDADE II'::text, 'eCR MI   - EQUIPE DOS CONSULTORIOS NA RUA MODALIDADE I'::text, 'ECR - EQUIPE DOS CONSULTORIOS NA RUA'::text, 'EAPP - EQUIPE DE ATENCAO PRIMARIA PRISIONAL'::text, 'EABP3   - EQ ATENCAO BASICA PRISIONAL TIPO III'::text, 'EABP2SM - EQ ATENCAO BASICA PRISIONAL TIPO II C SAUDE MENTAL'::text, 'EABP1   - EQ ATENCAO BASICA PRISIONAL TIPO I'::text, 'EABP2   - EQ ATENCAO BASICA PRISIONAL TIPO II'::text, 'EABP1SM - EQ ATENCAO BASICA PRISIONAL TIPO I C SAUDE MENTAL'::text, 'EAP - EQUIPE DE ATENCAO PRIMARIA'::text])) AND (prof.profissional_ocupacao = ANY (ARRAY['ENFERMEIRO'::text, 'ENFERMEIRO DA ESTRATEGIA DE SAUDE DA FAMILIA'::text, 'GERENTE DE SERVICOS DE SAUDE'::text])) AND (estab.estabelecimento_tipo = ANY (ARRAY['POSTO DE SAUDE'::text, 'CENTRO DE SAUDE/UNIDADE BASICA'::text, 'CENTRO DE APOIO A SAUDE DA FAMILIA'::text])) AND equipes.periodo_desativacao IS NULL
        )
 SELECT cnes.ultima_competencia,
    cnes.municipio_uf,
    cnes.municipio_id_sus,
    cnes.equipe_id_ine,
    cnes.profissional_nome,
    cnes.profissional_cns,
    cnes.profissional_cbo,
    cnes.profissional_ocupacao,
    cnes.periodo_data_entrada,
    cnes.periodo_data_desligamento,
    cnes.equipe_nome,
    cnes.equipe_tipo,
    cnes.estabelecimento_cnes_id,
    cnes.estabelecimento_nome,
    cnes.estabelecimento_natureza_juridica,
    cnes.estabelecimento_tipo,
    cnes.ultima_atualizacao_equipe
   FROM cnes
  WHERE cnes.ultima_atualizacao_equipe IS TRUE
WITH DATA;