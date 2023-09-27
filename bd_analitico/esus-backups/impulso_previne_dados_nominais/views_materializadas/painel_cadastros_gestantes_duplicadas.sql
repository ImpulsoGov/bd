
CREATE MATERIALIZED VIEW impulso_previne_dados_nominais.painel_cadastros_gestantes_duplicadas
TABLESPACE pg_default
AS WITH rel_duplicadas AS (
         SELECT lista_nominal_gestantes_duplicadas_por_erro_grafia.municipio_id_sus,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.gestante_nome,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.gestante_data_de_nascimento,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.gestante_documento_cpf,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.gestante_documento_cns,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.periodo_data_transmissao,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.gestante_dum,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.gestante_dpp,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.equipe_ine,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.equipe_nome,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.estabelecimento_cnes,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.estabelecimento_nome,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.acs_nome,
            'Erro de grafia'::text AS duplicacao_motivo
           FROM impulso_previne_dados_nominais.lista_nominal_gestantes_duplicadas_por_erro_grafia
        UNION ALL
         SELECT '100111'::character varying AS municipio_id_sus,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.gestante_nome,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.gestante_data_de_nascimento,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.gestante_documento_cpf,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.gestante_documento_cns,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.periodo_data_transmissao,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.gestante_dum,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.gestante_dpp,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.equipe_ine,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.equipe_nome,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.estabelecimento_cnes,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.estabelecimento_nome,
            lista_nominal_gestantes_duplicadas_por_erro_grafia.acs_nome,
            'Erro de grafia'::text AS duplicacao_motivo
           FROM impulso_previne_dados_nominais.lista_nominal_gestantes_duplicadas_por_erro_grafia
          WHERE lista_nominal_gestantes_duplicadas_por_erro_grafia.municipio_id_sus::text <> '111111'::text
        UNION ALL
         SELECT lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.municipio_id_sus,
            lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.gestante_nome,
            lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.gestante_data_de_nascimento,
            lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.gestante_documento_cpf,
            lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.gestante_documento_cns,
            lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.periodo_data_transmissao,
            lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.gestante_dum,
            lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.gestante_dpp,
            lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.equipe_ine,
            lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.equipe_nome,
            lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.estabelecimento_cnes,
            lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.estabelecimento_nome,
            lista_nominal_gestantes_duplicadas_por_erro_data_nascimento.acs_nome,
            'Erro na data de nascimento'::text AS duplicacao_motivo
           FROM impulso_previne_dados_nominais.lista_nominal_gestantes_duplicadas_por_erro_data_nascimento
        )
 SELECT tb1.municipio_id_sus,
    concat(tb2.nome, ' - ', tb2.uf_sigla) AS municipio_uf,
    tb1.gestante_nome,
    tb1.gestante_data_de_nascimento,
    tb1.gestante_documento_cpf,
    tb1.gestante_documento_cns,
    tb1.periodo_data_transmissao,
    tb1.gestante_dum,
    tb1.gestante_dpp,
    tb1.duplicacao_motivo,
    CURRENT_TIMESTAMP AS atualizacao_data,
    tb1.equipe_ine,
    tb1.equipe_nome,
    tb1.estabelecimento_cnes,
    tb1.estabelecimento_nome,
    tb1.acs_nome
   FROM rel_duplicadas tb1
     JOIN listas_de_codigos.municipios tb2 ON tb1.municipio_id_sus::bpchar = tb2.id_sus
WITH DATA;