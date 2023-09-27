
CREATE MATERIALIZED VIEW configuracoes.monitoramento_transmissoes_equipes_estabelecimentos_variacao
TABLESPACE pg_default
AS WITH selecao_historico_listas AS (
         SELECT concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
            lnd.municipio_id_sus,
            'lista_nominal_citopatologico'::text AS lista_nominal,
            count(DISTINCT lnd.equipe_ine_cadastro) AS quantidade_equipes,
            count(DISTINCT lnd.estabelecimento_cnes_cadastro) AS quantidade_estabelecimentos,
            lnd.criacao_data AS periodo_data_transmissao
           FROM impulso_previne_dados_nominais.lista_nominal_citopatologico_historico lnd
             JOIN listas_de_codigos.municipios m ON lnd.municipio_id_sus::bpchar = m.id_sus
          GROUP BY lnd.municipio_id_sus, (concat(m.nome, ' - ', m.uf_sigla)), lnd.criacao_data
        UNION
         SELECT concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
            lnd.municipio_id_sus,
            'lista_nominal_diabeticos'::text AS lista_nominal,
            count(DISTINCT lnd.equipe_ine_cadastro) AS quantidade_equipes,
            count(DISTINCT lnd.estabelecimento_cnes_cadastro) AS quantidade_estabelecimentos,
            lnd.periodo_data_transmissao
           FROM impulso_previne_dados_nominais.lista_nominal_diabeticos_historico lnd
             JOIN listas_de_codigos.municipios m ON lnd.municipio_id_sus::bpchar = m.id_sus
          GROUP BY lnd.municipio_id_sus, (concat(m.nome, ' - ', m.uf_sigla)), lnd.periodo_data_transmissao
        UNION
         SELECT concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
            lnh.municipio_id_sus,
            'lista_nominal_hipertensos'::text AS lista_nominal,
            count(DISTINCT lnh.equipe_ine_cadastro) AS quantidade_equipes,
            count(DISTINCT lnh.estabelecimento_cnes_cadastro) AS quantidade_estabelecimentos,
            lnh.periodo_data_transmissao
           FROM impulso_previne_dados_nominais.lista_nominal_hipertensos_historico lnh
             JOIN listas_de_codigos.municipios m ON lnh.municipio_id_sus::bpchar = m.id_sus
          GROUP BY lnh.municipio_id_sus, (concat(m.nome, ' - ', m.uf_sigla)), lnh.periodo_data_transmissao
        UNION
         SELECT res.municipio_uf,
            res.municipio_id_sus,
            res.lista_nominal,
            count(DISTINCT res.equipe_ine) AS quantidade_equipes,
            count(DISTINCT res.estabelecimento_cnes) AS quantidade_estabelecimentos,
            res.periodo_data_transmissao
           FROM ( SELECT concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
                    lng.municipio_id_sus,
                    'lista_nominal_gestantes'::text AS lista_nominal,
                        CASE
                            WHEN date_part('month'::text, lng.gestante_dpp::date) >= 1::double precision AND date_part('month'::text, lng.gestante_dpp::date) <= 4::double precision THEN concat(date_part('year'::text, lng.gestante_dpp::date), '.Q1')
                            WHEN date_part('month'::text, lng.gestante_dpp::date) >= 5::double precision AND date_part('month'::text, lng.gestante_dpp::date) <= 8::double precision THEN concat(date_part('year'::text, lng.gestante_dpp::date), '.Q2')
                            WHEN date_part('month'::text, lng.gestante_dpp::date) >= 9::double precision AND date_part('month'::text, lng.gestante_dpp::date) <= 12::double precision THEN concat(date_part('year'::text, lng.gestante_dpp::date), '.Q3')
                            ELSE NULL::text
                        END AS quadrimestre_gestante,
                    lng.equipe_ine,
                    lng.estabelecimento_cnes,
                    lng.periodo_data_transmissao
                   FROM impulso_previne_dados_nominais._lista_nominal_gestantes_historico lng
                     JOIN listas_de_codigos.municipios m ON lng.municipio_id_sus::bpchar = m.id_sus) res
          WHERE res.quadrimestre_gestante = (( SELECT
                        CASE
                            WHEN date_part('month'::text, CURRENT_DATE) >= 1::double precision AND date_part('month'::text, CURRENT_DATE) <= 4::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '.Q1')
                            WHEN date_part('month'::text, CURRENT_DATE) >= 5::double precision AND date_part('month'::text, CURRENT_DATE) <= 8::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '.Q2')
                            WHEN date_part('month'::text, CURRENT_DATE) >= 9::double precision AND date_part('month'::text, CURRENT_DATE) <= 12::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '.Q3')
                            ELSE NULL::text
                        END AS "case"))
          GROUP BY res.municipio_uf, res.municipio_id_sus, res.lista_nominal, res.periodo_data_transmissao
        )
 SELECT tb.municipio_id_sus,
    tb.municipio_uf,
    tb.lista_nominal,
    tb.periodo_data_transmissao,
    tb.quantidade_equipes,
        CASE
            WHEN tb.quantidade_equipes > 0 AND lag(tb.quantidade_equipes) OVER (PARTITION BY tb.municipio_id_sus, tb.lista_nominal ORDER BY tb.periodo_data_transmissao) > 0 THEN (tb.quantidade_equipes - lag(tb.quantidade_equipes) OVER (PARTITION BY tb.municipio_id_sus, tb.lista_nominal ORDER BY tb.periodo_data_transmissao))::numeric
            ELSE 0::numeric
        END AS delta_equipes,
    tb.quantidade_estabelecimentos,
        CASE
            WHEN tb.quantidade_estabelecimentos > 0 AND lag(tb.quantidade_estabelecimentos) OVER (PARTITION BY tb.municipio_id_sus, tb.lista_nominal ORDER BY tb.periodo_data_transmissao) > 0 THEN (tb.quantidade_estabelecimentos - lag(tb.quantidade_estabelecimentos) OVER (PARTITION BY tb.municipio_id_sus, tb.lista_nominal ORDER BY tb.periodo_data_transmissao))::numeric
            ELSE 0::numeric
        END AS delta_estabelecimentos
   FROM selecao_historico_listas tb
  GROUP BY tb.municipio_id_sus, tb.municipio_uf, tb.lista_nominal, tb.periodo_data_transmissao, tb.quantidade_equipes, tb.quantidade_estabelecimentos
  ORDER BY tb.municipio_id_sus, tb.municipio_uf, tb.lista_nominal, tb.periodo_data_transmissao DESC
WITH DATA;