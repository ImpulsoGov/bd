
CREATE MATERIALIZED VIEW configuracoes.monitoramento_transmissoes_denominadores_variacoes
TABLESPACE pg_default
AS WITH listas AS (
         SELECT concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
            lnd.municipio_id_sus,
            'Diabéticos'::text AS lista_nominal,
            lnd.cidadao_nome || lnd.dt_nascimento AS id_paciente,
            lnd.dt_consulta_mais_recente AS ultima_consulta
           FROM impulso_previne_dados_nominais.api_futuro_painel_diabeticos_lista_nominal lnd
             JOIN listas_de_codigos.municipios m ON lnd.municipio_id_sus::bpchar = m.id_sus
        UNION ALL
         SELECT concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
            lnh.municipio_id_sus,
            'Hipertensos'::text AS lista_nominal,
            lnh.cidadao_nome || lnh.dt_nascimento AS id_paciente,
            lnh.dt_consulta_mais_recente AS ultima_consulta
           FROM impulso_previne_dados_nominais.api_futuro_painel_hipertensos_lista_nominal lnh
             JOIN listas_de_codigos.municipios m ON lnh.municipio_id_sus::bpchar = m.id_sus
        UNION ALL
         SELECT res.municipio_uf,
            res.municipio_id_sus,
            res.lista_nominal,
            res.chave_gestante AS id_paciente,
            res.consulta_prenatal_ultima_data AS ultima_consulta
           FROM ( SELECT concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
                    lng.municipio_id_sus,
                    'Gestantes'::text AS lista_nominal,
                        CASE
                            WHEN date_part('month'::text, lng.gestacao_data_dpp) >= 1::double precision AND date_part('month'::text, lng.gestacao_data_dpp) <= 4::double precision THEN concat(date_part('year'::text, lng.gestacao_data_dpp), '.Q1')
                            WHEN date_part('month'::text, lng.gestacao_data_dpp) >= 5::double precision AND date_part('month'::text, lng.gestacao_data_dpp) <= 8::double precision THEN concat(date_part('year'::text, lng.gestacao_data_dpp), '.Q2')
                            WHEN date_part('month'::text, lng.gestacao_data_dpp) >= 9::double precision AND date_part('month'::text, lng.gestacao_data_dpp) <= 12::double precision THEN concat(date_part('year'::text, lng.gestacao_data_dpp), '.Q3')
                            ELSE NULL::text
                        END AS quadrimestre_gestante,
                    lng.consulta_prenatal_ultima_data,
                    lng.chave_gestante
                   FROM impulso_previne_dados_nominais.lista_nominal_gestantes_unificada lng
                     JOIN listas_de_codigos.municipios m ON lng.municipio_id_sus::bpchar = m.id_sus) res
          WHERE res.quadrimestre_gestante = (( SELECT
                        CASE
                            WHEN date_part('month'::text, CURRENT_DATE) >= 1::double precision AND date_part('month'::text, CURRENT_DATE) <= 4::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '.Q1')
                            WHEN date_part('month'::text, CURRENT_DATE) >= 5::double precision AND date_part('month'::text, CURRENT_DATE) <= 8::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '.Q2')
                            WHEN date_part('month'::text, CURRENT_DATE) >= 9::double precision AND date_part('month'::text, CURRENT_DATE) <= 12::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '.Q3')
                            ELSE NULL::text
                        END AS "case"))
        ), base AS (
         SELECT DISTINCT listas.municipio_id_sus,
            listas.municipio_uf,
            listas.lista_nominal,
            listas.id_paciente,
            listas.ultima_consulta
           FROM listas
          WHERE listas.municipio_id_sus::text <> ALL (ARRAY['111111'::text, '100111'::text])
        ), ind_sisab AS (
         SELECT sisab.municipio_id_sus,
            sisab.municipio_uf,
            sisab.periodo_codigo,
            sisab.indicador_ordem,
            sisab.indicador_nome,
            sisab.indicador_denominador_informado,
            sisab.indicador_denominador_estimado,
                CASE
                    WHEN sisab.indicador_ordem = 1 THEN 'Gestantes'::text
                    WHEN sisab.indicador_ordem = 6 THEN 'Hipertensos'::text
                    WHEN sisab.indicador_ordem = 7 THEN 'Diabéticos'::text
                    ELSE NULL::text
                END AS lista_nominal
           FROM _impulso_previne_dados_abertos.indicadores_desempenho_score_equipes_validas2 sisab
          WHERE sisab.periodo_codigo::text = '2023.Q1'::text
        ), aux AS (
         SELECT b.municipio_id_sus,
            b.municipio_uf,
            b.lista_nominal,
            count(DISTINCT b.id_paciente) AS denominador_identificado_local,
            s.indicador_denominador_informado AS ultimo_denominador_informado,
            s.indicador_denominador_estimado AS denominador_estimado,
            count(DISTINCT b.id_paciente) - s.indicador_denominador_informado AS diff_denominadores,
            count(DISTINCT b.id_paciente)::numeric * 1.00 / s.indicador_denominador_informado::numeric - 1::numeric AS diff_perc,
            max(b.ultima_consulta) AS ultima_consulta
           FROM base b
             LEFT JOIN ind_sisab s ON b.municipio_id_sus::bpchar = s.municipio_id_sus AND b.lista_nominal = s.lista_nominal
          GROUP BY b.municipio_id_sus, b.municipio_uf, b.lista_nominal, s.indicador_denominador_informado, s.indicador_denominador_estimado
          ORDER BY b.municipio_uf
        )
 SELECT aux.municipio_id_sus,
    aux.municipio_uf,
    aux.denominador_identificado_local,
    aux.ultimo_denominador_informado,
    aux.denominador_estimado,
    aux.diff_denominadores,
    aux.lista_nominal
   FROM aux
WITH DATA;