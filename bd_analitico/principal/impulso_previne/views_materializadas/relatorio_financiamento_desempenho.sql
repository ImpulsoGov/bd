-- impulso_previne.relatorio_financiamento_desempenho source

CREATE MATERIALIZED VIEW impulso_previne.relatorio_financiamento_desempenho
TABLESPACE pg_default
AS SELECT res.periodo_data_inicio,
    res.mes,
    res.quadrimestre,
    res.municipio_uf,
    res.regiao,
    res.isf_nota,
    res.pagamento_total,
    res.pagamento_potencial,
    res.pagamento_desempenho_global,
    res.pagamento_desempenho_real,
        CASE
            WHEN res.isf_nota < 10::numeric THEN res.pagamento_desempenho_global * ((10::numeric - res.isf_nota) / 10::numeric)
            ELSE 0::numeric
        END AS pagamento_poderia_perder,
        CASE
            WHEN res.isf_nota < 10::numeric AND (res.pagamento_potencial - res.pagamento_total) > 0::numeric THEN res.pagamento_potencial - res.pagamento_total
            ELSE 0::numeric
        END AS pagamento_ja_perdido,
        CASE
            WHEN res.isf_nota < 10::numeric AND (res.pagamento_desempenho_global * ((10::numeric - res.isf_nota) / 10::numeric)) > (res.pagamento_potencial - res.pagamento_total) THEN res.pagamento_desempenho_global * ((10::numeric - res.isf_nota) / 10::numeric) - (res.pagamento_potencial - res.pagamento_total)
            ELSE 0::numeric
        END AS pagamento_pode_perder,
    CURRENT_TIMESTAMP AS criacao_data,
    CURRENT_TIMESTAMP AS atualizacao_data
   FROM ( SELECT tb1.periodo_data_inicio,
            p.codigo AS mes,
                CASE
                    WHEN tb1.periodo_data_inicio >= '2021-01-01'::date AND tb1.periodo_data_inicio <= '2021-04-01'::date THEN '2020.Q3'::text
                    WHEN tb1.periodo_data_inicio >= '2021-05-01'::date AND tb1.periodo_data_inicio <= '2021-08-01'::date THEN '2021.Q1'::text
                    WHEN tb1.periodo_data_inicio >= '2021-09-01'::date AND tb1.periodo_data_inicio <= '2021-12-01'::date THEN '2021.Q2'::text
                    WHEN tb1.periodo_data_inicio >= '2022-01-01'::date AND tb1.periodo_data_inicio <= '2022-04-01'::date THEN '2021.Q3'::text
                    WHEN tb1.periodo_data_inicio >= '2022-05-01'::date AND tb1.periodo_data_inicio <= '2022-09-01'::date THEN '2022.Q1'::text
                    WHEN tb1.periodo_data_inicio >= '2022-10-01'::date AND tb1.periodo_data_inicio <= '2023-01-01'::date THEN '2022.Q2'::text
                    WHEN tb1.periodo_data_inicio >= '2023-02-01'::date AND tb1.periodo_data_inicio <= '2023-05-01'::date THEN '2022.Q3'::text
                    ELSE NULL::text
                END AS quadrimestre,
            concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
            m.macrorregiao_nome AS regiao,
            trunc(tb1.isf_nota::numeric, 2) AS isf_nota,
            round(tb1.pagamento_total - tb1.pagamento_adicional - tb1.pagamento_desconto::numeric, 0) AS pagamento_total,
                CASE
                    WHEN round(tb1.pagamento_total - tb1.pagamento_equipes_novas::numeric - tb1.pagamento_adicional - tb1.pagamento_desconto::numeric, 0) > round(tb1.esf::numeric * 3225.0 + tb1.eap_30h::numeric * 2418.75 + tb1.eap_20h::numeric * 1612.5, 0) THEN round(tb1.esf::numeric * 3225.0 + tb1.eap_30h::numeric * 1612.5 + tb1.eap_20h::numeric * 2418.75, 0)
                    ELSE round(tb1.esf::numeric * 3225.0 + tb1.eap_30h::numeric * 2418.75 + tb1.eap_20h::numeric * 1612.5, 0)
                END AS pagamento_desempenho_global,
                CASE
                    WHEN round(tb1.pagamento_total - tb1.pagamento_equipes_novas::numeric - tb1.pagamento_adicional - tb1.pagamento_desconto::numeric, 0) > round(tb1.esf::numeric * 3225.0 + tb1.eap_30h::numeric * 2418.75 + tb1.eap_20h::numeric * 1612.5, 0) THEN round(tb1.esf::numeric * 3225.0 + tb1.eap_30h::numeric * 1612.5 + tb1.eap_20h::numeric * 2418.75 + tb1.pagamento_equipes_novas::numeric, 0)
                    ELSE round(tb1.esf::numeric * 3225.0 + tb1.eap_30h::numeric * 2418.75 + tb1.eap_20h::numeric * 1612.5 + tb1.pagamento_equipes_novas::numeric, 0)
                END AS pagamento_potencial,
            tb1.pagamento_desempenho AS pagamento_desempenho_real
           FROM impulso_previne.relatorio_egestor_corrigido_painel_insights_financiamento_isf tb1
             LEFT JOIN listas_de_codigos.periodos p ON tb1.periodo_id = p.id
             LEFT JOIN listas_de_codigos.municipios m ON tb1.municipio_id_sus = m.id_sus) res
WITH DATA;