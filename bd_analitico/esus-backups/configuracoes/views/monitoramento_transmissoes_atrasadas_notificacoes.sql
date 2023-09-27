
CREATE OR REPLACE VIEW configuracoes.monitoramento_transmissoes
AS WITH rel_transmissao AS (
         SELECT hc_1.municipio_id_sus,
            hc_1.tabela_nome,
            count(*) AS transmissoes
           FROM configuracoes.transmissor_historico hc_1
          WHERE hc_1.mensagem ~~ '%com sucesso%'::text
          GROUP BY hc_1.municipio_id_sus, hc_1.tabela_nome
        ), rel_transmissao_status_dia AS (
         SELECT concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
            tb1.municipio_id_sus,
            tb1.tabela_nome AS lista_nominal,
            max(tb1.execucao_data_hora) AS ultima_tranmissao,
                CASE
                    WHEN max(tb1.execucao_data_hora::date) = CURRENT_DATE THEN 'Transmissão realizada'::text
                    WHEN max(tb1.execucao_data_hora::date) <= (CURRENT_DATE - 2) THEN 'Transmissão atrasada'::text
                    WHEN max(tb1.execucao_data_hora::date) <= (CURRENT_DATE - 1) THEN 'Transmissão em agenda'::text
                    ELSE NULL::text
                END AS status_transmissao,
                CASE
                    WHEN max(tb1.execucao_data_hora::date) < CURRENT_DATE THEN (CURRENT_TIMESTAMP - max(tb1.execucao_data_hora))::text
                    ELSE NULL::text
                END AS transmissao_atrasao_dias
           FROM configuracoes.transmissor_historico tb1
             JOIN listas_de_codigos.municipios m ON tb1.municipio_id_sus::bpchar = m.id_sus
          GROUP BY m.nome, m.uf_sigla, tb1.municipio_id_sus, tb1.tabela_nome
          ORDER BY m.nome, tb1.tabela_nome
        )
 SELECT rel_transmissao_status_dia.municipio_uf,
        CASE
            WHEN rel_transmissao_status_dia.lista_nominal = 'lista_nominal_hipertensos'::text THEN 'Hipertensos'::text
            WHEN rel_transmissao_status_dia.lista_nominal = 'lista_nominal_diabeticos'::text THEN 'Diabéticos'::text
            WHEN rel_transmissao_status_dia.lista_nominal = 'lista_nominal_gestantes'::text THEN 'Gestantes'::text
            WHEN rel_transmissao_status_dia.lista_nominal = 'lista_nominal_citopatologico'::text THEN 'Citopatológico'::text
            ELSE NULL::text
        END AS lista_nominal,
    rel_transmissao_status_dia.ultima_tranmissao,
    rel_transmissao_status_dia.status_transmissao,
        CASE
            WHEN rel_transmissao_status_dia.transmissao_atrasao_dias ~~ '%day%'::text THEN rel_transmissao_status_dia.transmissao_atrasao_dias
            ELSE '-'::text
        END AS transmissao_atrasao_dias,
    rel_transmissao_status_dia.municipio_id_sus,
        CASE
            WHEN rel_transmissao_status_dia.transmissao_atrasao_dias >= '2 days'::text AND rel_transmissao_status_dia.transmissao_atrasao_dias < '3 days'::text THEN 'Atrasado mais de 48h'::text
            WHEN rel_transmissao_status_dia.transmissao_atrasao_dias >= '3 days'::text THEN 'Atrasado mais de 72h'::text
            ELSE NULL::text
        END AS notificacao_tipo
   FROM rel_transmissao_status_dia
  WHERE (rel_transmissao_status_dia.lista_nominal <> ALL (ARRAY['lista_nominal_vacinacao'::text, 'relatorio_mensal_indicadores'::text])) AND (rel_transmissao_status_dia.municipio_id_sus::text <> ALL (ARRAY['210280'::character varying::text, '315210'::character varying::text, '111111'::text]));