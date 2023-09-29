
CREATE MATERIALIZED VIEW configuracoes.monitoramento_transmissoes_historico
TABLESPACE pg_default
AS WITH distintos_municipios_id_sus AS (
         SELECT municipios_transmissoes_ativas.municipio_id_sus,
            municipios_transmissoes_ativas.primeira_transmissao,
            municipios_transmissoes_ativas.table_name
           FROM configuracoes.municipios_transmissoes_ativas
        ), transmissoes_agendadas_por_municipio AS (
         SELECT distintos_municipios_id_sus.municipio_id_sus,
            distintos_municipios_id_sus.table_name,
            generate_series(distintos_municipios_id_sus.primeira_transmissao::timestamp with time zone, '2023-12-30'::date::timestamp with time zone, '1 day'::interval)::date AS transmissao_dia
           FROM distintos_municipios_id_sus
        ), transmissoes_agendadas_por_municipio_filtrado AS (
         SELECT transmissoes_agendadas_por_municipio.transmissao_dia,
                CASE
                    WHEN date_part('dow'::text, transmissoes_agendadas_por_municipio.transmissao_dia) = 0::double precision THEN 'DOM'::text
                    WHEN date_part('dow'::text, transmissoes_agendadas_por_municipio.transmissao_dia) = 1::double precision THEN 'SEG'::text
                    WHEN date_part('dow'::text, transmissoes_agendadas_por_municipio.transmissao_dia) = 2::double precision THEN 'TER'::text
                    WHEN date_part('dow'::text, transmissoes_agendadas_por_municipio.transmissao_dia) = 3::double precision THEN 'QUA'::text
                    WHEN date_part('dow'::text, transmissoes_agendadas_por_municipio.transmissao_dia) = 4::double precision THEN 'QUI'::text
                    WHEN date_part('dow'::text, transmissoes_agendadas_por_municipio.transmissao_dia) = 5::double precision THEN 'SEX'::text
                    WHEN date_part('dow'::text, transmissoes_agendadas_por_municipio.transmissao_dia) = 6::double precision THEN 'SAB'::text
                    ELSE NULL::text
                END AS dia_da_semana,
            transmissoes_agendadas_por_municipio.municipio_id_sus,
            transmissoes_agendadas_por_municipio.table_name
           FROM transmissoes_agendadas_por_municipio
        ), transmissoes_por_municipio_por_lista AS (
         SELECT tb1_1.municipio_id_sus,
            tb1_1.tabela_nome,
            concat(m_1.nome, ' - ', m_1.uf_sigla) AS municipio_uf,
            max(tb1_1.execucao_data_hora::date) OVER (PARTITION BY tb1_1.municipio_id_sus, tb1_1.tabela_nome, (tb1_1.execucao_data_hora::date)) AS execucao_data_hora_unica
           FROM configuracoes.transmissor_historico tb1_1
             JOIN listas_de_codigos.municipios m_1 ON tb1_1.municipio_id_sus::bpchar = m_1.id_sus
          WHERE tb1_1.mensagem ~~ '%realizada com sucesso%'::text
          ORDER BY m_1.nome, tb1_1.execucao_data_hora DESC
        ), transmissoes_agendada_realizadas AS (
         SELECT tb1_1.dia_da_semana,
            tb1_1.transmissao_dia,
            tb1_1.table_name,
            tb1_1.municipio_id_sus,
            tb2.tabela_nome,
            tb2.municipio_uf
           FROM transmissoes_agendadas_por_municipio_filtrado tb1_1
             LEFT JOIN transmissoes_por_municipio_por_lista tb2 ON tb1_1.municipio_id_sus::text = tb2.municipio_id_sus::text AND tb1_1.transmissao_dia = tb2.execucao_data_hora_unica AND tb1_1.table_name = tb2.tabela_nome
          WHERE (tb1_1.dia_da_semana <> ALL (ARRAY['DOM'::text, 'SAB'::text])) AND (tb1_1.transmissao_dia <> ALL (ARRAY['2023-04-07'::date, '2023-04-21'::date, '2023-05-01'::date, '2023-09-07'::date, '2023-10-12'::date, '2023-11-02'::date, '2023-11-15'::date, '2023-12-25'::date]))
        ), transmissoes_status AS (
         SELECT tb1.dia_da_semana,
            tb1.transmissao_dia,
            tb1.municipio_id_sus,
            tb1.tabela_nome,
            tb1.table_name,
                CASE
                    WHEN tb1.municipio_uf IS NOT NULL THEN tb1.municipio_uf
                    ELSE concat(m.nome, ' - ', m.uf_sigla)
                END AS municipio_uf,
                CASE
                    WHEN tb1.tabela_nome IS NOT NULL THEN 'Transmissão realizada'::text
                    ELSE 'Transmissão perdida'::text
                END AS status_transmissao
           FROM transmissoes_agendada_realizadas tb1
             JOIN listas_de_codigos.municipios m ON tb1.municipio_id_sus::text = m.id_sus::text
          WHERE tb1.transmissao_dia < CURRENT_DATE
        )
 SELECT ts.dia_da_semana,
    ts.transmissao_dia,
    ts.municipio_id_sus,
    ts.table_name AS lista_nominal,
    ts.municipio_uf,
    ts.status_transmissao
   FROM transmissoes_status ts
  WHERE ts.table_name <> 'relatorio_mensal_indicadores'::text
WITH DATA;

-- View indexes:
CREATE INDEX monitoramento_transmissoes_historico_municipio_id_sus_idx ON configuracoes.monitoramento_transmissoes_historico USING btree (municipio_id_sus, transmissao_dia);