
CREATE MATERIALIZED VIEW configuracoes.nomes_ficticios_citopatologico
TABLESPACE pg_default
AS WITH geracao_nomes AS (
         WITH base AS (
                 SELECT row_number() OVER (PARTITION BY 0::integer) AS sequencia,
                    concat(arrays.primeiro_nome[s.a % array_length(arrays.primeiro_nome, 1) + 1], ' ', arrays.primeiro_sobrenome[s.a % array_length(arrays.primeiro_sobrenome, 1) + 1], ' ', arrays.segundo_sobrenome[s.a % array_length(arrays.segundo_sobrenome, 1) + 1]) AS nome_ficticio
                   FROM generate_series(1, 16000) s(a)
                     CROSS JOIN ( SELECT ( SELECT array_agg(DISTINCT "substring"(tb_1.paciente_nome::text, '^[^ ]+'::text)) AS nome
                                   FROM dados_nominais_mg_itabira.lista_nominal_citopatologico tb_1
                                  WHERE "substring"(tb_1.paciente_nome::text, '^[^ ]+'::text) <> ALL (ARRAY['ABIDA'::text, 'ACHILE'::text, 'ADEBIA'::text, 'ADEIZA'::text, 'ABEGAIL'::text, 'ADALA'::text, 'ADIL'::text, 'ACLESIA'::text, 'ABGAIL'::text, 'EDSON'::text, 'VICTOR'::text, 'GENEZIO'::text, 'ADEIR'::text, 'ADIONES'::text, 'ALAIR'::text, 'RODRIGO'::text, 'PAULO'::text])) AS primeiro_nome,
                            ( SELECT array_agg(DISTINCT reverse(split_part(reverse("substring"(tb_1.paciente_nome::text, '[^ ]+$'::text)), ' '::text, 1))) AS ultimo_nome
                                   FROM dados_nominais_ba_jacobina.lista_nominal_citopatologico tb_1
                                  WHERE "substring"(tb_1.paciente_nome::text, '^[^ ]+'::text) <> ALL (ARRAY['FILHO'::text, '%NETO%'::text, '%BISNETO%'::text])) AS primeiro_sobrenome,
                            ( SELECT array_agg(DISTINCT reverse(split_part(reverse("substring"(tb_1.paciente_nome::text, '[^ ]+$'::text)), ' '::text, 1))) AS ultimo_nome
                                   FROM dados_nominais_rn_barauna.lista_nominal_citopatologico tb_1
                                  WHERE "substring"(tb_1.paciente_nome::text, '^[^ ]+'::text) <> ALL (ARRAY['FILHO'::text, '%NETO%'::text, '%BISNETO%'::text])) AS segundo_sobrenome) arrays
                )
         SELECT base.sequencia,
            base.nome_ficticio
           FROM base
          ORDER BY (random())
        )
 SELECT row_number() OVER (PARTITION BY 0::integer) AS seq,
    tb.nome_ficticio
   FROM geracao_nomes tb
WITH DATA;