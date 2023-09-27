-- configuracoes.nomes_ficticios_gestantes source

CREATE MATERIALIZED VIEW configuracoes.nomes_ficticios_gestantes
TABLESPACE pg_default
AS WITH geracao_nomes AS (
         WITH base AS (
                 SELECT row_number() OVER (PARTITION BY 0::integer) AS sequencia,
                    concat(arrays.primeiro_nome[s.a % array_length(arrays.primeiro_nome, 1) + 1], ' ', arrays.primeiro_sobrenome[s.a % array_length(arrays.primeiro_sobrenome, 1) + 1], ' ', arrays.segundo_sobrenome[s.a % array_length(arrays.segundo_sobrenome, 1) + 1]) AS nome_ficticio
                   FROM generate_series(1, 1000) s(a)
                     CROSS JOIN ( SELECT ( SELECT array_agg(DISTINCT "substring"(tb_1.paciente_nome::text, '^[^ ]+'::text)) AS nome
                                   FROM dados_nominais_sp_amparo.lista_nominal_citopatologico tb_1
                                  WHERE "substring"(tb_1.paciente_nome::text, '^[^ ]+'::text) <> ALL (ARRAY['ABIDA'::text, 'ACHILE'::text, 'ABEGAIL'::text, 'ADIL'::text, 'ACLESIA'::text, 'ABGAIL'::text, 'EDSON'::text, 'VICTOR'::text, 'GENEZIO'::text, 'ADEIR'::text, 'ADIONES'::text, 'ALAIR'::text])) AS primeiro_nome,
                            ( SELECT array_agg(DISTINCT reverse(split_part(reverse("substring"(tb_1.cidadao_nome::text, '[^ ]+$'::text)), ' '::text, 1))) AS ultimo_nome
                                   FROM dados_nominais_ac_xapuri.lista_nominal_hipertensos tb_1
                                  WHERE "substring"(tb_1.cidadao_nome::text, '^[^ ]+'::text) <> ALL (ARRAY['ALVEW'::text, 'FILHO'::text, 'NETO'::text, 'BISNETO'::text])) AS primeiro_sobrenome,
                            ( SELECT array_agg(DISTINCT reverse(split_part(reverse("substring"(tb_1.cidadao_nome::text, '[^ ]+$'::text)), ' '::text, 1))) AS ultimo_nome
                                   FROM dados_nominais_sp_juquitiba.lista_nominal_hipertensos tb_1
                                  WHERE "substring"(tb_1.cidadao_nome::text, '^[^ ]+'::text) <> ALL (ARRAY['ALVEW'::text, 'FILHO'::text, 'NETO'::text, 'BISNETO'::text])) AS segundo_sobrenome) arrays
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