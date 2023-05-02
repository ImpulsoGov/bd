WITH 
selecao_mulheres_denominador AS (
  -- Seleciona todas as mulheres do município com faixa etária entre 25 e 64 anos
SELECT tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento AS chave_mulher,
       tfcp.no_cidadao AS paciente_nome,
       tempocidadaopec.dt_registro AS data_de_nascimento,
       tfcp.nu_cpf_cidadao AS paciente_documento_cpf,
       tfcp.nu_cns AS paciente_documento_cns,
       tfcp.nu_telefone_celular AS paciente_telefone,
       (CURRENT_DATE - tempocidadaopec.dt_registro)/365 AS paciente_idade_atual
   FROM esus_3169356_tresmarias_mg_20230314.tb_fat_cidadao_pec tfcp
   JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_tempo tempocidadaopec ON tfcp.co_dim_tempo_nascimento = tempocidadaopec.co_seq_dim_tempo
   JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_sexo tds ON tds.co_seq_dim_sexo = tfcp.co_dim_sexo
   WHERE tds.ds_sexo = 'Feminino'
     AND (CURRENT_DATE - tempocidadaopec.dt_registro)/365 BETWEEN 25 AND 64
     AND tfcp.st_faleceu <> 1
     ),
realizacao_exames AS (
-- Seleciona todas mulheres que tiveram exame citopatológico realizado por enfermeiros ou médicos (com famílias de SIGTAP consideradas para o procedimento)
SELECT tfcp.nu_cns AS paciente_documento_cns,
       tfcp.nu_cpf_cidadao AS paciente_documento_cpf,
       tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento AS chave_mulher,
       tempoprocedimento.dt_registro AS data_realizacao_exame,
       max(tempoprocedimento.dt_registro) OVER (PARTITION BY tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento) AS data_ultimo_exame
   FROM esus_3169356_tresmarias_mg_20230314.tb_fat_proced_atend_proced tfpap
   JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_tempo tempoprocedimento ON tfpap.co_dim_tempo = tempoprocedimento.co_seq_dim_tempo
   JOIN esus_3169356_tresmarias_mg_20230314.tb_fat_cidadao_pec tfcp ON tfpap.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec
   JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_procedimento procedimentos ON tfpap.co_dim_procedimento = procedimentos.co_seq_dim_procedimento
   JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_cbo cbo ON tfpap.co_dim_cbo = cbo.co_seq_dim_cbo
   WHERE (procedimentos.co_proced::text = ANY (ARRAY['0201020033'::CHARACTER varying::text,'ABPG010'::CHARACTER varying::text]))
     AND (cbo.nu_cbo::text ~~ ANY (ARRAY['%2235%'::text, '%2251%'::text, '%2252%'::text, '%2253%'::text, '%2231%'::text]))
     AND tfcp.st_faleceu <> 1 )
SELECT tb1.chave_mulher,
       tb1.paciente_nome,
       tb1.paciente_documento_cns,
       tb1.paciente_documento_cpf,
       tb1.paciente_idade_atual,
       tb1.data_de_nascimento,
       tb2.data_ultimo_exame,
       CASE
           WHEN (CURRENT_DATE - tb2.data_ultimo_exame) <= 1095 THEN TRUE
           ELSE FALSE
       END AS realizou_exame_ultimos_36_meses,
       CASE
           WHEN date_part('month'::text, tb2.data_ultimo_exame) >= 1::double precision AND date_part('month'::text, tb2.data_ultimo_exame) <= 4::double precision THEN concat(date_part('year'::text, tb2.data_ultimo_exame), '.Q1')
           WHEN date_part('month'::text, tb2.data_ultimo_exame) >= 5::double precision AND date_part('month'::text, tb2.data_ultimo_exame) <= 8::double precision THEN concat(date_part('year'::text, tb2.data_ultimo_exame), '.Q2')
           ELSE 'exame_nunca_realizado'
       END AS quadrimestre_realizou_ultimo_exame
FROM selecao_mulheres_denominador tb1
LEFT JOIN realizacao_exames tb2 ON tb1.chave_mulher = tb2.chave_mulher
AND tb1.paciente_documento_cns = tb2.paciente_documento_cns
GROUP BY tb1.chave_mulher,
         tb1.paciente_nome,
         tb1.paciente_documento_cns,
         tb1.paciente_documento_cpf,
         tb1.paciente_idade_atual,
         tb1.data_de_nascimento,
         tb2.data_ultimo_exame
ORDER BY tb1.chave_mulher asc