WITH 
selecao_mulheres_denominador AS (
  -- Seleciona todas as mulheres do município com faixa etária entre 25 e 64 anos
with tb as (
SELECT 
	   tfcp.co_seq_fat_cidadao_pec as id_cidadao_pec,
	   tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento AS chave_mulher,
       tfcp.no_cidadao AS paciente_nome,
       tempocidadaopec.dt_registro AS data_de_nascimento,
       tfcp.nu_cpf_cidadao AS paciente_documento_cpf,
       tfcp.nu_cns AS paciente_documento_cns,
       tds.ds_sexo as paciente_sexo,
       tfcp.nu_telefone_celular AS paciente_telefone,
       date_part('year'::text, age(CURRENT_DATE::timestamp with time zone, tempocidadaopec.dt_registro::timestamp with time zone))::integer AS paciente_idade_atual,
       CASE
            WHEN date_part('month'::text, CURRENT_DATE) >= 1::double precision AND date_part('month'::text, CURRENT_DATE) <= 4::double precision THEN concat(date_part('year'::text, CURRENT_DATE ::date), '-01-01')
            WHEN date_part('month'::text, CURRENT_DATE) >= 5::double precision AND date_part('month'::text, CURRENT_DATE) <= 8::double precision THEN concat(date_part('year'::text, CURRENT_DATE ::date), '-05-01')
            WHEN date_part('month'::text, CURRENT_DATE) >= 9::double precision AND date_part('month'::text, CURRENT_DATE) <= 12::double precision THEN concat(date_part('year'::text, CURRENT_DATE ::date), '-09-01')
            ELSE NULL::text
        end as data_inicio_quadrimestre,
        CASE
            WHEN date_part('month'::text, CURRENT_DATE) >= 1::double precision AND date_part('month'::text, CURRENT_DATE) <= 4::double precision THEN concat(date_part('year'::text, CURRENT_DATE ::date), '-04-30')
            WHEN date_part('month'::text, CURRENT_DATE) >= 5::double precision AND date_part('month'::text, CURRENT_DATE) <= 8::double precision THEN concat(date_part('year'::text, CURRENT_DATE ::date), '-08-31')
            WHEN date_part('month'::text, CURRENT_DATE) >= 9::double precision AND date_part('month'::text, CURRENT_DATE) <= 12::double precision THEN concat(date_part('year'::text, CURRENT_DATE ::date), '-12-31')
            ELSE NULL::text
        end as data_fim_quadrimestre
   FROM esus_3169356_tresmarias_mg_20230314.tb_fat_cidadao_pec tfcp
   JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_tempo tempocidadaopec ON tfcp.co_dim_tempo_nascimento = tempocidadaopec.co_seq_dim_tempo
   JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_sexo tds ON tds.co_seq_dim_sexo = tfcp.co_dim_sexo
   WHERE tds.ds_sexo = 'Feminino'
     AND tfcp.st_faleceu <> 1)
     select 
     tb.chave_mulher,
     tb.paciente_nome,
     tb.data_de_nascimento,
     (array_agg(tb.paciente_documento_cpf) FILTER (WHERE tb.paciente_documento_cpf IS NOT NULL) OVER (PARTITION BY tb.chave_mulher ORDER BY tb.id_cidadao_pec DESC))[1] AS paciente_documento_cpf,
     (array_agg(tb.paciente_documento_cns) FILTER (WHERE tb.paciente_documento_cns IS NOT NULL) OVER (PARTITION BY tb.chave_mulher ORDER BY tb.id_cidadao_pec DESC))[1] AS paciente_documento_cns,
     (array_agg(tb.paciente_telefone) FILTER (WHERE tb.paciente_telefone IS NOT NULL) OVER (PARTITION BY tb.chave_mulher ORDER BY tb.id_cidadao_pec DESC))[1] AS paciente_telefone,
     tb.paciente_idade_atual,
     date_part('year'::text, age(tb.data_inicio_quadrimestre::timestamp with time zone, tb.data_de_nascimento::timestamp with time zone))::integer AS idade_inicio_quadrimestre,
     date_part('year'::text, age(tb.data_fim_quadrimestre::timestamp with time zone, tb.data_de_nascimento::timestamp with time zone))::integer AS idade_fim_quadrimestre 
     from tb
     where date_part('year'::text, age(tb.data_inicio_quadrimestre::timestamp with time zone, tb.data_de_nascimento::timestamp with time zone))::integer between 25 and 64
     or date_part('year'::text, age(tb.data_fim_quadrimestre::timestamp with time zone, tb.data_de_nascimento::timestamp with time zone))::integer between 25 and 64
     ),
-- Seleciona todas mulheres que tiveram exame citopatológico realizado por enfermeiros ou médicos (com famílias de SIGTAP consideradas para o procedimento)
-- Pegar pelo codigo do procedimento -> maior codigo
realizacao_exames as (
with tb as (
SELECT 
	   tfcp.nu_cns AS paciente_documento_cns,
       tfcp.nu_cpf_cidadao AS paciente_documento_cpf,
       tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento AS chave_mulher,
       tempoprocedimento.dt_registro AS data_realizacao_exame,
       --max(tempoprocedimento.dt_registro) OVER (PARTITION BY tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento) AS data_ultimo_exame,
       tfpap.co_seq_fat_proced_atend_proced as id_registro,
       max(tfpap.co_seq_fat_proced_atend_proced) OVER (PARTITION BY tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento) AS codigo_ultimo_exame
   FROM esus_3169356_tresmarias_mg_20230314.tb_fat_proced_atend_proced tfpap
   JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_tempo tempoprocedimento ON tfpap.co_dim_tempo = tempoprocedimento.co_seq_dim_tempo
   JOIN esus_3169356_tresmarias_mg_20230314.tb_fat_cidadao_pec tfcp ON tfpap.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec
   JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_procedimento procedimentos ON tfpap.co_dim_procedimento = procedimentos.co_seq_dim_procedimento
   JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_cbo cbo ON tfpap.co_dim_cbo = cbo.co_seq_dim_cbo
   WHERE (procedimentos.co_proced::text = ANY (ARRAY['0201020033'::CHARACTER varying::text,'ABPG010'::CHARACTER varying::text]))
     AND (cbo.nu_cbo::text ~~ ANY (ARRAY['%2235%'::text, '%2251%'::text, '%2252%'::text, '%2253%'::text, '%2231%'::text]))
     AND tfcp.st_faleceu <> 1
     ), selecao_ultimo_exame as (
     select 
     tb.paciente_documento_cns,
     tb.paciente_documento_cpf,
     tb.chave_mulher,
     tb.data_realizacao_exame as data_ultimo_exame,
     tb.id_registro,
     --tb.codigo_ultimo_exame,
     row_number() OVER (PARTITION BY tb.chave_mulher ORDER BY tb.id_registro desc) = 1 AS ultimo_exame_realizado
	from tb)
	select * from selecao_ultimo_exame where ultimo_exame_realizado is true
     ), 
cadastro_individual_recente AS (
-- Dados do cadastro individual (dados para vinculação de equipe e ACS da mulher)
	WITH base AS (
		SELECT 
			mu.chave_mulher,
			tdt.dt_registro AS data_cadastro_individual,
			tfci.nu_micro_area AS micro_area_cad_individual,
			uns.nu_cnes AS cnes_estabelecimento_cad_individual,
			uns.no_unidade_saude AS estabelecimento_cad_individual,
			eq.nu_ine AS ine_equipe_cad_individual,
			eq.no_equipe AS equipe_cad_individual,
			acs.no_profissional AS acs_cad_individual,
			row_number() OVER (PARTITION BY mu.chave_mulher ORDER BY tdt.dt_registro DESC) = 1 AS ultimo_cadastro_individual
		FROM esus_3169356_tresmarias_mg_20230314.tb_fat_cad_individual tfci
		JOIN esus_3169356_tresmarias_mg_20230314.tb_fat_cidadao_pec tfcpec
			ON tfcpec.co_seq_fat_cidadao_pec = tfci.co_fat_cidadao_pec
		JOIN selecao_mulheres_denominador mu 
			ON mu.chave_mulher = tfcpec.no_cidadao::text||tfcpec.co_dim_tempo_nascimento
		LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_tempo tdt 
			ON tdt.co_seq_dim_tempo = tfci.co_dim_tempo
		LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_equipe eq
			ON eq.co_seq_dim_equipe = tfci.co_dim_equipe
		LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_profissional acs
			ON acs.co_seq_dim_profissional = tfci.co_dim_profissional
		LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_unidade_saude uns
			ON uns.co_seq_dim_unidade_saude = tfci.co_dim_unidade_saude  
		)
	SELECT * FROM base WHERE ultimo_cadastro_individual IS true
	order by chave_mulher
), 
visita_domiciliar_recente AS (
-- Dados das visitas domiciliares realizadas pelos ACS (dados para vinculação de ACS da mulher)
	WITH base AS (
		SELECT 
			mu.chave_mulher,
		    tfcpec.co_seq_fat_cidadao_pec,
			tdt.dt_registro AS data_visita_acs,
			acs.no_profissional AS acs_visita_domiciliar,
			row_number() OVER (PARTITION BY mu.chave_mulher ORDER BY tdt.dt_registro DESC) = 1 AS ultima_visita_domiciliar
		FROM esus_3169356_tresmarias_mg_20230314.tb_fat_visita_domiciliar visitadomiciliar
		JOIN esus_3169356_tresmarias_mg_20230314.tb_fat_cidadao_pec tfcpec
			ON tfcpec.co_seq_fat_cidadao_pec = visitadomiciliar.co_fat_cidadao_pec 
		JOIN selecao_mulheres_denominador mu
			ON mu.chave_mulher = tfcpec.no_cidadao::text||tfcpec.co_dim_tempo_nascimento
		LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_profissional acs
			ON acs.co_seq_dim_profissional = visitadomiciliar.co_dim_profissional
		LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_tempo tdt 
			ON tdt.co_seq_dim_tempo = visitadomiciliar.co_dim_tempo
		)
	SELECT * FROM base WHERE ultima_visita_domiciliar IS TRUE 
), 
cadastro_domiciliar_recente AS (
-- Dados do cadastro da família e do domicílio da mulher (dados para vinculação de ACS da mulher)
	WITH base AS (
		SELECT 
			mu.chave_mulher,
			tdt.dt_registro AS data_cadastro_dom_familia,
			caddomiciliarfamilia.nu_micro_area AS micro_area_domicilio,
			uns.nu_cnes AS cnes_estabelecimento_cad_dom_familia,
			uns.no_unidade_saude AS estabelecimento_cad_dom_familia,
			eq.nu_ine AS ine_equipe_cad_dom_familia,
			eq.no_equipe AS equipe_cad_dom_familia,
			acs.no_profissional AS acs_cad_dom_familia,
			NULLIF(concat(cadomiciliar.no_logradouro, ', ', cadomiciliar.nu_num_logradouro), ', '::text) AS gestante_endereco,
			row_number() OVER (PARTITION BY mu.chave_mulher ORDER BY tdt.dt_registro DESC) = 1  AS ultimo_cadastro_domiciliar_familia
		FROM esus_3169356_tresmarias_mg_20230314.tb_fat_cad_dom_familia caddomiciliarfamilia
		JOIN esus_3169356_tresmarias_mg_20230314.tb_fat_cad_domiciliar cadomiciliar 
			ON cadomiciliar.co_seq_fat_cad_domiciliar = caddomiciliarfamilia.co_fat_cad_domiciliar
		JOIN esus_3169356_tresmarias_mg_20230314.tb_fat_cidadao_pec tfcpec
			ON tfcpec.co_seq_fat_cidadao_pec = caddomiciliarfamilia.co_fat_cidadao_pec 
		JOIN selecao_mulheres_denominador mu
			ON mu.chave_mulher = tfcpec.no_cidadao::text||tfcpec.co_dim_tempo_nascimento
		LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_tempo tdt 
			ON tdt.co_seq_dim_tempo = caddomiciliarfamilia.co_dim_tempo
		LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_equipe eq
			ON eq.co_seq_dim_equipe = caddomiciliarfamilia.co_dim_equipe
		LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_profissional acs
			ON acs.co_seq_dim_profissional = caddomiciliarfamilia.co_dim_profissional
		LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_unidade_saude uns
			ON uns.co_seq_dim_unidade_saude = caddomiciliarfamilia.co_dim_unidade_saude  		
		)
	SELECT * FROM base WHERE ultimo_cadastro_domiciliar_familia IS true
),
relatorio_preliminar as (
SELECT tb1.chave_mulher,
       tb1.paciente_nome,
       tb1.paciente_documento_cns,
       tb1.paciente_documento_cpf,
       tb1.paciente_idade_atual,
       tb1.idade_inicio_quadrimestre,
       tb1.idade_fim_quadrimestre,
       tb1.data_de_nascimento,
       tb2.data_ultimo_exame,
       CASE
           WHEN (CURRENT_DATE - tb2.data_ultimo_exame) <= 1095 THEN TRUE
           ELSE FALSE
       END AS realizou_exame_ultimos_36_meses,
       CASE
           WHEN date_part('month'::text, tb2.data_ultimo_exame) >= 1::double precision AND date_part('month'::text, tb2.data_ultimo_exame) <= 4::double precision THEN concat(date_part('year'::text, tb2.data_ultimo_exame), '.Q1')
           WHEN date_part('month'::text, tb2.data_ultimo_exame) >= 5::double precision AND date_part('month'::text, tb2.data_ultimo_exame) <= 8::double precision THEN concat(date_part('year'::text, tb2.data_ultimo_exame), '.Q2')
           WHEN date_part('month'::text, tb2.data_ultimo_exame) >= 9::double precision AND date_part('month'::text, tb2.data_ultimo_exame) <= 12::double precision THEN concat(date_part('year'::text, tb2.data_ultimo_exame), '.Q3')
		   else 'exame nunca realizado'
       END AS quadrimestre_realizou_ultimo_exame,
       (tb2.data_ultimo_exame + 1095) as data_limite_proximo_exame,
       (tb2.data_ultimo_exame + 1095) - current_date as dias_para_proximo_exame,
       case 
       	when (tb2.data_ultimo_exame + 1095) - current_date <= 0 then 'exame_vencido'
       	when (tb2.data_ultimo_exame + 1095) - current_date > 0 then 'exame_em_dia'
       	else 'exame_nunca_realizado'
       end as status_exame_mulher
FROM selecao_mulheres_denominador tb1
LEFT JOIN realizacao_exames tb2 ON tb1.chave_mulher = tb2.chave_mulher
AND tb1.paciente_documento_cns = tb2.paciente_documento_cns
GROUP BY tb1.chave_mulher,
         tb1.paciente_nome,
         tb1.paciente_documento_cns,
         tb1.paciente_documento_cpf,
         tb1.paciente_idade_atual,
         tb1.idade_inicio_quadrimestre,
       	 tb1.idade_fim_quadrimestre,
         tb1.data_de_nascimento,
         tb2.data_ultimo_exame
ORDER BY tb1.chave_mulher asc
) select * from relatorio_preliminar order by paciente_idade_atual asc