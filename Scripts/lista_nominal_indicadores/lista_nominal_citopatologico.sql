WITH dados_cidadao_pec AS (
    SELECT 
        tfcp.co_seq_fat_cidadao_pec AS id_cidadao_pec,
        replace(tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento, ' ', '') AS chave_mulher,
        replace(tfcp.no_cidadao, '  ', ' ') AS paciente_nome,
        tempocidadaopec.dt_registro AS data_de_nascimento,
        tfcp.nu_cpf_cidadao AS paciente_documento_cpf,
        tfcp.nu_cns AS paciente_documento_cns,
        tds.ds_sexo AS paciente_sexo,
        tfcp.nu_telefone_celular AS paciente_telefone,
        date_part('year', age(CURRENT_DATE::timestamp with time zone, tempocidadaopec.dt_registro::timestamp with time zone))::integer AS paciente_idade_atual,
        CASE
            WHEN date_part('month', CURRENT_DATE) >= 1 AND date_part('month', CURRENT_DATE) <= 4 THEN concat(date_part('year', CURRENT_DATE ::date), '-04-30')
            WHEN date_part('month', CURRENT_DATE) >= 5 AND date_part('month', CURRENT_DATE) <= 8 THEN concat(date_part('year', CURRENT_DATE ::date), '-08-31')
            WHEN date_part('month', CURRENT_DATE) >= 9 AND date_part('month', CURRENT_DATE) <= 12 THEN concat(date_part('year', CURRENT_DATE ::date), '-12-31')
            ELSE NULL
        END AS data_fim_quadrimestre
    FROM esus_3169356_tresmarias_mg_20230314.tb_fat_cidadao_pec tfcp
    JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_tempo tempocidadaopec ON tfcp.co_dim_tempo_nascimento = tempocidadaopec.co_seq_dim_tempo
    JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_sexo tds ON tds.co_seq_dim_sexo = tfcp.co_dim_sexo
    WHERE tds.ds_sexo = 'Feminino'
      AND tfcp.st_faleceu <> 1
),
selecao_mulheres_denominador as (
     SELECT 
	     dc.chave_mulher,
	     dc.paciente_nome,
	     dc.data_de_nascimento,
	     (array_agg(dc.paciente_documento_cpf) FILTER (WHERE dc.paciente_documento_cpf IS NOT NULL) OVER (PARTITION BY dc.chave_mulher ORDER BY dc.id_cidadao_pec DESC))[1] AS paciente_documento_cpf,
	     (array_agg(dc.paciente_documento_cns) FILTER (WHERE dc.paciente_documento_cns IS NOT NULL) OVER (PARTITION BY dc.chave_mulher ORDER BY dc.id_cidadao_pec DESC))[1] AS paciente_documento_cns,
	     dc.paciente_idade_atual,
	     date_part('year', age(dc.data_fim_quadrimestre::timestamp with time zone, dc.data_de_nascimento::timestamp with time zone))::integer AS idade_fim_quadrimestre 
	 FROM dados_cidadao_pec dc
	 WHERE date_part('year', age(dc.data_fim_quadrimestre::timestamp with time zone, dc.data_de_nascimento::timestamp with time zone))::integer BETWEEN 25 AND 64
),
historico_exames_citopatologico as (
SELECT 
	   tfcp.nu_cns AS paciente_documento_cns,
       tfcp.nu_cpf_cidadao AS paciente_documento_cpf,
       replace(tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento,' ','') AS chave_mulher,
       tempoprocedimento.dt_registro AS data_realizacao_exame,
       tfpap.co_seq_fat_proced_atend_proced as id_registro,
       max(tfpap.co_seq_fat_proced_atend_proced) OVER (PARTITION BY tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento) AS codigo_ultimo_exame,
 		tdus.nu_cnes as cnes_estabelecimento_exame,
     	tdus.no_unidade_saude as nome_estabelecimento_exame,
     	tde.nu_ine as ine_equipe_exame,
     	tde.no_equipe as nome_equipe_exame,
     	tdp.nu_cns as cns_profissional_exame,
    	 tdp.no_profissional as nome_profissional_exame
   FROM esus_3169356_tresmarias_mg_20230314.tb_fat_proced_atend_proced tfpap
   JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_tempo tempoprocedimento ON tfpap.co_dim_tempo = tempoprocedimento.co_seq_dim_tempo
   JOIN esus_3169356_tresmarias_mg_20230314.tb_fat_cidadao_pec tfcp ON tfpap.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec
   JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_procedimento procedimentos ON tfpap.co_dim_procedimento = procedimentos.co_seq_dim_procedimento
   JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_cbo cbo ON tfpap.co_dim_cbo = cbo.co_seq_dim_cbo
   join esus_3169356_tresmarias_mg_20230314.tb_dim_unidade_saude tdus on tfpap.co_dim_unidade_saude = tdus.co_seq_dim_unidade_saude
   join esus_3169356_tresmarias_mg_20230314.tb_dim_equipe tde on tfpap.co_dim_equipe = tde.co_seq_dim_equipe
   join esus_3169356_tresmarias_mg_20230314.tb_dim_profissional tdp on tfpap.co_dim_profissional = tdp.co_seq_dim_profissional
   WHERE (procedimentos.co_proced::text = ANY (ARRAY['0201020033'::CHARACTER varying::text,'ABPG010'::CHARACTER varying::text]))
     AND (cbo.nu_cbo::text ~~ ANY (ARRAY['%2235%'::text, '%2251%'::text, '%2252%'::text, '%2253%'::text, '%2231%'::text]))
     AND tfcp.st_faleceu <> 1
), 
selecao_ultimo_exame AS (
	with base as (
		SELECT
			hc.chave_mulher,
			hc.data_realizacao_exame AS data_ultimo_exame,
			hc.id_registro,
			COUNT(*) OVER (PARTITION BY hc.chave_mulher) AS contagem_exames,
			hc.cnes_estabelecimento_exame,
			hc.nome_estabelecimento_exame,
			hc.ine_equipe_exame,
			hc.nome_equipe_exame,
			hc.cns_profissional_exame,
			hc.nome_profissional_exame,
			row_number() OVER (PARTITION BY hc.chave_mulher ORDER BY hc.id_registro desc) = 1 AS ultimo_exame_realizado
		FROM historico_exames_citopatologico hc
	) select * from base where ultimo_exame_realizado is true 
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
			ON mu.chave_mulher = replace(tfcpec.no_cidadao::text||tfcpec.co_dim_tempo_nascimento,' ','')
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
			ON mu.chave_mulher = replace(tfcpec.no_cidadao::text||tfcpec.co_dim_tempo_nascimento,' ','')
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
			ON mu.chave_mulher = replace(tfcpec.no_cidadao::text||tfcpec.co_dim_tempo_nascimento,' ','')
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
infos_mulheres_atendimento_individual_recente as (
		 SELECT b.chave_mulher,
				b.paciente_nome,
				--b.data_de_nascimento,
				--b.paciente_documento_cpf,
				--b.paciente_documento_cns,
				--b.paciente_telefone,
				cir.data_cadastro_individual AS data_ultimo_cadastro_individual,
				cir.micro_area_cad_individual,
				cir.cnes_estabelecimento_cad_individual,
				cir.estabelecimento_cad_individual,
				cir.ine_equipe_cad_individual,
				cir.equipe_cad_individual,
				cir.acs_cad_individual,
				vdr.data_visita_acs AS data_ultima_visita_acs,
				vdr.acs_visita_domiciliar,
				cdr.data_cadastro_dom_familia AS data_ultimo_cadastro_dom_familia,
				cdr.micro_area_domicilio,
				cdr.cnes_estabelecimento_cad_dom_familia,
				cdr.estabelecimento_cad_dom_familia,
				cdr.ine_equipe_cad_dom_familia,
				cdr.equipe_cad_dom_familia,
				cdr.acs_cad_dom_familia	
					FROM selecao_mulheres_denominador b 
					LEFT JOIN cadastro_individual_recente cir 
						ON cir.chave_mulher = b.chave_mulher
					LEFT JOIN visita_domiciliar_recente vdr 
						ON vdr.chave_mulher = b.chave_mulher
					LEFT JOIN cadastro_domiciliar_recente cdr 
						ON cdr.chave_mulher = b.chave_mulher
					group by b.paciente_nome,
				--b.data_de_nascimento,
				--b.paciente_documento_cpf,
				--b.paciente_documento_cns,
				--b.paciente_telefone,
				b.chave_mulher,
				cir.data_cadastro_individual,
				cir.micro_area_cad_individual,
				cir.cnes_estabelecimento_cad_individual,
				cir.estabelecimento_cad_individual,
				cir.ine_equipe_cad_individual,
				cir.equipe_cad_individual,
				cir.acs_cad_individual,
				vdr.data_visita_acs,
				vdr.acs_visita_domiciliar,
				cdr.data_cadastro_dom_familia,
				cdr.micro_area_domicilio,
				cdr.cnes_estabelecimento_cad_dom_familia,
				cdr.estabelecimento_cad_dom_familia,
				cdr.ine_equipe_cad_dom_familia,
				cdr.equipe_cad_dom_familia,
				cdr.acs_cad_dom_familia
), relatorio_preliminar as (
		with base_preliminar as (
		SELECT 
				CASE
		            WHEN date_part('month'::text, current_date) >= 1::double precision AND date_part('month'::text, current_date) <= 4::double precision THEN concat(date_part('year'::text, current_date), '.Q1')
		            WHEN date_part('month'::text, current_date) >= 5::double precision AND date_part('month'::text, current_date) <= 8::double precision THEN concat(date_part('year'::text, current_date), '.Q2')
		            WHEN date_part('month'::text, current_date) >= 9::double precision AND date_part('month'::text, current_date) <= 12::double precision THEN concat(date_part('year'::text, current_date), '.Q3')
		            ELSE NULL::text
		        END AS quadrimestre_atual,
			   replace (tb1.chave_mulher, ' ','') as chave_mulher,
		       tb1.paciente_nome,
		       --tb1.paciente_documento_cns,
		       tb1.paciente_documento_cpf,
		       tb1.paciente_idade_atual,
		       tb1.idade_fim_quadrimestre,
		       tb1.data_de_nascimento,
		       tb2.data_ultimo_exame,
		       tb2.contagem_exames,
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
		       (tb2.data_ultimo_exame + 1095) as ultimo_exame_mais_36_meses,
		       CASE
		           WHEN date_part('month'::text, (tb2.data_ultimo_exame + 1095)) >= 1::double precision AND date_part('month'::text, (tb2.data_ultimo_exame + 1095)) <= 4::double precision THEN concat(date_part('year'::text, (tb2.data_ultimo_exame + 1095)), '.Q1')
		           WHEN date_part('month'::text, (tb2.data_ultimo_exame + 1095)) >= 5::double precision AND date_part('month'::text, (tb2.data_ultimo_exame + 1095)) <= 8::double precision THEN concat(date_part('year'::text, (tb2.data_ultimo_exame + 1095)), '.Q2')
		           WHEN date_part('month'::text, (tb2.data_ultimo_exame + 1095)) >= 9::double precision AND date_part('month'::text, (tb2.data_ultimo_exame + 1095)) <= 12::double precision THEN concat(date_part('year'::text, (tb2.data_ultimo_exame + 1095)), '.Q3')
				   else 'exame nunca realizado'
		       END AS quadrimestre_a_realizar_proximo_exame,
		       CASE
		           WHEN date_part('month'::text, (tb2.data_ultimo_exame + 1095)) >= 1::double precision AND date_part('month'::text, (tb2.data_ultimo_exame + 1095)) <= 4::double precision THEN concat(date_part('year'::text, (tb2.data_ultimo_exame + 1095)), '-04-30')
		           WHEN date_part('month'::text, (tb2.data_ultimo_exame + 1095)) >= 5::double precision AND date_part('month'::text, (tb2.data_ultimo_exame + 1095)) <= 8::double precision THEN concat(date_part('year'::text, (tb2.data_ultimo_exame + 1095)), '-08-31')
		           WHEN date_part('month'::text, (tb2.data_ultimo_exame + 1095)) >= 9::double precision AND date_part('month'::text, (tb2.data_ultimo_exame + 1095)) <= 12::double precision THEN concat(date_part('year'::text, (tb2.data_ultimo_exame + 1095)), '-12-31')
				   --else 'exame nunca realizado'
		       END AS data_limite_a_realizar_proximo_exame,
		       (tb2.data_ultimo_exame + 1095) - current_date as dias_para_proximo_exame,
		       tb2.cnes_estabelecimento_exame,
			   tb2.nome_estabelecimento_exame,
			     tb2.ine_equipe_exame,
			     tb2.nome_equipe_exame,
			     tb2.cns_profissional_exame,
			     tb2.nome_profissional_exame
		FROM selecao_mulheres_denominador tb1
		LEFT JOIN selecao_ultimo_exame tb2 ON tb1.chave_mulher = tb2.chave_mulher
		GROUP by 
				 quadrimestre_atual,
				 tb1.chave_mulher,
		         tb1.paciente_nome,
		         --tb1.paciente_documento_cns,
		         tb1.paciente_documento_cpf,
		         tb1.paciente_idade_atual,
		       	 tb1.idade_fim_quadrimestre,
		         tb1.data_de_nascimento,
		         tb2.data_ultimo_exame,
		         tb2.contagem_exames,
		         tb2.cnes_estabelecimento_exame,
			   	 tb2.nome_estabelecimento_exame,
			     tb2.ine_equipe_exame,
			     tb2.nome_equipe_exame,
			     tb2.cns_profissional_exame,
			     tb2.nome_profissional_exame
		) select 
				bp.quadrimestre_atual,
				bp.chave_mulher, 
				bp.paciente_nome,
				--bp.paciente_documento_cns,
				bp.paciente_documento_cpf,
				bp.paciente_idade_atual,
				bp.idade_fim_quadrimestre,
				bp.data_de_nascimento,
				bp.data_ultimo_exame,
				bp.contagem_exames,
				bp.realizou_exame_ultimos_36_meses,
				bp.ultimo_exame_mais_36_meses,
				bp.quadrimestre_a_realizar_proximo_exame,
				bp.data_limite_a_realizar_proximo_exame,
				bp.dias_para_proximo_exame,
				(bp.data_limite_a_realizar_proximo_exame::date - current_date) as dias_para_data_limite_fim_quadrimestre,
				case 
		       		when (bp.data_limite_a_realizar_proximo_exame::date - current_date) <= 0 then 'exame_vencido'
		       		when (bp.data_limite_a_realizar_proximo_exame::date - current_date) > 0 then 'exame_em_dia'
		       		else 'exame_nunca_realizado'
		       end as status_exame_mulher,
		       	 bp.cnes_estabelecimento_exame,
			   	 bp.nome_estabelecimento_exame,
			     bp.ine_equipe_exame,
			     bp.nome_equipe_exame,
			     bp.cns_profissional_exame,
			     bp.nome_profissional_exame
		 		from base_preliminar bp ) 
		 select * from relatorio_preliminar

 
 
