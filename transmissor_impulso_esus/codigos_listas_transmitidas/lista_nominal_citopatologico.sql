-- DENOMINADOR: todas as mulheres com idade entre 25 e 64 anos até o final do quadrimestre atual
WITH dados_cidadao_pec AS (
    SELECT 
        tfcp.co_seq_fat_cidadao_pec AS id_cidadao_pec,
        replace(tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento, ' ', '') AS chave_mulher,
        replace(tfcp.no_cidadao, '  ', ' ') AS paciente_nome,
        tempocidadaopec.dt_registro AS data_de_nascimento,
        (array_agg(tfcp.nu_cpf_cidadao) FILTER (WHERE tfcp.nu_cpf_cidadao IS NOT NULL) OVER (PARTITION BY replace(tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento, ' ', '') ORDER BY tfcp.co_seq_fat_cidadao_pec DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS paciente_documento_cpf,
	    (array_agg(tfcp.nu_cns) FILTER (WHERE tfcp.nu_cns IS NOT NULL) OVER (PARTITION BY replace(tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento, ' ', '') ORDER BY tfcp.co_seq_fat_cidadao_pec DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS paciente_documento_cns,
       	(array_agg(tfcp.st_faleceu) FILTER (WHERE tfcp.st_faleceu IS NOT NULL) OVER (PARTITION BY replace(tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento, ' ', '') ORDER BY tfcp.co_seq_fat_cidadao_pec DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS se_faleceu,
	    (array_agg(tds.ds_sexo) FILTER (WHERE tds.ds_sexo IS NOT NULL) OVER (PARTITION BY replace(tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento, ' ', '') ORDER BY tfcp.co_seq_fat_cidadao_pec DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS paciente_sexo,
	   	(array_agg(tfcp.nu_telefone_celular) FILTER (WHERE tfcp.nu_telefone_celular IS NOT NULL) OVER (PARTITION BY replace(tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento, ' ', '') ORDER BY tfcp.co_seq_fat_cidadao_pec DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS cidadao_telefone,
        date_part('year', age(CURRENT_DATE::timestamp with time zone, tempocidadaopec.dt_registro::timestamp with time zone))::integer AS paciente_idade_atual,
        CASE
            WHEN date_part('month', CURRENT_DATE) >= 1 AND date_part('month', CURRENT_DATE) <= 4 THEN concat(date_part('year', CURRENT_DATE ::date), '-04-30')
            WHEN date_part('month', CURRENT_DATE) >= 5 AND date_part('month', CURRENT_DATE) <= 8 THEN concat(date_part('year', CURRENT_DATE ::date), '-08-31')
            WHEN date_part('month', CURRENT_DATE) >= 9 AND date_part('month', CURRENT_DATE) <= 12 THEN concat(date_part('year', CURRENT_DATE ::date), '-12-31')
            ELSE NULL
        END AS data_fim_quadrimestre
    FROM tb_fat_cidadao_pec tfcp
    JOIN tb_dim_tempo tempocidadaopec ON tfcp.co_dim_tempo_nascimento = tempocidadaopec.co_seq_dim_tempo
    JOIN tb_dim_sexo tds ON tds.co_seq_dim_sexo = tfcp.co_dim_sexo
    WHERE tds.ds_sexo = 'Feminino'
),
selecao_mulheres_denominador as (
     SELECT 
	     dcp.chave_mulher,
	     dcp.paciente_nome,
	     dcp.data_de_nascimento,
	     dcp.paciente_documento_cpf,
	     dcp.paciente_documento_cns,
	     dcp.cidadao_telefone,
	     dcp.paciente_idade_atual,
	     date_part('year', age(dcp.data_fim_quadrimestre::timestamp with time zone, dcp.data_de_nascimento::timestamp with time zone))::integer AS idade_fim_quadrimestre 
	 FROM dados_cidadao_pec dcp
	 WHERE date_part('year', age(dcp.data_fim_quadrimestre::timestamp with time zone, dcp.data_de_nascimento::timestamp with time zone))::integer BETWEEN 25 AND 64
	 	  and coalesce(dcp.se_faleceu,0) != 1
	 group by 
	 	 dcp.chave_mulher,
	     dcp.paciente_nome,
	     dcp.data_de_nascimento,
	     dcp.paciente_documento_cpf,
	     dcp.paciente_documento_cns,
	     dcp.cidadao_telefone,
	     dcp.paciente_idade_atual,
	     dcp.data_fim_quadrimestre
),
-- HISTORICO DE REALIZACAO DE EXAMES CITOPATOLÓGICO
historico_exames_citopatologico as (
SELECT 
       replace(tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento,' ','') AS chave_mulher,
       tempoprocedimento.dt_registro AS data_realizacao_exame,
       tfpap.co_seq_fat_proced_atend_proced as id_registro,
       max(tfpap.co_seq_fat_proced_atend_proced) OVER (PARTITION BY tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento) AS codigo_ultimo_exame,
 		COALESCE(tdus.nu_cnes::text, '-'::text) as cnes_estabelecimento_exame,
     	COALESCE(tdus.no_unidade_saude ::text, 'Não informado'::text) as nome_estabelecimento_exame,
     	COALESCE(tde.nu_ine::text, '-'::text) as ine_equipe_exame,
     	COALESCE(tde.no_equipe::text, 'SEM EQUIPE'::text) as nome_equipe_exame,
     	COALESCE(tdp.nu_cns::text, '-'::text) as cns_profissional_exame,
    	COALESCE(tdp.no_profissional::text, 'Não informado'::text) as nome_profissional_exame
   FROM tb_fat_proced_atend_proced tfpap
   JOIN tb_dim_tempo tempoprocedimento ON tfpap.co_dim_tempo = tempoprocedimento.co_seq_dim_tempo
   JOIN tb_fat_cidadao_pec tfcp ON tfpap.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec
   JOIN tb_dim_procedimento procedimentos ON tfpap.co_dim_procedimento = procedimentos.co_seq_dim_procedimento
   JOIN tb_dim_cbo cbo ON tfpap.co_dim_cbo = cbo.co_seq_dim_cbo
   join tb_dim_unidade_saude tdus on tfpap.co_dim_unidade_saude = tdus.co_seq_dim_unidade_saude
   join tb_dim_equipe tde on tfpap.co_dim_equipe = tde.co_seq_dim_equipe
   join tb_dim_profissional tdp on tfpap.co_dim_profissional = tdp.co_seq_dim_profissional
   JOIN tb_dim_sexo tds ON tds.co_seq_dim_sexo = tfpap.co_dim_sexo
   WHERE (procedimentos.co_proced::text = ANY (ARRAY['0201020033'::CHARACTER varying::text,'ABPG010'::CHARACTER varying::text]))
     AND (cbo.nu_cbo::text ~~ ANY (ARRAY['%2235%'::text, '%2251%'::text, '%2252%'::text, '%2253%'::text, '%2231%'::text]))
     AND tfcp.st_faleceu <> 1
     and tds.ds_sexo = 'Feminino'
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
	WITH base AS (
		SELECT 
			mu.chave_mulher,
			tdt.dt_registro AS data_cadastro_individual,
			nullif(tfci.nu_micro_area::text, '-'::text) AS micro_area_cad_individual,
			uns.nu_cnes AS cnes_estabelecimento_cad_individual,
			uns.no_unidade_saude AS estabelecimento_cad_individual,
			eq.nu_ine AS ine_equipe_cad_individual,
			eq.no_equipe AS equipe_cad_individual,
			acs.no_profissional AS acs_cad_individual,
			tfct.co_fat_familia_territorio,
			cci.nu_celular_cidadao as cidadao_celular,
			st.ds_dim_situacao_trabalho as cidadao_situacao_trabalho,
			pct.ds_povo_comunidade_tradicional as cidadao_povo_comunidade_tradicional,
			idg.ds_identidade_genero as cidadao_identidade_genero,
			tdrc.ds_raca_cor as cidadao_raca_cor,
			tfci.st_plano_saude_privado as cidadao_plano_saude_privado,
			row_number() OVER (PARTITION BY mu.chave_mulher ORDER BY tdt.dt_registro DESC) = 1 AS ultimo_cadastro_individual
		FROM tb_fat_cad_individual tfci
		JOIN tb_fat_cidadao_pec tfcpec
			ON tfcpec.co_seq_fat_cidadao_pec = tfci.co_fat_cidadao_pec
		JOIN selecao_mulheres_denominador mu 
			ON mu.chave_mulher = replace(tfcpec.no_cidadao::text||tfcpec.co_dim_tempo_nascimento,' ','')
		left JOIN tb_fat_cidadao_territorio tfct
			ON tfct.co_fat_cad_individual = tfci.co_seq_fat_cad_individual 
		LEFT JOIN tb_dim_tempo tdt 
			ON tdt.co_seq_dim_tempo = tfci.co_dim_tempo
		LEFT JOIN tb_dim_equipe eq
			ON eq.co_seq_dim_equipe = tfci.co_dim_equipe
		LEFT JOIN tb_dim_profissional acs
			ON acs.co_seq_dim_profissional = tfci.co_dim_profissional
		LEFT JOIN tb_dim_unidade_saude uns
			ON uns.co_seq_dim_unidade_saude = tfci.co_dim_unidade_saude  
		LEFT JOIN tb_dim_situacao_trabalho st 
			ON st.co_seq_dim_situacao_trabalho  = tfci.co_dim_situacao_trabalho  
		LEFT JOIN tb_dim_povo_comunidad_trad pct 
			ON pct.co_seq_dim_povo_comunidad_trad = tfci.co_dim_povo_comunidad_trad  
		LEFT JOIN tb_dim_identidade_genero idg 
			ON idg.co_seq_dim_identidade_genero = tfci.co_dim_identidade_genero  
		LEFT JOIN tb_dim_raca_cor tdrc
			ON tdrc.co_seq_dim_raca_cor = tfci.co_dim_raca_cor  
		LEFT JOIN tb_cds_cad_individual cci 
			ON tfci.nu_uuid_ficha = cci.co_unico_ficha
		)
	SELECT * FROM base WHERE ultimo_cadastro_individual IS true
), 
atendimento_mais_recente AS (
-- Filtro de atendimento individual mais recente
with base as (	
SELECT 
			tfai.co_seq_fat_atd_ind::TEXT AS id_registro,
			tdt.dt_registro AS data_registro,
			mu.chave_mulher,
			tfcp.nu_telefone_celular AS cidadao_telefone,
			tdprof.nu_cns AS profissional_cns_atendimento_recente,
			tdprof.no_profissional AS profissional_atendimento_recente,
			uns.nu_cnes AS estabelecimento_cnes_atendimento_recente,
			uns.no_unidade_saude AS estabelecimento_atendimento_recente,
			eq.nu_ine AS ine_equipe_atendimento_recente,
			eq.no_equipe AS equipe_atendimento_recente,
			row_number() OVER (PARTITION BY mu.chave_mulher ORDER BY tfai.co_seq_fat_atd_ind DESC) = 1 AS ultimo_atendimento_individual
	    FROM tb_fat_atendimento_individual tfai
	    JOIN tb_dim_tempo tdt 
	    	ON tfai.co_dim_tempo = tdt.co_seq_dim_tempo
	    LEFT JOIN tb_dim_profissional tdprof
			ON tdprof.co_seq_dim_profissional = tfai.co_dim_profissional_1
		LEFT JOIN tb_dim_equipe eq
			ON eq.co_seq_dim_equipe = tfai.co_dim_equipe_1
		LEFT JOIN tb_dim_unidade_saude uns 
			ON uns.co_seq_dim_unidade_saude = tfai.co_dim_unidade_saude_1
	    JOIN tb_fat_cidadao_pec tfcp 
	    	ON tfcp.co_seq_fat_cidadao_pec = tfai.co_fat_cidadao_pec
	    JOIN tb_dim_tempo tempocidadaopec 
	    	ON tempocidadaopec.co_seq_dim_tempo = tfcp.co_dim_tempo_nascimento
	  	JOIN selecao_mulheres_denominador mu 
			ON mu.chave_mulher = replace(tfcp.no_cidadao::text||tfcp.co_dim_tempo_nascimento,' ','')
		) 	
		SELECT * FROM base WHERE ultimo_atendimento_individual IS true
),
visitas_ubs_12_meses as (
	SELECT 
			mu.chave_mulher,
			COUNT(*) as numero_visitas_ubs_ultimos_12_meses
	FROM tb_fat_atendimento_individual tfai
    JOIN tb_fat_cidadao_pec tfcp 
	    	ON tfcp.co_seq_fat_cidadao_pec = tfai.co_fat_cidadao_pec
	JOIN selecao_mulheres_denominador mu 
			ON mu.chave_mulher = replace(tfcp.no_cidadao::text||tfcp.co_dim_tempo_nascimento,' ','')
	JOIN tb_dim_local_atendimento tdla
            ON tfai.co_dim_local_atendimento = tdla.co_seq_dim_local_atendimento
	WHERE 
		tdla.ds_local_atendimento = 'UBS'
        AND tfai.dt_inicial_atendimento >= (CURRENT_DATE - INTERVAL '12 months')
    GROUP by mu.chave_mulher
),
visita_domiciliar_recente AS (
	WITH base AS (
		SELECT 
			mu.chave_mulher,
		    tfcpec.co_seq_fat_cidadao_pec,
			tdt.dt_registro AS data_visita_acs,
			acs.no_profissional AS acs_visita_domiciliar,
			row_number() OVER (PARTITION BY mu.chave_mulher ORDER BY tdt.dt_registro DESC) = 1 AS ultima_visita_domiciliar
		FROM tb_fat_visita_domiciliar visitadomiciliar
		JOIN tb_fat_cidadao_pec tfcpec
			ON tfcpec.co_seq_fat_cidadao_pec = visitadomiciliar.co_fat_cidadao_pec 
		JOIN selecao_mulheres_denominador mu
			ON mu.chave_mulher = replace(tfcpec.no_cidadao::text||tfcpec.co_dim_tempo_nascimento,' ','')
		LEFT JOIN tb_dim_profissional acs
			ON acs.co_seq_dim_profissional = visitadomiciliar.co_dim_profissional
		LEFT JOIN tb_dim_tempo tdt 
			ON tdt.co_seq_dim_tempo = visitadomiciliar.co_dim_tempo
		)
	SELECT * FROM base WHERE ultima_visita_domiciliar IS TRUE 
), 
cadastro_domiciliar_recente AS (
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
			NULLIF(concat(cadomiciliar.no_logradouro, ', ', cadomiciliar.nu_num_logradouro), ', '::text) AS paciente_endereco,
			row_number() OVER (PARTITION BY mu.chave_mulher ORDER BY tdt.dt_registro DESC) = 1 AS ultimo_cadastro_domiciliar_familia
		FROM tb_fat_cad_dom_familia caddomiciliarfamilia
		JOIN tb_fat_cad_domiciliar cadomiciliar
			ON cadomiciliar.co_seq_fat_cad_domiciliar = caddomiciliarfamilia.co_fat_cad_domiciliar
		JOIN tb_fat_cidadao_pec tfcpec
			ON tfcpec.co_seq_fat_cidadao_pec = caddomiciliarfamilia.co_fat_cidadao_pec
		JOIN selecao_mulheres_denominador mu
			ON mu.chave_mulher = replace(tfcpec.no_cidadao::text || tfcpec.co_dim_tempo_nascimento, ' ', '')
		LEFT JOIN tb_dim_tempo tdt
			ON tdt.co_seq_dim_tempo = caddomiciliarfamilia.co_dim_tempo
		LEFT JOIN tb_dim_equipe eq
			ON eq.co_seq_dim_equipe = caddomiciliarfamilia.co_dim_equipe
		LEFT JOIN tb_dim_profissional acs
			ON acs.co_seq_dim_profissional = caddomiciliarfamilia.co_dim_profissional
		LEFT JOIN tb_dim_unidade_saude uns
			ON uns.co_seq_dim_unidade_saude = caddomiciliarfamilia.co_dim_unidade_saude
	)
	SELECT * FROM base WHERE ultimo_cadastro_domiciliar_familia IS true
),
infos_mulheres_atendimento_individual_recente AS (
	SELECT
		b.chave_mulher,
		cir.data_cadastro_individual,
		cir.micro_area_cad_individual,
		cir.cnes_estabelecimento_cad_individual,
		cir.estabelecimento_cad_individual,
		cir.ine_equipe_cad_individual,
		cir.equipe_cad_individual,
		cir.acs_cad_individual,
		ar.profissional_cns_atendimento_recente,
		ar.profissional_atendimento_recente,
		ar.estabelecimento_cnes_atendimento_recente,
		ar.estabelecimento_atendimento_recente,
		ar.ine_equipe_atendimento_recente,
		ar.equipe_atendimento_recente,
		ar.data_registro as data_atendimento_recente,
		vdr.data_visita_acs,
		vdr.acs_visita_domiciliar,
		cdr.data_cadastro_dom_familia,
		cdr.micro_area_domicilio,
		cdr.cnes_estabelecimento_cad_dom_familia,
		cdr.estabelecimento_cad_dom_familia,
		cdr.ine_equipe_cad_dom_familia,
		cdr.equipe_cad_dom_familia,
		cdr.acs_cad_dom_familia,
		cdr.paciente_endereco,
		cir.co_fat_familia_territorio,
		b.cidadao_telefone,
		cir.cidadao_celular,
		cir.cidadao_situacao_trabalho,
		cir.cidadao_povo_comunidade_tradicional,
		cir.cidadao_identidade_genero,
		cir.cidadao_raca_cor,
		cir.cidadao_plano_saude_privado
	FROM selecao_mulheres_denominador b
	LEFT JOIN cadastro_individual_recente cir
		ON cir.chave_mulher = b.chave_mulher
	LEFT JOIN visita_domiciliar_recente vdr
		ON vdr.chave_mulher = b.chave_mulher
	LEFT JOIN cadastro_domiciliar_recente cdr
		ON cdr.chave_mulher = b.chave_mulher
	left join atendimento_mais_recente ar
		on ar.chave_mulher = b.chave_mulher
	GROUP BY
		b.chave_mulher,
		cir.data_cadastro_individual,
		cir.micro_area_cad_individual,
		cir.cnes_estabelecimento_cad_individual,
		cir.estabelecimento_cad_individual,
		cir.ine_equipe_cad_individual,
		cir.equipe_cad_individual,
		cir.acs_cad_individual,
		ar.profissional_cns_atendimento_recente,
		ar.profissional_atendimento_recente,
		ar.estabelecimento_cnes_atendimento_recente,
		ar.estabelecimento_atendimento_recente,
		ar.ine_equipe_atendimento_recente,
		ar.equipe_atendimento_recente,
		ar.data_registro,
		vdr.data_visita_acs,
		vdr.acs_visita_domiciliar,
		cdr.data_cadastro_dom_familia,
		cdr.micro_area_domicilio,
		cdr.cnes_estabelecimento_cad_dom_familia,
		cdr.estabelecimento_cad_dom_familia,
		cdr.ine_equipe_cad_dom_familia,
		cdr.equipe_cad_dom_familia,
		cdr.acs_cad_dom_familia,
		cdr.paciente_endereco,
		cir.co_fat_familia_territorio,
		b.cidadao_telefone,
		cir.cidadao_celular,
		cir.cidadao_situacao_trabalho,
		cir.cidadao_povo_comunidade_tradicional,
		cir.cidadao_identidade_genero,
		cir.cidadao_raca_cor,
		cir.cidadao_plano_saude_privado
), 
indicador_regras_de_negocio as (
	SELECT
		CASE
			WHEN date_part('month', current_date) >= 1::double precision AND date_part('month', current_date) <= 4::double precision THEN concat(date_part('year', current_date), '.Q1')
			WHEN date_part('month', current_date) >= 5::double precision AND date_part('month', current_date) <= 8::double precision THEN concat(date_part('year', current_date), '.Q2')
			WHEN date_part('month', current_date) >= 9::double precision AND date_part('month', current_date) <= 12::double precision THEN concat(date_part('year', current_date), '.Q3')
			ELSE NULL::text
		END AS quadrimestre_atual,
		replace(tb1.chave_mulher, ' ', '') AS chave_mulher,
		tb1.paciente_nome,
		tb1.paciente_documento_cpf,
		tb1.paciente_documento_cns,
		tb1.paciente_idade_atual,
		tb1.idade_fim_quadrimestre,
		tb1.data_de_nascimento,
		tb2.data_ultimo_exame,
		date_part('year', age(tb2.data_ultimo_exame ::timestamp with time zone, tb1.data_de_nascimento::timestamp with time zone))::integer as idade_realizou_ultimo_exame,
		CASE
			WHEN CURRENT_DATE - INTERVAL '36 months' <= tb2.data_ultimo_exame and (date_part('year', age(tb2.data_ultimo_exame ::timestamp with time zone, tb1.data_de_nascimento::timestamp with time zone)))::integer between 25 and 64 THEN TRUE
			ELSE FALSE
		END AS realizou_exame_ultimos_36_meses,
		(tb2.data_ultimo_exame + INTERVAL '36 months') AS ultimo_exame_mais_36_meses,
		CASE
			WHEN date_part('month', (tb2.data_ultimo_exame + INTERVAL '36 months')) >= 1::double precision AND date_part('month', (tb2.data_ultimo_exame + INTERVAL '36 months')) <= 4::double precision THEN concat(date_part('year', (tb2.data_ultimo_exame + INTERVAL '36 months')), '.Q1')
			WHEN date_part('month', (tb2.data_ultimo_exame + INTERVAL '36 months')) >= 5::double precision AND date_part('month', (tb2.data_ultimo_exame + INTERVAL '36 months')) <= 8::double precision THEN concat(date_part('year', (tb2.data_ultimo_exame + INTERVAL '36 months')), '.Q2')
			WHEN date_part('month', (tb2.data_ultimo_exame + INTERVAL '36 months')) >= 9::double precision AND date_part('month', (tb2.data_ultimo_exame + INTERVAL '36 months')) <= 12::double precision THEN concat(date_part('year', (tb2.data_ultimo_exame + INTERVAL '36 months')), '.Q3')
			ELSE 'exame_nunca_realizado'
		END AS quadrimestre_a_realizar_proximo_exame,
		CASE
			WHEN tb2.data_ultimo_exame IS NULL 
			or (tb2.data_ultimo_exame + INTERVAL '36 months') < current_date 
			or ((CASE
						WHEN date_part('month', (tb2.data_ultimo_exame + INTERVAL '36 months')) >= 1::double precision AND date_part('month', (tb2.data_ultimo_exame + INTERVAL '36 months')) <= 4::double precision THEN concat(date_part('year', (tb2.data_ultimo_exame + INTERVAL '36 months')), '.Q1')
						WHEN date_part('month', (tb2.data_ultimo_exame + INTERVAL '36 months')) >= 5::double precision AND date_part('month', (tb2.data_ultimo_exame + INTERVAL '36 months')) <= 8::double precision THEN concat(date_part('year', (tb2.data_ultimo_exame + INTERVAL '36 months')), '.Q2')
						WHEN date_part('month', (tb2.data_ultimo_exame + INTERVAL '36 months')) >= 9::double precision AND date_part('month', (tb2.data_ultimo_exame + INTERVAL '36 months')) <= 12::double precision THEN concat(date_part('year', (tb2.data_ultimo_exame + INTERVAL '36 months')), '.Q3')
				  END ) = ( CASE
								WHEN date_part('month', current_date) >= 1::double precision AND date_part('month', current_date) <= 4::double precision THEN concat(date_part('year', current_date), '.Q1')
								WHEN date_part('month', current_date) >= 5::double precision AND date_part('month', current_date) <= 8::double precision THEN concat(date_part('year', current_date), '.Q2')
								WHEN date_part('month', current_date) >= 9::double precision AND date_part('month', current_date) <= 12::double precision THEN concat(date_part('year', current_date), '.Q3')
							end 
			) 
			)
			or date_part('year', age(tb2.data_ultimo_exame ::timestamp with time zone, tb1.data_de_nascimento::timestamp with time zone)) < 25 and ((tb2.data_ultimo_exame + INTERVAL '36 months')::date - current_date) > 0
			THEN (
				CASE
					WHEN date_part('month', CURRENT_DATE) >= 1 AND date_part('month', CURRENT_DATE) <= 4 THEN concat(date_part('year', CURRENT_DATE::date), '-04-30')::date
					WHEN date_part('month', CURRENT_DATE) >= 5 AND date_part('month', CURRENT_DATE) <= 8 THEN concat(date_part('year', CURRENT_DATE::date), '-08-31')::date
					WHEN date_part('month', CURRENT_DATE) >= 9 AND date_part('month', CURRENT_DATE) <= 12 THEN concat(date_part('year', CURRENT_DATE::date), '-12-31')::date
				END
			)
			ELSE (tb2.data_ultimo_exame + INTERVAL '36 months')::date
		END AS data_limite_a_realizar_proximo_exame,
		CASE
			when date_part('year', age(tb2.data_ultimo_exame ::timestamp with time zone, tb1.data_de_nascimento::timestamp with time zone)) < 25 and ((tb2.data_ultimo_exame + INTERVAL '36 months')::date - current_date) > 0 then 'exame_realizado_antes_dos_25'
			WHEN (CASE
						WHEN date_part('month', (tb2.data_ultimo_exame + INTERVAL '36 months')) >= 1::double precision AND date_part('month', (tb2.data_ultimo_exame + INTERVAL '36 months')) <= 4::double precision THEN concat(date_part('year', (tb2.data_ultimo_exame + INTERVAL '36 months')), '.Q1')
						WHEN date_part('month', (tb2.data_ultimo_exame + INTERVAL '36 months')) >= 5::double precision AND date_part('month', (tb2.data_ultimo_exame + INTERVAL '36 months')) <= 8::double precision THEN concat(date_part('year', (tb2.data_ultimo_exame + INTERVAL '36 months')), '.Q2')
						WHEN date_part('month', (tb2.data_ultimo_exame + INTERVAL '36 months')) >= 9::double precision AND date_part('month', (tb2.data_ultimo_exame + INTERVAL '36 months')) <= 12::double precision THEN concat(date_part('year', (tb2.data_ultimo_exame + INTERVAL '36 months')), '.Q3')
				  END ) = ( CASE
								WHEN date_part('month', current_date) >= 1::double precision AND date_part('month', current_date) <= 4::double precision THEN concat(date_part('year', current_date), '.Q1')
								WHEN date_part('month', current_date) >= 5::double precision AND date_part('month', current_date) <= 8::double precision THEN concat(date_part('year', current_date), '.Q2')
								WHEN date_part('month', current_date) >= 9::double precision AND date_part('month', current_date) <= 12::double precision THEN concat(date_part('year', current_date), '.Q3')
							end 
			) THEN 'exame_vence_no_quadrimestre_atual'			
			WHEN ((tb2.data_ultimo_exame + INTERVAL '36 months')::date - current_date) < 0 THEN 'exame_vencido'
			WHEN ((tb2.data_ultimo_exame + INTERVAL '36 months')::date - current_date) > 0 THEN 'exame_em_dia'
			when tb2.data_ultimo_exame is null then 'exame_nunca_realizado'
			else 'status'
		END as status_exame,
		tb2.cnes_estabelecimento_exame,
		tb2.nome_estabelecimento_exame,
		tb2.ine_equipe_exame,
		tb2.nome_equipe_exame,
		tb2.cns_profissional_exame,
		tb2.nome_profissional_exame
		FROM selecao_mulheres_denominador tb1
		LEFT JOIN selecao_ultimo_exame tb2 ON tb1.chave_mulher = tb2.chave_mulher
		GROUP BY 
				 quadrimestre_atual,
				 tb1.chave_mulher,
		         tb1.paciente_nome,
		         tb1.paciente_documento_cpf,
		         tb1.paciente_documento_cns,
		         tb1.paciente_idade_atual,
		       	 tb1.idade_fim_quadrimestre,
		         tb1.data_de_nascimento,
		         tb2.data_ultimo_exame,
		         tb2.cnes_estabelecimento_exame,
			   	 tb2.nome_estabelecimento_exame,
			     tb2.ine_equipe_exame,
			     tb2.nome_equipe_exame,
			     tb2.cns_profissional_exame,
			     tb2.nome_profissional_exame	
		),
lista_citopatologico as (	
		 select 
		 irn.quadrimestre_atual,
		 irn.chave_mulher, 
		 irn.paciente_nome,
		 irn.paciente_documento_cns as cidadao_cns,
		 irn.paciente_documento_cpf as cidadao_cpf,
		 irn.paciente_idade_atual,
		 irn.data_de_nascimento as dt_nascimento,
		 irn.data_ultimo_exame as dt_ultimo_exame,
		 irn.realizou_exame_ultimos_36_meses,
		 irn.ultimo_exame_mais_36_meses as data_projetada_proximo_exame,
		 irn.status_exame,
		 irn.data_limite_a_realizar_proximo_exame,
		 irn.cnes_estabelecimento_exame,
		 irn.nome_estabelecimento_exame,
		 irn.ine_equipe_exame,
		 irn.nome_equipe_exame,
		 irn.nome_profissional_exame,
		 atr.data_cadastro_individual as dt_ultimo_cadastro,
		 atr.estabelecimento_cad_individual as estabelecimento_nome_cadastro,
		 atr.cnes_estabelecimento_cad_individual as estabelecimento_cnes_cadastro,
		 atr.ine_equipe_cad_individual as equipe_ine_cadastro,
		 atr.equipe_cad_individual as equipe_nome_cadastro,
		 atr.acs_cad_individual as acs_nome_cadastro,
		 atr.data_atendimento_recente as dt_ultimo_atendimento,
		 atr.estabelecimento_cnes_atendimento_recente as estabelecimento_cnes_ultimo_atendimento,
		 atr.estabelecimento_atendimento_recente as estabelecimento_nome_ultimo_atendimento,
		 atr.ine_equipe_atendimento_recente as equipe_ine_ultimo_atendimento,
		 atr.equipe_atendimento_recente as equipe_nome_ultimo_atendimento,
		 atr.profissional_atendimento_recente as acs_nome_ultimo_atendimento,
		 atr.acs_visita_domiciliar as acs_nome_visita, 
		 atr.co_fat_familia_territorio,
		 atr.cidadao_telefone,
		 atr.cidadao_celular,
		 atr.cidadao_situacao_trabalho,
		 atr.cidadao_povo_comunidade_tradicional,
	   	 atr.cidadao_identidade_genero,
		 atr.cidadao_raca_cor,
		 atr.cidadao_plano_saude_privado,
		 vu.numero_visitas_ubs_ultimos_12_meses,
		 now() as criacao_data,
		 now() as atualizacao_data
		 from indicador_regras_de_negocio irn
		 left join infos_mulheres_atendimento_individual_recente atr on irn.chave_mulher = atr.chave_mulher
		 left join visitas_ubs_12_meses vu on irn.chave_mulher = vu.chave_mulher
)
select * from lista_citopatologico