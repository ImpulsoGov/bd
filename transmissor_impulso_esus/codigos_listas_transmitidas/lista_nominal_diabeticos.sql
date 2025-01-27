-- DENOMINADOR - todas pessoas diabetes autorreferida ou diagnósticada
WITH possui_diabetes_autoreferida AS (
	WITH ultimo_cadastro_individual AS (
		SELECT 
			tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento AS chave_paciente,
			cadastro.st_diabete,
			ROW_NUMBER() OVER (PARTITION BY tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento ORDER BY cadastro.co_seq_fat_cad_individual DESC) = 1 AS ultimo_cadastro_individual
		FROM tb_fat_cad_individual cadastro
		JOIN tb_fat_cidadao_pec tfcp 
			ON cadastro.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec 
		JOIN tb_dim_tempo tempo 
			ON cadastro.co_dim_tempo = tempo.co_seq_dim_tempo
		WHERE cadastro.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec 
	)
	SELECT 
		ci.chave_paciente,
		ci.st_diabete = 1 AS possui_diabetes_autoreferida
	FROM ultimo_cadastro_individual ci 
	WHERE ultimo_cadastro_individual IS TRUE 
		AND ci.st_diabete = 1
)
, possui_diabetes_diagnosticada AS (
	SELECT 
		DISTINCT 
		tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento AS chave_paciente,
		TRUE AS possui_diabetes_diagnosticada
	FROM tb_fat_atendimento_individual atendimento
	JOIN tb_fat_cidadao_pec tfcp 
		ON atendimento.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec 
	LEFT JOIN tb_dim_tempo tempo 
		ON atendimento.co_dim_tempo = tempo.co_seq_dim_tempo
	LEFT JOIN tb_fat_atd_ind_problemas problemas 
		ON atendimento.co_seq_fat_atd_ind = problemas.co_fat_atd_ind
	LEFT JOIN tb_dim_cbo cbo 
		ON problemas.co_dim_cbo_1 = cbo.co_seq_dim_cbo
	LEFT JOIN tb_dim_ciap ciap 
		ON problemas.co_dim_ciap = ciap.co_seq_dim_ciap
	LEFT JOIN tb_dim_cid cid 
		ON problemas.co_dim_cid = cid.co_seq_dim_cid 
	WHERE ((ciap.nu_ciap::text = ANY (ARRAY['T89'::character varying::text, 'T90'::character varying::text, 'ABP006'::character varying::text])) 
		OR (cid.nu_cid::text = ANY (ARRAY['E10'::character varying::text, 'E100'::character varying::text, 'E101'::character varying::text, 'E102'::character varying::text, 'E103'::character varying::text, 'E104'::character varying::text, 'E105'::character varying::text, 'E106'::character varying::text, 'E107'::character varying::text, 'E108'::character varying::text, 'E109'::character varying::text, 'E11'::character varying::text, 'E110'::character varying::text, 'E111'::character varying::text, 'E112'::character varying::text, 'E113'::character varying::text, 'E114'::character varying::text, 'E115'::character varying::text, 'E116'::character varying::text, 'E117'::character varying::text, 'E118'::character varying::text, 'E119'::character varying::text, 'E12'::character varying::text, 'E120'::character varying::text, 'E121'::character varying::text, 'E122'::character varying::text, 'E123'::character varying::text, 'E124'::character varying::text, 'E125'::character varying::text, 'E126'::character varying::text, 'E127'::character varying::text, 'E128'::character varying::text, 'E129'::character varying::text, 'E13'::character varying::text, 'E130'::character varying::text, 'E131'::character varying::text, 'E132'::character varying::text, 'E133'::character varying::text, 'E134'::character varying::text, 'E135'::character varying::text, 'E136'::character varying::text, 'E137'::character varying::text, 'E138'::character varying::text, 'E139'::character varying::text, 'E14'::character varying::text, 'E140'::character varying::text, 'E141'::character varying::text, 'E142'::character varying::text, 'E143'::character varying::text, 'E144'::character varying::text, 'E145'::character varying::text, 'E146'::character varying::text, 'E147'::character varying::text, 'E148'::character varying::text, 'E149'::character varying::text, 'O240'::character varying::text, 'O241'::character varying::text, 'O242'::character varying::text, 'O243'::character varying::text])))
		AND (cbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text])) 
		AND tempo.nu_ano <> 3000 
		AND tempo.dt_registro <= current_date
)
, denominador_diabeticos AS (
	SELECT 
		DISTINCT 
		tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento AS chave_paciente,
		tfcp.no_cidadao AS cidadao_nome,
		tdt.dt_registro AS dt_nascimento,
		(array_agg(tfcp.no_social_cidadao) FILTER (WHERE tfcp.no_social_cidadao IS NOT NULL) OVER (PARTITION BY tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento ORDER BY tfcp.co_seq_fat_cidadao_pec DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS cidadao_nome_social,
		(array_agg(tfcp.nu_cpf_cidadao) FILTER (WHERE tfcp.nu_cpf_cidadao IS NOT NULL) OVER (PARTITION BY tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento ORDER BY tfcp.co_seq_fat_cidadao_pec DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS cidadao_cpf,
		(array_agg(tfcp.nu_cns) FILTER (WHERE tfcp.nu_cns IS NOT NULL) OVER (PARTITION BY tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento ORDER BY tfcp.co_seq_fat_cidadao_pec DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS cidadao_cns,
		(array_agg(tds.ds_sexo) FILTER (WHERE tds.ds_sexo IS NOT NULL) OVER (PARTITION BY tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento ORDER BY tfcp.co_seq_fat_cidadao_pec DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS cidadao_sexo,
		(array_agg(tfcp.nu_telefone_celular) FILTER (WHERE tfcp.nu_telefone_celular IS NOT NULL) OVER (PARTITION BY tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento ORDER BY tfcp.co_seq_fat_cidadao_pec DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS cidadao_telefone,
		FIRST_VALUE(tfcp.co_seq_fat_cidadao_pec) OVER (PARTITION BY tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento ORDER BY tfcp.co_seq_fat_cidadao_pec DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS co_seq_fat_cidadao_pec, -- valor arbitrario
		COALESCE(daref.possui_diabetes_autoreferida,FALSE) AS possui_diabetes_autoreferida,
		COALESCE(ddia.possui_diabetes_diagnosticada,FALSE) AS possui_diabetes_diagnosticada,
		COALESCE(FIRST_VALUE(tfcp.st_faleceu) OVER (PARTITION BY tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento ORDER BY tfcp.co_seq_fat_cidadao_pec DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),0) AS se_faleceu
	FROM tb_fat_cidadao_pec tfcp
	LEFT JOIN tb_dim_tempo tdt 
		ON tfcp.co_dim_tempo_nascimento = tdt.co_seq_dim_tempo
	LEFT JOIN tb_dim_sexo tds 
		ON tfcp.co_dim_sexo = tds.co_seq_dim_sexo
	LEFT JOIN possui_diabetes_autoreferida daref
		ON daref.chave_paciente = tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento
	LEFT JOIN possui_diabetes_diagnosticada ddia
		ON ddia.chave_paciente = tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento
	WHERE (ddia.possui_diabetes_diagnosticada OR daref.possui_diabetes_autoreferida)
)
-- NUMERADOR
, hemoglobina_glicada AS (
WITH ultima_ficha_procedimento AS (
	SELECT 
		dd.chave_paciente,
		tempo.dt_registro AS dt_solicitacao_hemoglobina_glicada_mais_recente,
		eq.nu_ine AS equipe_ine_procedimento,
		eq.no_equipe AS equipe_nome_procedimento,
		prof.no_profissional AS profissional_nome_procedimento,
		ROW_NUMBER() OVER (PARTITION BY dd.chave_paciente ORDER BY tempo.dt_registro DESC, tfaip.co_dim_procedimento_solicitado DESC) = 1 AS ultimo_procedimento
	FROM tb_fat_atd_ind_procedimentos tfaip
	JOIN tb_fat_cidadao_pec tfcp 
		ON tfaip.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec 
	JOIN denominador_diabeticos dd 
		ON dd.chave_paciente = tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento
	JOIN tb_dim_procedimento tdp 
		ON tdp.co_seq_dim_procedimento = tfaip.co_dim_procedimento_solicitado
	JOIN tb_dim_cbo cbo 
		ON cbo.co_seq_dim_cbo = tfaip.co_dim_cbo_1
	JOIN tb_dim_tempo tempo 
		ON tempo.co_seq_dim_tempo = tfaip.co_dim_tempo
	LEFT JOIN tb_dim_equipe eq
		ON eq.co_seq_dim_equipe = tfaip.co_dim_equipe_1
	LEFT JOIN tb_dim_profissional prof
		ON prof.co_seq_dim_profissional = tfaip.co_dim_profissional_1
	WHERE (cbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text])) 
		AND (tdp.co_proced::text = ANY (ARRAY['0202010503'::text, 'ABEX008'::text])) 
		AND tempo.dt_registro <= current_date
		AND tempo.nu_ano <> 3000 
	)
	SELECT 
		*
	FROM ultima_ficha_procedimento
	WHERE ultimo_procedimento IS TRUE 
)
, consulta_diabetes AS (
	SELECT 
		dd.chave_paciente,
		max(tempo.dt_registro) AS dt_consulta_mais_recente
	FROM tb_fat_atendimento_individual atendimento
	JOIN tb_fat_cidadao_pec tfcp 
		ON atendimento.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec 
	JOIN denominador_diabeticos dd 
		ON dd.chave_paciente = tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento
	LEFT  JOIN tb_dim_tempo tempo 
		ON atendimento.co_dim_tempo = tempo.co_seq_dim_tempo
	LEFT  JOIN tb_fat_atd_ind_problemas problemas 
		ON atendimento.co_seq_fat_atd_ind = problemas.co_fat_atd_ind
	LEFT JOIN tb_dim_cbo cbo 
		ON problemas.co_dim_cbo_1 = cbo.co_seq_dim_cbo 
	LEFT JOIN tb_dim_ciap ciap 
		ON problemas.co_dim_ciap = ciap.co_seq_dim_ciap
	LEFT JOIN tb_dim_cid cid 
		ON problemas.co_dim_cid = cid.co_seq_dim_cid
	WHERE ((cid.nu_cid::text = ANY (ARRAY['E10'::character varying::text, 'E100'::character varying::text, 'E101'::character varying::text, 'E102'::character varying::text, 'E103'::character varying::text, 'E104'::character varying::text, 'E105'::character varying::text, 'E106'::character varying::text, 'E107'::character varying::text, 'E108'::character varying::text, 'E109'::character varying::text, 'E11'::character varying::text, 'E110'::character varying::text, 'E111'::character varying::text, 'E112'::character varying::text, 'E113'::character varying::text, 'E114'::character varying::text, 'E115'::character varying::text, 'E116'::character varying::text, 'E117'::character varying::text, 'E118'::character varying::text, 'E119'::character varying::text, 'E12'::character varying::text, 'E120'::character varying::text, 'E121'::character varying::text, 'E122'::character varying::text, 'E123'::character varying::text, 'E124'::character varying::text, 'E125'::character varying::text, 'E126'::character varying::text, 'E127'::character varying::text, 'E128'::character varying::text, 'E129'::character varying::text, 'E13'::character varying::text, 'E130'::character varying::text, 'E131'::character varying::text, 'E132'::character varying::text, 'E133'::character varying::text, 'E134'::character varying::text, 'E135'::character varying::text, 'E136'::character varying::text, 'E137'::character varying::text, 'E138'::character varying::text, 'E139'::character varying::text, 'E14'::character varying::text, 'E140'::character varying::text, 'E141'::character varying::text, 'E142'::character varying::text, 'E143'::character varying::text, 'E144'::character varying::text, 'E145'::character varying::text, 'E146'::character varying::text, 'E147'::character varying::text, 'E148'::character varying::text, 'E149'::character varying::text, 'O240'::character varying::text, 'O241'::character varying::text, 'O242'::character varying::text, 'O243'::character varying::text])) 
		OR (ciap.nu_ciap::text = ANY (ARRAY['T89'::character varying::text, 'T90'::character varying::text, 'ABP006'::character varying::text]))) 
		AND tempo.dt_registro <= current_date
		AND (cbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text]))
	GROUP BY 1
)
-- Informações de vinculação
, cadastro_individual_recente AS (
-- Dados do cadastro individual (dados para vinculação de equipe e ACS do cidadao)
				SELECT 
						dd.chave_paciente,
						tdt.dt_registro AS data_ultimo_cadastro,
						tfci.nu_micro_area AS micro_area_cad_individual,
						uns.nu_cnes AS estabelecimento_cnes_cadastro,
						uns.no_unidade_saude AS estabelecimento_nome_cadastro,
						eq.nu_ine AS equipe_ine_cadastro,
						eq.no_equipe AS equipe_nome_cadastro,
						acs.no_profissional AS acs_nome_cadastro,
						COALESCE(cidadaoterritoriorecente.st_mudou_se,0) AS se_mudou,
						ROW_NUMBER() OVER (PARTITION BY dd.chave_paciente ORDER BY tfci.co_seq_fat_cad_individual DESC) = 1 AS ultimo_cadastro_individual
				FROM tb_fat_cad_individual tfci
				JOIN tb_fat_cidadao_pec tfcp
						ON tfcp.co_seq_fat_cidadao_pec = tfci.co_fat_cidadao_pec
				JOIN denominador_diabeticos dd  
						ON dd.chave_paciente = tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento
				LEFT JOIN tb_dim_tempo tdt 
						ON tdt.co_seq_dim_tempo = tfci.co_dim_tempo
				LEFT JOIN tb_dim_equipe eq
						ON eq.co_seq_dim_equipe = tfci.co_dim_equipe
				LEFT JOIN tb_dim_profissional acs
						ON acs.co_seq_dim_profissional = tfci.co_dim_profissional
				LEFT JOIN tb_dim_unidade_saude uns
						ON uns.co_seq_dim_unidade_saude = tfci.co_dim_unidade_saude 
				LEFT JOIN tb_fat_cidadao_territorio cidadaoterritoriorecente 
						ON cidadaoterritoriorecente.co_fat_cad_individual = tfci.co_seq_fat_cad_individual
) 
, visita_domiciliar_recente AS (
-- Dados das visitas domiciliares realizadas pelos ACS (dados para vinculação de ACS da mulher)
		SELECT 
			dd.chave_paciente,
			tfcp.co_seq_fat_cidadao_pec,
			tdt.dt_registro AS data_visita_acs,
			acs.no_profissional AS acs_nome_visita,
			row_number() OVER (PARTITION BY dd.chave_paciente ORDER BY tdt.dt_registro DESC) = 1 AS ultima_visita_domiciliar
		FROM tb_fat_visita_domiciliar visitadomiciliar
		JOIN tb_fat_cidadao_pec tfcp
			ON tfcp.co_seq_fat_cidadao_pec = visitadomiciliar.co_fat_cidadao_pec 
		JOIN denominador_diabeticos dd 
			ON dd.chave_paciente = tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento
		LEFT JOIN tb_dim_profissional acs
			ON acs.co_seq_dim_profissional = visitadomiciliar.co_dim_profissional
		LEFT JOIN tb_dim_tempo tdt 
			ON tdt.co_seq_dim_tempo = visitadomiciliar.co_dim_tempo
		)
, cadastro_domiciliar_recente AS (
-- Dados do cadastro da família e do domicílio da mulher (dados para vinculação de ACS da mulher)
		SELECT 
			dd.chave_paciente,
			tdt.dt_registro AS data_cadastro_dom_familia,
			caddomiciliarfamilia.nu_micro_area AS micro_area_domicilio,
			uns.nu_cnes AS cnes_estabelecimento_cad_dom_familia,
			uns.no_unidade_saude AS estabelecimento_cad_dom_familia,
			eq.nu_ine AS ine_equipe_cad_dom_familia,
			eq.no_equipe AS equipe_cad_dom_familia,
			acs.no_profissional AS acs_cad_dom_familia,
			NULLIF(concat(cadomiciliar.no_logradouro, ', ', cadomiciliar.nu_num_logradouro), ', '::text) AS paciente_endereco,
			row_number() OVER (PARTITION BY dd.chave_paciente ORDER BY tdt.dt_registro DESC) = 1  AS ultimo_cadastro_domiciliar_familia
		FROM tb_fat_cad_dom_familia caddomiciliarfamilia
		JOIN tb_fat_cad_domiciliar cadomiciliar 
			ON cadomiciliar.co_seq_fat_cad_domiciliar = caddomiciliarfamilia.co_fat_cad_domiciliar
		JOIN tb_fat_cidadao_pec tfcp
			ON tfcp.co_seq_fat_cidadao_pec = caddomiciliarfamilia.co_fat_cidadao_pec 
		JOIN denominador_diabeticos dd 
			ON dd.chave_paciente = tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento
		LEFT JOIN tb_dim_tempo tdt 
			ON tdt.co_seq_dim_tempo = caddomiciliarfamilia.co_dim_tempo
		LEFT JOIN tb_dim_equipe eq
			ON eq.co_seq_dim_equipe = caddomiciliarfamilia.co_dim_equipe
		LEFT JOIN tb_dim_profissional acs
			ON acs.co_seq_dim_profissional = caddomiciliarfamilia.co_dim_profissional
		LEFT JOIN tb_dim_unidade_saude uns
			ON uns.co_seq_dim_unidade_saude = caddomiciliarfamilia.co_dim_unidade_saude     
)
, atendimento_recente AS (
	SELECT 
		dd.chave_paciente,
		tdt.dt_registro AS dt_ultima_consulta,
		unidadeatendimentorecente.nu_cnes AS estabelecimento_cnes_atendimento,
		unidadeatendimentorecente.no_unidade_saude AS estabelecimento_nome_atendimento,
		equipeatendimentorecente.nu_ine AS equipe_ine_atendimento,
		equipeatendimentorecente.no_equipe AS equipe_nome_atendimento,
		prof.no_profissional AS profissional_nome_atendimento,
		ROW_NUMBER() OVER (PARTITION BY dd.chave_paciente ORDER BY tdt.dt_registro DESC) = 1  AS ultimo_atendimento
	FROM tb_fat_atendimento_individual atendimento  
	JOIN tb_dim_tempo tdt 
		ON atendimento.co_dim_tempo = tdt.co_seq_dim_tempo
	JOIN tb_fat_cidadao_pec tfcp
		ON tfcp.co_seq_fat_cidadao_pec = atendimento.co_fat_cidadao_pec 
	JOIN denominador_diabeticos dd 
		ON dd.chave_paciente = tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento
	LEFT JOIN tb_dim_equipe equipeatendimentorecente 
		ON equipeatendimentorecente.co_seq_dim_equipe = atendimento.co_dim_equipe_1
	LEFT JOIN tb_dim_unidade_saude unidadeatendimentorecente 
		ON unidadeatendimentorecente.co_seq_dim_unidade_saude = atendimento.co_dim_unidade_saude_1
	LEFT JOIN tb_dim_profissional prof
		ON prof.co_seq_dim_profissional = atendimento.co_dim_profissional_1
)
	SELECT 
		CASE
			WHEN date_part('month'::text, current_date) >= 1::double precision AND date_part('month'::text, current_date) <= 4::double precision THEN concat(date_part('year'::text, current_date), '.Q1')
			WHEN date_part('month'::text, current_date) >= 5::double precision AND date_part('month'::text, current_date) <= 8::double precision THEN concat(date_part('year'::text, current_date), '.Q2')
			WHEN date_part('month'::text, current_date) >= 9::double precision AND date_part('month'::text, current_date) <= 12::double precision THEN concat(date_part('year'::text, current_date), '.Q3')
			ELSE NULL::text
		END AS quadrimestre_atual,
		CASE
			WHEN hg.dt_solicitacao_hemoglobina_glicada_mais_recente <=
			CASE
				WHEN date_part('month'::text, current_date) >= 1::double precision AND date_part('month'::text, current_date) <= 4::double precision THEN concat(date_part('year'::text, current_date), '-04-30')
				WHEN date_part('month'::text, current_date) >= 5::double precision AND date_part('month'::text, current_date) <= 8::double precision THEN concat(date_part('year'::text, current_date), '-08-31')
				WHEN date_part('month'::text, current_date) >= 9::double precision AND date_part('month'::text, current_date) <= 12::double precision THEN concat(date_part('year'::text, current_date), '-12-31')
				ELSE NULL::text
			END::date AND hg.dt_solicitacao_hemoglobina_glicada_mais_recente >= (
			CASE
				WHEN date_part('month'::text, current_date) >= 1::double precision AND date_part('month'::text, current_date) <= 4::double precision THEN concat(date_part('year'::text, current_date), '-04-30')
				WHEN date_part('month'::text, current_date) >= 5::double precision AND date_part('month'::text, current_date) <= 8::double precision THEN concat(date_part('year'::text, current_date), '-08-31')
				WHEN date_part('month'::text, current_date) >= 9::double precision AND date_part('month'::text, current_date) <= 12::double precision THEN concat(date_part('year'::text, current_date), '-12-31')
				ELSE NULL::text
			END::date - '6 months'::interval) THEN true
			ELSE false
		END AS realizou_solicitacao_hemoglobina_ultimos_6_meses,
		hg.dt_solicitacao_hemoglobina_glicada_mais_recente,
		CASE
			WHEN cd.dt_consulta_mais_recente <=
			CASE
				WHEN date_part('month'::text, current_date) >= 1::double precision AND date_part('month'::text, current_date) <= 4::double precision THEN concat(date_part('year'::text, current_date), '-04-30')
				WHEN date_part('month'::text, current_date) >= 5::double precision AND date_part('month'::text, current_date) <= 8::double precision THEN concat(date_part('year'::text, current_date), '-08-31')
				WHEN date_part('month'::text, current_date) >= 9::double precision AND date_part('month'::text, current_date) <= 12::double precision THEN concat(date_part('year'::text, current_date), '-12-31')
				ELSE NULL::text
			END::date AND cd.dt_consulta_mais_recente >= (
			CASE
				WHEN date_part('month'::text, current_date) >= 1::double precision AND date_part('month'::text, current_date) <= 4::double precision THEN concat(date_part('year'::text, current_date), '-04-30')
				WHEN date_part('month'::text, current_date) >= 5::double precision AND date_part('month'::text, current_date) <= 8::double precision THEN concat(date_part('year'::text, current_date), '-08-31')
				WHEN date_part('month'::text, current_date) >= 9::double precision AND date_part('month'::text, current_date) <= 12::double precision THEN concat(date_part('year'::text, current_date), '-12-31')
				ELSE NULL::text
			END::date - '6 months'::interval) THEN true
			ELSE false
		END AS realizou_consulta_ultimos_6_meses,
	cd.dt_consulta_mais_recente,
	dd.co_seq_fat_cidadao_pec,
	dd.cidadao_cpf,
	dd.cidadao_cns,
	dd.cidadao_nome,
	dd.cidadao_nome_social,
	dd.cidadao_sexo,
	dd.dt_nascimento,
	ar.estabelecimento_cnes_atendimento,
	cir.estabelecimento_cnes_cadastro,
	ar.estabelecimento_nome_atendimento,
	cir.estabelecimento_nome_cadastro,
	ar.equipe_ine_atendimento,
	cir.equipe_ine_cadastro,
	hg.equipe_ine_procedimento,
	ar.equipe_nome_atendimento,
	cir.equipe_nome_cadastro,
	hg.equipe_nome_procedimento,
	cir.acs_nome_cadastro,
	vdr.acs_nome_visita,
	ar.profissional_nome_atendimento,
	hg.profissional_nome_procedimento,
	dd.possui_diabetes_autoreferida,
	dd.possui_diabetes_diagnosticada,
	cir.data_ultimo_cadastro,
	ar.dt_ultima_consulta,
	dd.se_faleceu,
	cir.se_mudou, 
	dd.cidadao_telefone,
	now() as criacao_data
FROM denominador_diabeticos dd
LEFT JOIN hemoglobina_glicada hg 
	ON hg.chave_paciente = dd.chave_paciente
LEFT JOIN consulta_diabetes cd
	ON cd.chave_paciente = dd.chave_paciente
LEFT JOIN cadastro_individual_recente cir 
	ON cir.chave_paciente = dd.chave_paciente
	AND cir.ultimo_cadastro_individual IS TRUE 
LEFT JOIN visita_domiciliar_recente vdr 
	ON vdr.chave_paciente = dd.chave_paciente
	AND vdr.ultima_visita_domiciliar IS TRUE 
LEFT JOIN cadastro_domiciliar_recente cdr 
	ON cdr.chave_paciente = dd.chave_paciente
	AND cdr.ultimo_cadastro_domiciliar_familia IS TRUE
LEFT JOIN atendimento_recente ar 
	ON ar.chave_paciente = dd.chave_paciente
	AND ar.ultimo_atendimento IS TRUE