-- DENOMINADOR: crianças que completam 12 meses no quadrimestre atual
WITH dados_cidadao_pec AS (
    SELECT 
        tfcp.co_seq_fat_cidadao_pec AS id_cidadao_pec,
        replace(tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento, ' ', '') AS chave_cidadao,
        replace(tfcp.no_cidadao, '  ', ' ') AS paciente_nome,
        tempocidadaopec.dt_registro AS dt_nascimento,
        (array_agg(tfcp.nu_cpf_cidadao) FILTER (WHERE tfcp.nu_cpf_cidadao IS NOT NULL) OVER (PARTITION BY replace(tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento, ' ', '') ORDER BY tfcp.co_seq_fat_cidadao_pec DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS cidadao_cpf,
	    (array_agg(tfcp.nu_cns) FILTER (WHERE tfcp.nu_cns IS NOT NULL) OVER (PARTITION BY replace(tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento, ' ', '') ORDER BY tfcp.co_seq_fat_cidadao_pec DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS cidadao_cns,
       	(array_agg(tfcp.st_faleceu) FILTER (WHERE tfcp.st_faleceu IS NOT NULL) OVER (PARTITION BY replace(tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento, ' ', '') ORDER BY tfcp.co_seq_fat_cidadao_pec DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS se_faleceu,
	    (array_agg(tds.ds_sexo) FILTER (WHERE tds.ds_sexo IS NOT NULL) OVER (PARTITION BY replace(tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento, ' ', '') ORDER BY tfcp.co_seq_fat_cidadao_pec DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS paciente_sexo,
	    tfci.co_fat_cidadao_pec_responsvl as id_cidadao_pec_responsavel,
	    EXTRACT(YEAR FROM age(CURRENT_DATE::timestamp WITH time zone, tempocidadaopec.dt_registro::timestamp WITH time zone)) * 12 + EXTRACT(MONTH FROM age(CURRENT_DATE::timestamp WITH time zone, tempocidadaopec.dt_registro::timestamp WITH time zone)) AS paciente_idade_atual,
        case --puxar da tabela periodos
            WHEN date_part('month', CURRENT_DATE) >= 1 AND date_part('month', CURRENT_DATE) <= 4 THEN concat(date_part('year', CURRENT_DATE ::date), '-01-01')
            WHEN date_part('month', CURRENT_DATE) >= 5 AND date_part('month', CURRENT_DATE) <= 8 THEN concat(date_part('year', CURRENT_DATE ::date), '-05-01')
            WHEN date_part('month', CURRENT_DATE) >= 9 AND date_part('month', CURRENT_DATE) <= 12 THEN concat(date_part('year', CURRENT_DATE ::date), '-09-01')
            ELSE NULL
        END AS data_inicio_quadrimestre,
    	CASE
            WHEN date_part('month', CURRENT_DATE) >= 1 AND date_part('month', CURRENT_DATE) <= 4 THEN concat(date_part('year', CURRENT_DATE ::date), '-04-30')
            WHEN date_part('month', CURRENT_DATE) >= 5 AND date_part('month', CURRENT_DATE) <= 8 THEN concat(date_part('year', CURRENT_DATE ::date), '-08-31')
            WHEN date_part('month', CURRENT_DATE) >= 9 AND date_part('month', CURRENT_DATE) <= 12 THEN concat(date_part('year', CURRENT_DATE ::date), '-12-31')
            ELSE NULL
        END AS data_fim_quadrimestre
    FROM esus_160050_oiapoque_ap_20230405.tb_fat_cidadao_pec tfcp
    JOIN esus_160050_oiapoque_ap_20230405.tb_dim_tempo tempocidadaopec ON tfcp.co_dim_tempo_nascimento = tempocidadaopec.co_seq_dim_tempo
    JOIN esus_160050_oiapoque_ap_20230405.tb_dim_sexo tds ON tds.co_seq_dim_sexo = tfcp.co_dim_sexo
    left join esus_160050_oiapoque_ap_20230405.tb_fat_cad_individual tfci on tfci.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec 
),
selecao_denominador as (
WITH base as (
     SELECT 
		dcp.id_cidadao_pec,
	     dcp.chave_cidadao,
	     dcp.paciente_nome,
	     dcp.dt_nascimento,
	     dcp.cidadao_cpf,
	     dcp.cidadao_cns,
	     dcp.paciente_idade_atual,
	     case 
	     	when dcp.paciente_idade_atual < 2 then 'vacinacao_nao_iniciada'
	     	when dcp.paciente_idade_atual between 2 and 12 then 'vacinacao_em_andamento'
	     	when dcp.paciente_idade_atual > 12  then 'periodo_vacinacao_encerrado'
	     end as status_idade,	     
	     dcp.id_cidadao_pec_responsavel,
	     responsavel.no_cidadao as cidadao_nome_responsavel,
	     responsavel.nu_cns as cidadao_cns_responsavel,
	     responsavel.nu_cpf_cidadao as cidadao_cpf_responsavel,
	     EXTRACT(YEAR FROM age(dcp.data_inicio_quadrimestre::timestamp WITH time zone, dcp.dt_nascimento::timestamp WITH time zone)) * 12 + EXTRACT(MONTH FROM age(dcp.data_inicio_quadrimestre::timestamp WITH time zone, dcp.dt_nascimento::timestamp WITH time zone)) AS idade_inicio_do_quadri,
	     EXTRACT(YEAR FROM age(dcp.data_fim_quadrimestre::timestamp WITH time zone, dcp.dt_nascimento::timestamp WITH time zone)) * 12 + EXTRACT(MONTH FROM age(dcp.data_fim_quadrimestre::timestamp WITH time zone, dcp.dt_nascimento::timestamp WITH time zone)) AS idade_fim_do_quadri
	 FROM dados_cidadao_pec dcp
	left join esus_160050_oiapoque_ap_20230405.tb_fat_cidadao_pec responsavel on dcp.id_cidadao_pec_responsavel  = responsavel.co_seq_fat_cidadao_pec 
	 GROUP BY 
	 dcp.id_cidadao_pec,
	 	 dcp.chave_cidadao,
	     dcp.paciente_nome,
	     dcp.dt_nascimento,
	     dcp.cidadao_cpf,
	     dcp.cidadao_cns,
	     dcp.paciente_idade_atual,
	     dcp.id_cidadao_pec_responsavel,
	     dcp.data_fim_quadrimestre,
	     dcp.data_inicio_quadrimestre,
	     responsavel.no_cidadao,
	     responsavel.no_cidadao,
	     responsavel.nu_cns,
	     responsavel.nu_cpf_cidadao
	) SELECT * FROM base 
	  WHERE idade_fim_do_quadri <= 16 -- (dt_nascimento + INTERVAL '1 year' BETWEEN data_inicio_quadrimentes AND data_fim_quadrimestre)
),
historico_vacinacao_criancas as (
	SELECT 
		sd.id_cidadao_pec,
		tfv.co_seq_fat_vacinacao,
		sd.chave_cidadao,
		sd.dt_nascimento,
		sd.paciente_idade_atual,
		imunobiologico.nu_identificador as codigo_vacina,
		imunobiologico.no_imunobiologico as nome_vacina,
		dose.no_dose_imunobiologico as dose_vacina,
		tempo.dt_registro as data_registro_vacina,
		unidadesaude.nu_cnes,
		unidadesaude.no_unidade_saude,
		equipe.nu_ine,
		equipe.no_equipe,
		profissional.no_profissional,
		profissional.nu_cns,
		cbo.nu_cbo,
		cbo.no_cbo
	FROM  esus_160050_oiapoque_ap_20230405.tb_fat_vacinacao tfv
	JOIN selecao_denominador sd on sd.id_cidadao_pec = tfv.co_fat_cidadao_pec
	LEFT JOIN esus_160050_oiapoque_ap_20230405.tb_fat_vacinacao_vacina tfvv on tfv.co_seq_fat_vacinacao = tfvv.co_fat_vacinacao 
	LEFT JOIN esus_160050_oiapoque_ap_20230405.tb_dim_imunobiologico imunobiologico on tfvv.co_dim_imunobiologico = imunobiologico.co_seq_dim_imunobiologico 
	LEFT JOIN esus_160050_oiapoque_ap_20230405.tb_dim_tempo tempo on tfvv.co_dim_tempo_vacina_aplicada  = tempo.co_seq_dim_tempo
	LEFT JOIN esus_160050_oiapoque_ap_20230405.tb_dim_dose_imunobiologico dose on dose.co_seq_dim_dose_imunobiologico = tfvv.co_dim_dose_imunobiologico 
	LEFT JOIN esus_160050_oiapoque_ap_20230405.tb_dim_equipe equipe on equipe.co_seq_dim_equipe = tfv.co_dim_equipe 
	LEFT JOIN esus_160050_oiapoque_ap_20230405.tb_dim_profissional profissional on profissional.co_seq_dim_profissional = tfvv.co_dim_profissional 
	LEFT JOIN esus_160050_oiapoque_ap_20230405.tb_dim_cbo cbo on cbo.co_seq_dim_cbo = tfvv.co_dim_cbo
	LEFT JOIN esus_160050_oiapoque_ap_20230405.tb_dim_unidade_saude unidadesaude on unidadesaude.co_seq_dim_unidade_saude = tfvv.co_dim_unidade_saude 
	WHERE imunobiologico.nu_identificador in ('22','42','17','29','39','43','46','9')
	AND sd.idade_inicio_do_quadri = 12 or sd.idade_fim_do_quadri = 12
	GROUP BY sd.id_cidadao_pec,
		sd.chave_cidadao,
		sd.paciente_idade_atual,
		sd.dt_nascimento,
		tfv.co_seq_fat_vacinacao,
		imunobiologico.nu_identificador,
		imunobiologico.no_imunobiologico,
		dose.no_dose_imunobiologico,
		tempo.dt_registro,
		unidadesaude.nu_cnes,
		unidadesaude.no_unidade_saude,
		equipe.nu_ine,
		equipe.no_equipe,
		profissional.no_profissional,
		profissional.nu_cns,
		cbo.nu_cbo,
		cbo.no_cbo
), cadastro_individual_recente AS (
	WITH base AS (
		SELECT 
			sd.chave_cidadao,
			tdt.dt_registro AS data_cadastro_individual,
			nullif(tfci.nu_micro_area::text, '-'::text) AS micro_area_cad_individual,
			uns.nu_cnes AS cnes_estabelecimento_cad_individual,
			uns.no_unidade_saude AS estabelecimento_cad_individual,
			eq.nu_ine AS ine_equipe_cad_individual,
			eq.no_equipe AS equipe_cad_individual,
			acs.no_profissional AS acs_cad_individual,
			row_number() OVER (PARTITION BY sd.chave_cidadao ORDER BY tdt.dt_registro DESC) = 1 AS ultimo_cadastro_individual
		FROM esus_160050_oiapoque_ap_20230405.tb_fat_cad_individual tfci
		JOIN esus_160050_oiapoque_ap_20230405.tb_fat_cidadao_pec tfcpec
			ON tfcpec.co_seq_fat_cidadao_pec = tfci.co_fat_cidadao_pec
		JOIN selecao_denominador sd 
			ON sd.chave_cidadao = replace(tfcpec.no_cidadao::text||tfcpec.co_dim_tempo_nascimento,' ','')
		LEFT JOIN esus_160050_oiapoque_ap_20230405.tb_dim_tempo tdt 
			ON tdt.co_seq_dim_tempo = tfci.co_dim_tempo
		LEFT JOIN esus_160050_oiapoque_ap_20230405.tb_dim_equipe eq
			ON eq.co_seq_dim_equipe = tfci.co_dim_equipe
		LEFT JOIN esus_160050_oiapoque_ap_20230405.tb_dim_profissional acs
			ON acs.co_seq_dim_profissional = tfci.co_dim_profissional
		LEFT JOIN esus_160050_oiapoque_ap_20230405.tb_dim_unidade_saude uns
			ON uns.co_seq_dim_unidade_saude = tfci.co_dim_unidade_saude  
		)
	SELECT * FROM base WHERE ultimo_cadastro_individual IS true
), 
atendimento_mais_recente AS (
-- Filtro de atendimento individual mais recente
with base as (	
SELECT 
			tfai.co_seq_fat_atd_ind::TEXT AS id_registro,
			tdt.dt_registro AS data_registro,
			sd.chave_cidadao,
			tfcp.nu_telefone_celular AS paciente_telefone,
			tdprof.nu_cns AS profissional_cns_atendimento_recente,
			tdprof.no_profissional AS profissional_atendimento_recente,
			uns.nu_cnes AS estabelecimento_cnes_atendimento_recente,
			uns.no_unidade_saude AS estabelecimento_atendimento_recente,
			eq.nu_ine AS ine_equipe_atendimento_recente,
			eq.no_equipe AS equipe_atendimento_recente,
			row_number() OVER (PARTITION BY sd.chave_cidadao ORDER BY tfai.co_seq_fat_atd_ind DESC) = 1 AS ultimo_atendimento_individual
	    FROM esus_160050_oiapoque_ap_20230405.tb_fat_atendimento_individual tfai
	    JOIN esus_160050_oiapoque_ap_20230405.tb_dim_tempo tdt 
	    	ON tfai.co_dim_tempo = tdt.co_seq_dim_tempo
	    LEFT JOIN esus_160050_oiapoque_ap_20230405.tb_dim_profissional tdprof
			ON tdprof.co_seq_dim_profissional = tfai.co_dim_profissional_1
		LEFT JOIN esus_160050_oiapoque_ap_20230405.tb_dim_equipe eq
			ON eq.co_seq_dim_equipe = tfai.co_dim_equipe_1
		LEFT JOIN esus_160050_oiapoque_ap_20230405.tb_dim_unidade_saude uns 
			ON uns.co_seq_dim_unidade_saude = tfai.co_dim_unidade_saude_1
	    JOIN esus_160050_oiapoque_ap_20230405.tb_fat_cidadao_pec tfcp 
	    	ON tfcp.co_seq_fat_cidadao_pec = tfai.co_fat_cidadao_pec
	    JOIN esus_160050_oiapoque_ap_20230405.tb_dim_tempo tempocidadaopec 
	    	ON tempocidadaopec.co_seq_dim_tempo = tfcp.co_dim_tempo_nascimento
	  	JOIN selecao_denominador sd 
			ON sd.chave_cidadao = replace(tfcp.no_cidadao::text||tfcp.co_dim_tempo_nascimento,' ','')
		) 	
		SELECT * FROM base WHERE ultimo_atendimento_individual IS true
),
visita_domiciliar_recente AS (
	WITH base AS (
		SELECT 
			sd.chave_cidadao,
		    tfcpec.co_seq_fat_cidadao_pec,
			tdt.dt_registro AS data_visita_acs,
			acs.no_profissional AS acs_visita_domiciliar,
			row_number() OVER (PARTITION BY sd.chave_cidadao ORDER BY tdt.dt_registro DESC) = 1 AS ultima_visita_domiciliar
		FROM esus_160050_oiapoque_ap_20230405.tb_fat_visita_domiciliar visitadomiciliar
		JOIN esus_160050_oiapoque_ap_20230405.tb_fat_cidadao_pec tfcpec
			ON tfcpec.co_seq_fat_cidadao_pec = visitadomiciliar.co_fat_cidadao_pec 
		JOIN selecao_denominador sd
			ON sd.chave_cidadao = replace(tfcpec.no_cidadao::text||tfcpec.co_dim_tempo_nascimento,' ','')
		LEFT JOIN esus_160050_oiapoque_ap_20230405.tb_dim_profissional acs
			ON acs.co_seq_dim_profissional = visitadomiciliar.co_dim_profissional
		LEFT JOIN esus_160050_oiapoque_ap_20230405.tb_dim_tempo tdt 
			ON tdt.co_seq_dim_tempo = visitadomiciliar.co_dim_tempo
		)
	SELECT * FROM base WHERE ultima_visita_domiciliar IS TRUE 
), 
cadastro_domiciliar_recente AS (
	WITH base AS (
		SELECT
			sd.chave_cidadao,
			tdt.dt_registro AS data_cadastro_dom_familia,
			caddomiciliarfamilia.nu_micro_area AS micro_area_domicilio,
			uns.nu_cnes AS cnes_estabelecimento_cad_dom_familia,
			uns.no_unidade_saude AS estabelecimento_cad_dom_familia,
			eq.nu_ine AS ine_equipe_cad_dom_familia,
			eq.no_equipe AS equipe_cad_dom_familia,
			acs.no_profissional AS acs_cad_dom_familia,
			NULLIF(concat(cadomiciliar.no_logradouro, ', ', cadomiciliar.nu_num_logradouro), ', '::text) AS paciente_endereco,
			row_number() OVER (PARTITION BY sd.chave_cidadao ORDER BY tdt.dt_registro DESC) = 1 AS ultimo_cadastro_domiciliar_familia
		FROM esus_160050_oiapoque_ap_20230405.tb_fat_cad_dom_familia caddomiciliarfamilia
		JOIN esus_160050_oiapoque_ap_20230405.tb_fat_cad_domiciliar cadomiciliar
			ON cadomiciliar.co_seq_fat_cad_domiciliar = caddomiciliarfamilia.co_fat_cad_domiciliar
		JOIN esus_160050_oiapoque_ap_20230405.tb_fat_cidadao_pec tfcpec
			ON tfcpec.co_seq_fat_cidadao_pec = caddomiciliarfamilia.co_fat_cidadao_pec
		JOIN selecao_denominador sd
			ON sd.chave_cidadao = replace(tfcpec.no_cidadao::text || tfcpec.co_dim_tempo_nascimento, ' ', '')
		LEFT JOIN esus_160050_oiapoque_ap_20230405.tb_dim_tempo tdt
			ON tdt.co_seq_dim_tempo = caddomiciliarfamilia.co_dim_tempo
		LEFT JOIN esus_160050_oiapoque_ap_20230405.tb_dim_equipe eq
			ON eq.co_seq_dim_equipe = caddomiciliarfamilia.co_dim_equipe
		LEFT JOIN esus_160050_oiapoque_ap_20230405.tb_dim_profissional acs
			ON acs.co_seq_dim_profissional = caddomiciliarfamilia.co_dim_profissional
		LEFT JOIN esus_160050_oiapoque_ap_20230405.tb_dim_unidade_saude uns
			ON uns.co_seq_dim_unidade_saude = caddomiciliarfamilia.co_dim_unidade_saude
	)
	SELECT * FROM base WHERE ultimo_cadastro_domiciliar_familia IS true
),
informacoes_atendimento_recente AS (
	SELECT
		b.chave_cidadao,
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
		cdr.paciente_endereco
	FROM selecao_denominador b
	LEFT JOIN cadastro_individual_recente cir
		ON cir.chave_cidadao = b.chave_cidadao
	LEFT JOIN visita_domiciliar_recente vdr
		ON vdr.chave_cidadao = b.chave_cidadao
	LEFT JOIN cadastro_domiciliar_recente cdr
		ON cdr.chave_cidadao = b.chave_cidadao
	left join atendimento_mais_recente ar
		on ar.chave_cidadao = b.chave_cidadao
	GROUP BY
		b.chave_cidadao,
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
		cdr.paciente_endereco
), cenarios as (
	SELECT
	hvc.chave_cidadao,
	hvc.paciente_idade_atual,
    CASE
	    -- CENÁRIO A: Crianças (idade entre 6 e 12 meses) do denominador que foram imunizadas com a 3ª dose de VIP (22) + 3ª dose de Pentavalente Celular (42)
        WHEN COUNT(CASE WHEN hvc.codigo_vacina = '42' AND hvc.dose_vacina = '3ª DOSE' THEN 1 ELSE NULL END) > 0
             AND COUNT(CASE WHEN hvc.codigo_vacina = '22' AND hvc.dose_vacina = '3ª DOSE' THEN 1 ELSE NULL END) > 0
        THEN 'cenario_a'
        -- CENÁRIO B: Crianças (idade entre 6 e 12 meses) do denominador que foram imunizadas com a 3ª dose de VIP (22) + 3ª dose de Hexavalente (43)
        WHEN COUNT(CASE WHEN hvc.codigo_vacina = '22' AND hvc.dose_vacina = '3ª DOSE' THEN 1 ELSE NULL END) > 0
             AND COUNT(CASE WHEN hvc.codigo_vacina = '43' AND hvc.dose_vacina = '3ª DOSE' THEN 1 ELSE NULL END) > 0
        THEN 'cenario_b'
        -- CENÁRIO C: Crianças (idade entre 6 e 12 meses) do denominador que foram imunizadas com a 3ªdose de VIP (22) + 1 dose Penta Acelular (29) + 1 dose Hepatite B (09)
        WHEN COUNT(CASE WHEN hvc.codigo_vacina = '22' AND hvc.dose_vacina = '3ª DOSE' THEN 1 ELSE NULL END) > 0
             AND COUNT(CASE WHEN hvc.codigo_vacina = '29' THEN 1 ELSE NULL END) > 0
             AND count(CASE WHEN hvc.codigo_vacina = '9' THEN 1 ELSE null end) > 0
        THEN 'cenario_c'
        -- CENÁRIO D: 3ª dose de VIP (22) + 2ª dose de Pentavalente celular (42) + 1 dose DTP (46) + 1 dose Hepatite B (09) + 1 dose haemophilus b (17)
        WHEN COUNT(CASE WHEN hvc.codigo_vacina = '22' AND hvc.dose_vacina = '3ª DOSE' THEN 1 ELSE NULL END) > 0
             AND COUNT(CASE WHEN hvc.codigo_vacina = '42' AND hvc.dose_vacina = '2ª DOSE' THEN 1 ELSE NULL END) > 0
             AND SUM(CASE WHEN hvc.codigo_vacina = '46' AND hvc.dose_vacina in ('%DOSE%') THEN 1 ELSE null end) = 1
             AND SUM(CASE WHEN hvc.codigo_vacina = '9' AND hvc.dose_vacina in ('%DOSE%') THEN 1 ELSE null end) = 1
			 AND SUM(CASE WHEN hvc.codigo_vacina = '17' AND hvc.dose_vacina in ('%DOSE%') THEN 1 ELSE null end) = 1
        THEN 'cenario_d'
        -- CENÁRIO E: 3ª dose de VIP (22) + 2ª dose de Pentavalente Celular (42) + 1 dose Tetravalente (39) + 1 dose Hepatite B (09)
        WHEN COUNT(CASE WHEN hvc.codigo_vacina = '22' AND hvc.dose_vacina = '3ª DOSE' THEN 1 ELSE NULL END) > 0
             AND COUNT(CASE WHEN hvc.codigo_vacina = '42' AND hvc.dose_vacina = '2ª DOSE' THEN 1 ELSE NULL END) > 0
             AND SUM(CASE WHEN hvc.codigo_vacina = '39' AND hvc.dose_vacina in ('%DOSE%') THEN 1 ELSE null end) = 1
             AND SUM(CASE WHEN hvc.codigo_vacina = '9' AND hvc.dose_vacina in ('%DOSE%') THEN 1 ELSE null end) = 1
        THEN 'cenario_e'
        -- CENÁRIO F: 3ª dose de VIP (22) + 2ª dose de Pentavalente (42) + 1 dose Hexavalente (43)
		WHEN COUNT(CASE WHEN hvc.codigo_vacina = '22' AND hvc.dose_vacina = '3ª DOSE' THEN 1 ELSE NULL END) > 0
             AND COUNT(CASE WHEN hvc.codigo_vacina = '42' AND hvc.dose_vacina = '2ª DOSE' THEN 1 ELSE NULL END) > 0
             AND SUM(CASE WHEN hvc.codigo_vacina = '43' AND hvc.dose_vacina in ('%DOSE%') THEN 1 ELSE null end) = 1
        THEN 'cenario_f'
        -- CENÁRIO G: 3ª dose de VIP (22) + 1ª dose de Pentavalente (42) + 2 (duas) doses DTP (46) + 2(duas) doses Hepatite B (09) + 2 (duas) doses haemophilus b (17)
        WHEN COUNT(CASE WHEN hvc.codigo_vacina = '22' AND hvc.dose_vacina = '3ª DOSE' THEN 1 ELSE NULL END) > 0
             AND COUNT(CASE WHEN hvc.codigo_vacina = '42' AND hvc.dose_vacina = '1ª DOSE' THEN 1 ELSE NULL END) > 0
             AND SUM(CASE WHEN hvc.codigo_vacina = '46' AND hvc.dose_vacina in ('%DOSE%') THEN 1 ELSE null end) >= 2
             AND SUM(CASE WHEN hvc.codigo_vacina = '9' AND hvc.dose_vacina in ('%DOSE%') THEN 1 ELSE null end) >=  2
			 AND SUM(CASE WHEN hvc.codigo_vacina = '17' AND hvc.dose_vacina in ('%DOSE%') THEN 1 ELSE null end) >= 1
        THEN 'cenario_g'
        -- CENÁRIO H: 3ª dose de VIP (22) + 1ª dose de Pentavalente (42) + 2 doses Tetravalente (39) + 2 doses Hepatite B (09)
        WHEN COUNT(CASE WHEN hvc.codigo_vacina = '22' AND hvc.dose_vacina = '3ª DOSE' THEN 1 ELSE NULL END) > 0
             AND COUNT(CASE WHEN hvc.codigo_vacina = '42' AND hvc.dose_vacina = '1ª DOSE' THEN 1 ELSE NULL END) > 0
             AND SUM(CASE WHEN hvc.codigo_vacina = '39' AND hvc.dose_vacina in ('%DOSE%') THEN 1 ELSE null end) >= 2
             AND SUM(CASE WHEN hvc.codigo_vacina = '9' AND hvc.dose_vacina in ('%DOSE%') THEN 1 ELSE null end) >=  2
        THEN 'cenario_h'
         -- CENÁRIO I: 3ª dose de VIP (22) + 1ª dose de Pentavalente (42) + 1 dose de Tetravalente (39) + 1 dose de DTP (46) + 2 doses Hepatite B (09) + 1 dose haemophilus b (17)
        WHEN COUNT(CASE WHEN hvc.codigo_vacina = '22' AND hvc.dose_vacina = '3ª DOSE' THEN 1 ELSE NULL END) > 0
             AND COUNT(CASE WHEN hvc.codigo_vacina = '42' AND hvc.dose_vacina = '1ª DOSE' THEN 1 ELSE NULL END) > 0
             AND SUM(CASE WHEN hvc.codigo_vacina = '39' AND hvc.dose_vacina in ('%DOSE%') THEN 1 ELSE null end) >= 1
             AND SUM(CASE WHEN hvc.codigo_vacina = '46' AND hvc.dose_vacina in ('%DOSE%') THEN 1 ELSE null end) >=  1
			AND SUM(CASE WHEN hvc.codigo_vacina = '9' AND hvc.dose_vacina in ('%DOSE%') THEN 1 ELSE null end) >=  2
			AND SUM(CASE WHEN hvc.codigo_vacina = '17' AND hvc.dose_vacina in ('%DOSE%') THEN 1 ELSE null end) >= 1
        THEN 'cenario_i'
         -- CENÁRIO J: 3ª dose de VIP (22) + 1ª dose de Pentavalente (42) + 2 doses da Hexavalente (43)
        WHEN COUNT(CASE WHEN hvc.codigo_vacina = '22' AND hvc.dose_vacina = '3ª DOSE' THEN 1 ELSE NULL END) > 0
             AND COUNT(CASE WHEN hvc.codigo_vacina = '42' AND hvc.dose_vacina = '1ª DOSE' THEN 1 ELSE NULL END) > 0
			AND SUM(CASE WHEN hvc.codigo_vacina = '43' AND hvc.dose_vacina in ('%DOSE%') THEN 1 ELSE null end) >=  2
        THEN 'cenario_j'
        -- CENÁRIO K: 3ª dose de VIP (22) + 3ª dose da Tetravalente (39) + 3ª dose Hepatite B (09)
        WHEN COUNT(CASE WHEN hvc.codigo_vacina = '22' AND hvc.dose_vacina = '3ª DOSE' THEN 1 ELSE NULL END) > 0
             AND COUNT(CASE WHEN hvc.codigo_vacina = '39' AND hvc.dose_vacina = '3ª DOSE' THEN 1 ELSE NULL END) > 0
			AND SUM(CASE WHEN hvc.codigo_vacina = '9' AND hvc.dose_vacina in ('%DOSE%') THEN 1 ELSE null end) >=  3
        THEN 'cenario_k'
        -- CENÁRIO L: 3ª dose de VIP (22) + 3ª dose da DTP (46) + 3ª dose Hepatite B (09) + 3ª dose da haemophilus b (17)
        WHEN COUNT(CASE WHEN hvc.codigo_vacina = '22' AND hvc.dose_vacina = '3ª DOSE' THEN 1 ELSE NULL END) > 0
            AND COUNT(CASE WHEN hvc.codigo_vacina = '46' AND hvc.dose_vacina = '3ª DOSE' THEN 1 ELSE NULL END) > 0
			AND SUM(CASE WHEN hvc.codigo_vacina = '9' AND hvc.dose_vacina in ('%DOSE%') THEN 1 ELSE null end) >=  3
			AND SUM(CASE WHEN hvc.codigo_vacina = '17' AND hvc.dose_vacina in ('%DOSE%') THEN 1 ELSE null end) >= 3
        THEN 'cenario_k'
        ELSE 'nenhum_cenario'
    END AS cenario
FROM historico_vacinacao_criancas hvc
GROUP BY
    hvc.chave_cidadao,
    hvc.paciente_idade_atual	
), 
quantidade_vacinas_polio as ( --para indicar se tem mais que 3 doses
with base as ( 
select
	hvc.chave_cidadao,
    count(CASE WHEN codigo_vacina = '22' THEN 1 ELSE 0 END) OVER (PARTITION BY chave_cidadao) AS soma_doses_polio
    FROM historico_vacinacao_criancas hvc
	group by
	hvc.chave_cidadao,
	hvc.codigo_vacina
	) select * from base
),
aplicacao_polio as (
with base as (
	with base1 as (
	select
		hvc.chave_cidadao,
		hvc.dt_nascimento,
		hvc.data_registro_vacina,
		hvc.co_seq_fat_vacinacao,
		hvc.dose_vacina,
	    count(CASE WHEN codigo_vacina = '22' THEN 1 ELSE 0 END) OVER (PARTITION BY chave_cidadao) AS soma_doses_polio,
		case 
			when row_number() over (partition by hvc.chave_cidadao order by hvc.co_seq_fat_vacinacao asc) = 1 is true then 'primeira_dose_aplicada'
			when row_number() over (partition by hvc.chave_cidadao order by hvc.co_seq_fat_vacinacao asc) = 2 is true then 'segunda_dose_aplicada'
			when row_number() over (partition by hvc.chave_cidadao order by hvc.co_seq_fat_vacinacao asc) = 3 is true then 'terceira_dose_aplicada'
			else 'dose_a_mais'
		end as ordem_aplicacao_polio
		FROM historico_vacinacao_criancas hvc
		where hvc.codigo_vacina = '22' 
	)
		select 
		chave_cidadao,
		dt_nascimento,
		data_registro_vacina,
		co_seq_fat_vacinacao,
		dose_vacina,
		ordem_aplicacao_polio,
		case 
			when dose_vacina = '1ª DOSE' and ordem_aplicacao_polio = 'primeira_dose_aplicada' then 'ok'
			when dose_vacina = '2ª DOSE' and ordem_aplicacao_polio = 'segunda_dose_aplicada' then 'ok'
			when dose_vacina = '3ª DOSE' and ordem_aplicacao_polio = 'terceira_dose_aplicada' then 'ok'
			else concat ('erro_registro','_',ordem_aplicacao_polio)
		end as sinalizacao_erro
		from base1
	) select * from base
), 
co_seq_aplicacao_polio as (
with base1 as (
	with base as (
	select
	ap.chave_cidadao,
	ap.dt_nascimento,
	case 
		when ordem_aplicacao_polio = 'primeira_dose_aplicada' then co_seq_fat_vacinacao
		else null
	end as co_seq_fat_vacinacao_d1,
	case 
		when ordem_aplicacao_polio = 'segunda_dose_aplicada' then co_seq_fat_vacinacao
		else null
	end as co_seq_fat_vacinacao_d2,
	case 
		when ordem_aplicacao_polio = 'terceira_dose_aplicada' then co_seq_fat_vacinacao
		else null
	end as co_seq_fat_vacinacao_d3
	from aplicacao_polio ap
	) select 
		b.chave_cidadao, 
		b.dt_nascimento,
		sum(b.co_seq_fat_vacinacao_d1) over (partition by b.chave_cidadao) as co_seq_d1,
		sum(b.co_seq_fat_vacinacao_d2) over (partition by b.chave_cidadao) as co_seq_d2,
		sum(b.co_seq_fat_vacinacao_d3) over (partition by b.chave_cidadao) as co_seq_d3
		from base b
		) select 
		*
		from base1
		group by chave_cidadao,dt_nascimento, co_seq_d1, co_seq_d2, co_seq_d3
),
datas_aplicacao_polio as (
with base1 as (
	with base as (
	select
	ap.chave_cidadao,
	ap.dt_nascimento,
	case 
		when ordem_aplicacao_polio = 'primeira_dose_aplicada' then replace(data_registro_vacina::text,'-','')::numeric
	end as data_d1,
	case 
		when ordem_aplicacao_polio = 'segunda_dose_aplicada' then replace(data_registro_vacina::text,'-','')::numeric
	end as data_d2,
	case 
		when ordem_aplicacao_polio = 'terceira_dose_aplicada' then replace(data_registro_vacina::text,'-','')::numeric
	end as data_d3
	from aplicacao_polio ap
	) select 
		b.chave_cidadao, 
		b.dt_nascimento,
		sum(b.data_d1) over (partition by b.chave_cidadao) as d1,
		sum(b.data_d2) over (partition by b.chave_cidadao) as d2,
		sum(b.data_d3) over (partition by b.chave_cidadao) as d3
		from base b
		) select 
		chave_cidadao,
		dt_nascimento,
		"substring"(d1::text, 1, 10)::date  AS d1,
		"substring"(d2::text, 1, 10)::date  AS d2,
		"substring"(d3::text, 1, 10)::date  AS d3
		from base1
		group by chave_cidadao,dt_nascimento, d1, d2, d3
),
erro_doses_polio as (
with base1 as (
	with base as (
	select
	ap.chave_cidadao,
	case 
		when ordem_aplicacao_polio = 'primeira_dose_aplicada' and sinalizacao_erro != 'ok' then 1
		else null
	end as d1_com_erro,
	case 
		when ordem_aplicacao_polio = 'segunda_dose_aplicada' and sinalizacao_erro != 'ok' then 1
		else null
	end as d2_com_erro,
	case 
		when ordem_aplicacao_polio = 'terceira_dose_aplicada' and sinalizacao_erro != 'ok' then 1
		else null
	end as d3_com_erro
	from aplicacao_polio ap
	) select 
		b.chave_cidadao, 
		case when sum(b.d1_com_erro) over (partition by b.chave_cidadao) = 1 then 'sim' end as d1_polio_erro,
		case when sum(b.d2_com_erro) over (partition by b.chave_cidadao) = 1 then 'sim' end as d2_polio_erro,
		case when sum(b.d3_com_erro) over (partition by b.chave_cidadao) = 1 then 'sim' end as d3_polio_erro
		from base b
		) select * from base1
		group by chave_cidadao,d1_polio_erro, d2_polio_erro, d3_polio_erro
), sumarizacao_polio as (
select 
sd.chave_cidadao,
sd.dt_nascimento,
dap.d1 as d1_polio,
dap.d2 as d2_polio,
dap.d3 as d3_polio,
age(dap.d1::timestamp WITH time zone, sd.dt_nascimento::timestamp WITH time zone) as idade_1dose_polio,
age(dap.d2::timestamp WITH time zone, sd.dt_nascimento::timestamp WITH time zone) as idade_2dose_polio,
age(dap.d3::timestamp WITH time zone, sd.dt_nascimento::timestamp WITH time zone) as idade_3dose_polio,
erros.d1_polio_erro,
erros.d2_polio_erro,
erros.d3_polio_erro,
date(sd.dt_nascimento + interval '2 months') as prazo_1dose_polio,
date(sd.dt_nascimento + interval '6 months') as prazo_limite_1dose_polio,
date(dap.d2 + interval '2 months') as prazo_2dose_polio,
date(dap.d3 + interval '2 months') as prazo_3dose_polio
from selecao_denominador sd
left join datas_aplicacao_polio dap on sd.chave_cidadao = dap.chave_cidadao
left join erro_doses_polio erros on erros.chave_cidadao = sd.chave_cidadao 
group by sd.chave_cidadao,
sd.dt_nascimento,
dap.d1,
dap.d2,
dap.d3,
erros.d1_polio_erro,
erros.d2_polio_erro,
erros.d3_polio_erro
) select * from sumarizacao_polio