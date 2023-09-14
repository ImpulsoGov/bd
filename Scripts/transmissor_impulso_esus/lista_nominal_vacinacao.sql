-- DENOMINADOR: crianças que completam 12 meses no quadrimestre atual
WITH dados_cidadao_pec AS (
    SELECT 
        tfcp.co_seq_fat_cidadao_pec AS id_cidadao_pec,
        replace(tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento, ' ', '') AS chave_cidadao,
        replace(tfcp.no_cidadao, '  ', ' ') AS cidadao_nome,
        tempocidadaopec.dt_registro AS dt_nascimento,
        (array_agg(tfcp.nu_cpf_cidadao) FILTER (WHERE tfcp.nu_cpf_cidadao IS NOT NULL) OVER (PARTITION BY replace(tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento, ' ', '') ORDER BY tfcp.co_seq_fat_cidadao_pec DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS cidadao_cpf,
	    (array_agg(tfcp.nu_cns) FILTER (WHERE tfcp.nu_cns IS NOT NULL) OVER (PARTITION BY replace(tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento, ' ', '') ORDER BY tfcp.co_seq_fat_cidadao_pec DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS cidadao_cns,
       	(array_agg(tfcp.st_faleceu) FILTER (WHERE tfcp.st_faleceu IS NOT NULL) OVER (PARTITION BY replace(tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento, ' ', '') ORDER BY tfcp.co_seq_fat_cidadao_pec DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS se_faleceu,
	    (array_agg(tds.ds_sexo) FILTER (WHERE tds.ds_sexo IS NOT NULL) OVER (PARTITION BY replace(tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento, ' ', '') ORDER BY tfcp.co_seq_fat_cidadao_pec DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING))[1] AS cidadao_sexo,
	    tfci.co_fat_cidadao_pec_responsvl as id_cidadao_pec_responsavel,
	    EXTRACT(YEAR FROM age(CURRENT_DATE::timestamp WITH time zone, tempocidadaopec.dt_registro::timestamp WITH time zone)) * 12 + EXTRACT(MONTH FROM age(CURRENT_DATE::timestamp WITH time zone, tempocidadaopec.dt_registro::timestamp WITH time zone)) AS paciente_idade_atual,
        case 
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
    FROM public.tb_fat_cidadao_pec tfcp
    JOIN public.tb_dim_tempo tempocidadaopec ON tfcp.co_dim_tempo_nascimento = tempocidadaopec.co_seq_dim_tempo
    JOIN public.tb_dim_sexo tds ON tds.co_seq_dim_sexo = tfcp.co_dim_sexo
    left join public.tb_fat_cad_individual tfci on tfci.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec 
),
selecao_denominador as (
WITH base as (
     SELECT 
	     dcp.chave_cidadao,
	     dcp.cidadao_nome,
	     dcp.dt_nascimento,
	     dcp.cidadao_cpf,
	     dcp.cidadao_cns,
	     dcp.cidadao_sexo,
	     dcp.paciente_idade_atual,
	     dcp.se_faleceu,
	     (array_agg(responsavel.no_cidadao) FILTER (WHERE responsavel.no_cidadao IS NOT NULL) OVER (PARTITION BY dcp.chave_cidadao ORDER BY dcp.id_cidadao_pec_responsavel DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following))[1] AS cidadao_nome_responsavel,
	     (array_agg(responsavel.nu_cns) FILTER (WHERE responsavel.nu_cns IS NOT NULL) OVER (PARTITION BY dcp.chave_cidadao ORDER BY dcp.id_cidadao_pec_responsavel DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED following))[1] AS cidadao_cns_responsavel,
	     (array_agg(responsavel.nu_cpf_cidadao) FILTER (WHERE responsavel.nu_cpf_cidadao IS NOT NULL) OVER (PARTITION BY dcp.chave_cidadao ORDER BY dcp.id_cidadao_pec_responsavel DESC ROWS BETWEEN UNBOUNDED PRECEDING AND unbounded following))[1] AS cidadao_cpf_responsavel,
	     EXTRACT(YEAR FROM age(dcp.data_inicio_quadrimestre::timestamp WITH time zone, dcp.dt_nascimento::timestamp WITH time zone)) * 12 + EXTRACT(MONTH FROM age(dcp.data_inicio_quadrimestre::timestamp WITH time zone, dcp.dt_nascimento::timestamp WITH time zone)) AS idade_inicio_do_quadri,
	     EXTRACT(YEAR FROM age(dcp.data_fim_quadrimestre::timestamp WITH time zone, dcp.dt_nascimento::timestamp WITH time zone)) * 12 + EXTRACT(MONTH FROM age(dcp.data_fim_quadrimestre::timestamp WITH time zone, dcp.dt_nascimento::timestamp WITH time zone)) AS idade_fim_do_quadri
	     FROM dados_cidadao_pec dcp
	left join esus_160050_oiapoque_ap_20230405.tb_fat_cidadao_pec responsavel on dcp.id_cidadao_pec_responsavel  = responsavel.co_seq_fat_cidadao_pec 
	) SELECT * FROM base 
	  WHERE idade_fim_do_quadri <= 16 
	  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13
),
historico_vacinacao as (
	SELECT 
        replace(tfcp.no_cidadao || tfcp.co_dim_tempo_nascimento, ' ', '') AS chave_cidadao,
		tfv.co_seq_fat_vacinacao,
		tf.ds_tipo_ficha as tipo_ficha,
		imunobiologico.nu_identificador as codigo_vacina,
		imunobiologico.no_imunobiologico as nome_vacina,
		dose.no_dose_imunobiologico as dose_vacina,
		tempo.dt_registro as data_registro_vacina,
		unidadesaude.nu_cnes as estabelecimento_cnes_aplicacao_vacina,
		unidadesaude.no_unidade_saude as estabelecimento_nome_aplicacao_vacina,
		equipe.nu_ine as equipe_ine_aplicacao_vacina,
		equipe.no_equipe as equipe_nome_aplicacao_vacina,
		profissional.no_profissional as profissional_nome_aplicacao_vacina,
		profissional.nu_cns,
		cbo.nu_cbo,
		cbo.no_cbo
	FROM  public.tb_fat_cidadao_pec tfcp 
	left join public.tb_fat_vacinacao tfv on tfcp.co_seq_fat_cidadao_pec  = tfv.co_fat_cidadao_pec
	LEFT JOIN public.tb_fat_vacinacao_vacina tfvv on tfv.co_seq_fat_vacinacao = tfvv.co_fat_vacinacao 
	LEFT JOIN public.tb_dim_imunobiologico imunobiologico on tfvv.co_dim_imunobiologico = imunobiologico.co_seq_dim_imunobiologico 
	LEFT JOIN public.tb_dim_tempo tempo on tfvv.co_dim_tempo_vacina_aplicada  = tempo.co_seq_dim_tempo
	LEFT JOIN public.tb_dim_dose_imunobiologico dose on dose.co_seq_dim_dose_imunobiologico = tfvv.co_dim_dose_imunobiologico 
	LEFT JOIN public.tb_dim_equipe equipe on equipe.co_seq_dim_equipe = tfv.co_dim_equipe 
	LEFT JOIN public.tb_dim_profissional profissional on profissional.co_seq_dim_profissional = tfvv.co_dim_profissional 
	LEFT JOIN public.tb_dim_cbo cbo on cbo.co_seq_dim_cbo = tfvv.co_dim_cbo
	LEFT JOIN public.tb_dim_unidade_saude unidadesaude on unidadesaude.co_seq_dim_unidade_saude = tfvv.co_dim_unidade_saude 
	left join public.tb_dim_tipo_ficha tf on tfv.co_dim_tipo_ficha = tf.co_seq_dim_tipo_ficha 
	WHERE imunobiologico.nu_identificador in ('22','42','17','29','39','43','46','9')
	AND (cbo.nu_cbo::text ~~ ANY (ARRAY['%2235%'::text, '%2251%'::text, '%2252%'::text, '%2253%'::text, '%2231%'::text, '%3222%'::text]))
	GROUP BY
		tfcp.no_cidadao,
		tf.ds_tipo_ficha,
		tfcp.co_dim_tempo_nascimento,
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
), 
-- INFORMAÇÕES DE ATENDIMENTO MAIS RECENTE
cadastro_individual_recente AS (
-- Filtro de cadastro individual mais recente
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
		FROM public.tb_fat_cad_individual tfci
		JOIN public.tb_fat_cidadao_pec tfcpec
			ON tfcpec.co_seq_fat_cidadao_pec = tfci.co_fat_cidadao_pec
		JOIN selecao_denominador sd 
			ON sd.chave_cidadao = replace(tfcpec.no_cidadao::text||tfcpec.co_dim_tempo_nascimento,' ','')
		LEFT JOIN public.tb_dim_tempo tdt 
			ON tdt.co_seq_dim_tempo = tfci.co_dim_tempo
		LEFT JOIN public.tb_dim_equipe eq
			ON eq.co_seq_dim_equipe = tfci.co_dim_equipe
		LEFT JOIN public.tb_dim_profissional acs
			ON acs.co_seq_dim_profissional = tfci.co_dim_profissional
		LEFT JOIN public.tb_dim_unidade_saude uns
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
	    FROM public.tb_fat_atendimento_individual tfai
	    JOIN public.tb_dim_tempo tdt 
	    	ON tfai.co_dim_tempo = tdt.co_seq_dim_tempo
	    LEFT JOIN public.tb_dim_profissional tdprof
			ON tdprof.co_seq_dim_profissional = tfai.co_dim_profissional_1
		LEFT JOIN public.tb_dim_equipe eq
			ON eq.co_seq_dim_equipe = tfai.co_dim_equipe_1
		LEFT JOIN public.tb_dim_unidade_saude uns 
			ON uns.co_seq_dim_unidade_saude = tfai.co_dim_unidade_saude_1
	    JOIN public.tb_fat_cidadao_pec tfcp 
	    	ON tfcp.co_seq_fat_cidadao_pec = tfai.co_fat_cidadao_pec
	    JOIN public.tb_dim_tempo tempocidadaopec 
	    	ON tempocidadaopec.co_seq_dim_tempo = tfcp.co_dim_tempo_nascimento
	  	JOIN selecao_denominador sd 
			ON sd.chave_cidadao = replace(tfcp.no_cidadao::text||tfcp.co_dim_tempo_nascimento,' ','')
		) 	
		SELECT * FROM base WHERE ultimo_atendimento_individual IS true
),
visita_domiciliar_recente AS (
-- Filtro de visita domiciliar mais recente
	WITH base AS (
		SELECT 
			sd.chave_cidadao,
		    tfcpec.co_seq_fat_cidadao_pec,
			tdt.dt_registro AS data_visita_acs,
			acs.no_profissional AS acs_visita_domiciliar,
			row_number() OVER (PARTITION BY sd.chave_cidadao ORDER BY tdt.dt_registro DESC) = 1 AS ultima_visita_domiciliar
		FROM public.tb_fat_visita_domiciliar visitadomiciliar
		JOIN public.tb_fat_cidadao_pec tfcpec
			ON tfcpec.co_seq_fat_cidadao_pec = visitadomiciliar.co_fat_cidadao_pec 
		JOIN selecao_denominador sd
			ON sd.chave_cidadao = replace(tfcpec.no_cidadao::text||tfcpec.co_dim_tempo_nascimento,' ','')
		LEFT JOIN public.tb_dim_profissional acs
			ON acs.co_seq_dim_profissional = visitadomiciliar.co_dim_profissional
		LEFT JOIN public.tb_dim_tempo tdt 
			ON tdt.co_seq_dim_tempo = visitadomiciliar.co_dim_tempo
		)
	SELECT * FROM base WHERE ultima_visita_domiciliar IS TRUE 
), 
cadastro_domiciliar_recente AS (
-- Filtro de cadstro domiciliar mais recente
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
		FROM public.tb_fat_cad_dom_familia caddomiciliarfamilia
		JOIN public.tb_fat_cad_domiciliar cadomiciliar
			ON cadomiciliar.co_seq_fat_cad_domiciliar = caddomiciliarfamilia.co_fat_cad_domiciliar
		JOIN public.tb_fat_cidadao_pec tfcpec
			ON tfcpec.co_seq_fat_cidadao_pec = caddomiciliarfamilia.co_fat_cidadao_pec
		JOIN selecao_denominador sd
			ON sd.chave_cidadao = replace(tfcpec.no_cidadao::text || tfcpec.co_dim_tempo_nascimento, ' ', '')
		LEFT JOIN public.tb_dim_tempo tdt
			ON tdt.co_seq_dim_tempo = caddomiciliarfamilia.co_dim_tempo
		LEFT JOIN public.tb_dim_equipe eq
			ON eq.co_seq_dim_equipe = caddomiciliarfamilia.co_dim_equipe
		LEFT JOIN public.tb_dim_profissional acs
			ON acs.co_seq_dim_profissional = caddomiciliarfamilia.co_dim_profissional
		LEFT JOIN public.tb_dim_unidade_saude uns
			ON uns.co_seq_dim_unidade_saude = caddomiciliarfamilia.co_dim_unidade_saude
	)
	SELECT * FROM base WHERE ultimo_cadastro_domiciliar_familia IS true
),
vinculacao_equipe AS (
	SELECT
		sd.chave_cidadao,
		cir.data_cadastro_individual as data_ultimo_cadastro_individual,
		cir.cnes_estabelecimento_cad_individual as estabelecimento_cnes_cadastro,
		cir.estabelecimento_cad_individual as estabelecimento_nome_cadastro, 
		cir.ine_equipe_cad_individual as equipe_ine_cadastro,
		cir.equipe_cad_individual as equipe_nome_cadastro,
		cir.acs_cad_individual as acs_nome_cadastro,
		ar.estabelecimento_cnes_atendimento_recente as estabelecimento_cnes_atendimento, 
		ar.estabelecimento_atendimento_recente as estabelecimento_nome_atendimento, 
		ar.ine_equipe_atendimento_recente as equipe_ine_atendimento,
		ar.equipe_atendimento_recente as equipe_nome_atendimento,
		ar.data_registro as data_ultimo_atendimento_individual,
		vdr.data_visita_acs as data_ultima_vista_domiciliar,
		vdr.acs_visita_domiciliar as acs_nome_visita
	FROM selecao_denominador sd
	LEFT JOIN cadastro_individual_recente cir
		ON cir.chave_cidadao = sd.chave_cidadao
	LEFT JOIN visita_domiciliar_recente vdr
		ON vdr.chave_cidadao = sd.chave_cidadao
	LEFT JOIN cadastro_domiciliar_recente cdr
		ON cdr.chave_cidadao = sd.chave_cidadao
	left join atendimento_mais_recente ar
		on ar.chave_cidadao = sd.chave_cidadao
	GROUP BY
		sd.chave_cidadao,
		cir.data_cadastro_individual,
		cir.cnes_estabelecimento_cad_individual,
		cir.estabelecimento_cad_individual, 
		cir.ine_equipe_cad_individual,
		cir.equipe_cad_individual,
		cir.acs_cad_individual,
		ar.estabelecimento_cnes_atendimento_recente, 
		ar.estabelecimento_atendimento_recente, 
		ar.ine_equipe_atendimento_recente,
		ar.equipe_atendimento_recente,
		ar.data_registro,
		vdr.acs_visita_domiciliar,
		vdr.data_visita_acs 
) select
	sd.chave_cidadao,
	sd.cidadao_nome,
	sd.cidadao_cpf,
	sd.cidadao_cns,
	sd.cidadao_sexo,
	sd.dt_nascimento,
	sd.cidadao_nome_responsavel,
	sd.cidadao_cns_responsavel,
	sd.cidadao_cpf_responsavel,
	sd.paciente_idade_atual,
	sd.idade_inicio_do_quadri,
	sd.idade_fim_do_quadri,
	sd.se_faleceu,
	hvc.co_seq_fat_vacinacao,
	hvc.tipo_ficha,
	hvc.codigo_vacina,
	hvc.nome_vacina,
	hvc.dose_vacina,
	hvc.data_registro_vacina,
	hvc.estabelecimento_cnes_aplicacao_vacina,
	hvc.estabelecimento_nome_aplicacao_vacina,
	hvc.equipe_ine_aplicacao_vacina,
	hvc.equipe_nome_aplicacao_vacina,
	hvc.profissional_nome_aplicacao_vacina,
	vinculacao.data_ultimo_cadastro_individual,
	vinculacao.estabelecimento_cnes_cadastro,
	vinculacao.estabelecimento_nome_cadastro, 
	vinculacao.equipe_ine_cadastro,
	vinculacao.equipe_nome_cadastro,
	vinculacao.acs_nome_cadastro,
	vinculacao.estabelecimento_cnes_atendimento, 
	vinculacao.estabelecimento_nome_atendimento, 
	vinculacao.equipe_ine_atendimento,
	vinculacao.equipe_nome_atendimento,
	vinculacao.data_ultimo_atendimento_individual,
	vinculacao.data_ultima_vista_domiciliar,
	vinculacao.acs_nome_visita
	now() as criacao_data
from selecao_denominador sd 
	left join historico_vacinacao hvc on sd.chave_cidadao=hvc.chave_cidadao 
	left join vinculacao_equipe vinculacao on vinculacao.chave_cidadao = sd.chave_cidadao
