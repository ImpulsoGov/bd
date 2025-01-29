-- DENOMINADOR - todas pessoas com hipertensao autorrefeira ou diagnósticada
WITH possui_hipertensao_autorreferida AS (
        WITH ultimo_cadastro_individual AS (
                SELECT 
                        tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento AS chave_paciente,
                        cadastro.st_hipertensao_arterial,
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
                ci.st_hipertensao_arterial = 1 AS possui_hipertensao_autorreferida
        FROM ultimo_cadastro_individual ci 
        WHERE ci.ultimo_cadastro_individual IS TRUE 
                AND ci.st_hipertensao_arterial = 1
)
, possui_hipertensao_diagnosticada AS (
        SELECT 
                DISTINCT 
                tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento AS chave_paciente,
                TRUE AS possui_hipertensao_diagnosticada
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
        WHERE ((ciap.nu_ciap::text = ANY (ARRAY['K86'::character varying::text, 'K87'::character varying::text, 'ABP005'::character varying::text]))
                OR (cid.nu_cid::text = ANY (ARRAY['I10'::character varying::text, 'I11'::character varying::text, 'I110'::character varying::text, 'I119'::character varying::text, 'I12'::character varying::text, 'I120'::character varying::text, 'I129'::character varying::text, 'I13'::character varying::text, 'I130'::character varying::text, 'I131'::character varying::text, 'I132'::character varying::text, 'I139'::character varying::text, 'I15'::character varying::text, 'I150'::character varying::text, 'I151'::character varying::text, 'I152'::character varying::text, 'I158'::character varying::text, 'I159'::character varying::text, 'O10'::character varying::text, 'O100'::character varying::text, 'O101'::character varying::text, 'O102'::character varying::text, 'O103'::character varying::text, 'O104'::character varying::text, 'O109'::character varying::text, 'O11'::character varying::text]))) 
                AND (cbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text]))
                AND tempo.nu_ano <> 3000 
                AND tempo.dt_registro <= current_date
)
, denominador_hipertensos AS (
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
                COALESCE(haref.possui_hipertensao_autorreferida,FALSE) AS possui_hipertensao_autorreferida,
                COALESCE(hdia.possui_hipertensao_diagnosticada,FALSE) AS possui_hipertensao_diagnosticada,
                COALESCE(FIRST_VALUE(tfcp.st_faleceu) OVER (PARTITION BY tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento ORDER BY tfcp.co_seq_fat_cidadao_pec DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),0) AS se_faleceu
        FROM tb_fat_cidadao_pec tfcp
        LEFT JOIN tb_dim_tempo tdt 
                ON tfcp.co_dim_tempo_nascimento = tdt.co_seq_dim_tempo
        LEFT JOIN tb_dim_sexo tds 
                ON tfcp.co_dim_sexo = tds.co_seq_dim_sexo
        LEFT JOIN possui_hipertensao_autorreferida haref
                ON haref.chave_paciente = tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento
        LEFT JOIN possui_hipertensao_diagnosticada hdia
                ON hdia.chave_paciente = tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento
        WHERE (hdia.possui_hipertensao_diagnosticada OR haref.possui_hipertensao_autorreferida)
)
-- NUMERADOR
, afericao_pressao AS (
    WITH ultima_ficha_procedimento AS (
        SELECT 
                dh.chave_paciente,
                tempo.dt_registro AS dt_afericao_pressao_mais_recente,
                eq.nu_ine AS equipe_ine_procedimento,
                eq.no_equipe AS equipe_nome_procedimento,
                prof.no_profissional AS profissional_nome_procedimento,
                ROW_NUMBER() OVER (PARTITION BY dh.chave_paciente ORDER BY tempo.dt_registro DESC, fichaproced.co_seq_fat_proced_atend_proced DESC) = 1 AS ultimo_procedimento
        FROM tb_fat_proced_atend_proced fichaproced
        JOIN tb_fat_cidadao_pec tfcp 
                ON fichaproced.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec 
        JOIN denominador_hipertensos dh 
                ON dh.chave_paciente = tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento
        JOIN tb_dim_procedimento proced 
                ON fichaproced.co_dim_procedimento = proced.co_seq_dim_procedimento
        JOIN tb_dim_tempo tempo 
                ON fichaproced.co_dim_tempo = tempo.co_seq_dim_tempo
        JOIN tb_dim_cbo cbo 
                ON fichaproced.co_dim_cbo = cbo.co_seq_dim_cbo 
        LEFT JOIN tb_dim_equipe eq
                ON eq.co_seq_dim_equipe = fichaproced.co_dim_equipe
        LEFT JOIN tb_dim_profissional prof
                ON prof.co_seq_dim_profissional = fichaproced.co_dim_profissional
        WHERE  (proced.co_proced::text = ANY (ARRAY['0301100039'::character varying::text, 'ABPG033'::character varying::text])) 
                AND (cbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text]))
                AND tempo.dt_registro <= current_date
                AND tempo.nu_ano <> 3000 
    )
    SELECT 
        *
    FROM ultima_ficha_procedimento
    WHERE ultimo_procedimento IS TRUE 
)
, consulta_hipertensao AS (
        SELECT 
                dh.chave_paciente,
                max(tempo.dt_registro) AS dt_consulta_mais_recente
        FROM tb_fat_atendimento_individual atendimento
        JOIN tb_fat_cidadao_pec tfcp 
                ON atendimento.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec 
        JOIN denominador_hipertensos dh 
                ON dh.chave_paciente = tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento
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
        WHERE  ((ciap.nu_ciap::text = ANY (ARRAY['K86'::character varying::text, 'K87'::character varying::text, 'ABP005'::character varying::text])) 
                OR (cid.nu_cid::text = ANY (ARRAY['I10'::character varying::text, 'I11'::character varying::text, 'I110'::character varying::text, 'I119'::character varying::text, 'I12'::character varying::text, 'I120'::character varying::text, 'I129'::character varying::text, 'I13'::character varying::text, 'I130'::character varying::text, 'I131'::character varying::text, 'I132'::character varying::text, 'I139'::character varying::text, 'I15'::character varying::text, 'I150'::character varying::text, 'I151'::character varying::text, 'I152'::character varying::text, 'I158'::character varying::text, 'I159'::character varying::text, 'O10'::character varying::text, 'O100'::character varying::text, 'O101'::character varying::text, 'O102'::character varying::text, 'O103'::character varying::text, 'O104'::character varying::text, 'O109'::character varying::text, 'O11'::character varying::text])))
                AND (cbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text]))
                AND tempo.dt_registro <= current_date
                AND tempo.nu_ano <> 3000 
        GROUP BY 1
)
-- Informações de vinculação
, cadastro_individual_recente AS (
-- Dados do cadastro individual (dados para vinculação de equipe e ACS do cidadao)
                SELECT 
                        dh.chave_paciente,
                        tdt.dt_registro AS data_ultimo_cadastro,
                        tfci.nu_micro_area AS micro_area_cad_individual,
                        uns.nu_cnes AS estabelecimento_cnes_cadastro,
                        uns.no_unidade_saude AS estabelecimento_nome_cadastro,
                        eq.nu_ine AS equipe_ine_cadastro,
                        eq.no_equipe AS equipe_nome_cadastro,
                        acs.no_profissional AS acs_nome_cadastro,
                        COALESCE(cidadaoterritoriorecente.st_mudou_se,0) AS se_mudou,
                        cidadaoterritoriorecente.co_fat_familia_territorio as co_fat_familia_territorio,
                        cci.nu_celular_cidadao as cidadao_celular,
                        st.ds_dim_situacao_trabalho as cidadao_situacao_trabalho,
                        pct.ds_povo_comunidade_tradicional as cidadao_povo_comunidade_tradicional,
                        idg.ds_identidade_genero as cidadao_identidade_genero,
                        tdrc.ds_raca_cor as cidadao_raca_cor,
                        tfci.st_plano_saude_privado as cidadao_plano_saude_privado,
                        ROW_NUMBER() OVER (PARTITION BY dh.chave_paciente ORDER BY tfci.co_seq_fat_cad_individual DESC) = 1 AS ultimo_cadastro_individual
                FROM tb_fat_cad_individual tfci
                JOIN tb_fat_cidadao_pec tfcp
                        ON tfcp.co_seq_fat_cidadao_pec = tfci.co_fat_cidadao_pec
                JOIN denominador_hipertensos dh 
                        ON dh.chave_paciente = tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento
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
, visita_domiciliar_recente AS (
-- Dados das visitas domiciliares realizadas pelos ACS (dados para vinculação de ACS da mulher)
                SELECT 
                        dh.chave_paciente,
                        tfcp.co_seq_fat_cidadao_pec,
                        tdt.dt_registro AS data_visita_acs,
                        acs.no_profissional AS acs_nome_visita,
                        ROW_NUMBER() OVER (PARTITION BY dh.chave_paciente ORDER BY tdt.dt_registro DESC) = 1 AS ultima_visita_domiciliar
                FROM tb_fat_visita_domiciliar visitadomiciliar
                JOIN tb_fat_cidadao_pec tfcp
                        ON tfcp.co_seq_fat_cidadao_pec = visitadomiciliar.co_fat_cidadao_pec 
                JOIN denominador_hipertensos dh 
                        ON dh.chave_paciente = tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento
                LEFT JOIN tb_dim_profissional acs
                        ON acs.co_seq_dim_profissional = visitadomiciliar.co_dim_profissional
                LEFT JOIN tb_dim_tempo tdt 
                        ON tdt.co_seq_dim_tempo = visitadomiciliar.co_dim_tempo
                )
, 
visitas_ubs_12_meses as (
	SELECT 
			dh.chave_paciente,
			COUNT(*) as numero_visitas_ubs_ultimos_12_meses
	FROM tb_fat_atendimento_individual tfai
    JOIN tb_fat_cidadao_pec tfcp 
	    	ON tfcp.co_seq_fat_cidadao_pec = tfai.co_fat_cidadao_pec
	JOIN denominador_hipertensos dh 
			ON dh.chave_paciente = tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento
	JOIN tb_dim_local_atendimento tdla
            ON tfai.co_dim_local_atendimento = tdla.co_seq_dim_local_atendimento
	WHERE 
		tdla.ds_local_atendimento = 'UBS'
        AND tfai.dt_inicial_atendimento >= (CURRENT_DATE - INTERVAL '12 months')
    GROUP by dh.chave_paciente
),
cadastro_domiciliar_recente AS (
-- Dados do cadastro da família e do domicílio da mulher (dados para vinculação de ACS da mulher)
                SELECT 
                        dh.chave_paciente,
                        tdt.dt_registro AS data_cadastro_dom_familia,
                        caddomiciliarfamilia.nu_micro_area AS micro_area_domicilio,
                        uns.nu_cnes AS cnes_estabelecimento_cad_dom_familia,
                        uns.no_unidade_saude AS estabelecimento_cad_dom_familia,
                        eq.nu_ine AS ine_equipe_cad_dom_familia,
                        eq.no_equipe AS equipe_cad_dom_familia,
                        acs.no_profissional AS acs_cad_dom_familia,
                        NULLIF(concat(cadomiciliar.no_logradouro, ', ', cadomiciliar.nu_num_logradouro), ', '::text) AS paciente_endereco,
                        ROW_NUMBER() OVER (PARTITION BY dh.chave_paciente ORDER BY tdt.dt_registro DESC) = 1  AS ultimo_cadastro_domiciliar_familia
                FROM tb_fat_cad_dom_familia caddomiciliarfamilia
                JOIN tb_fat_cad_domiciliar cadomiciliar 
                        ON cadomiciliar.co_seq_fat_cad_domiciliar = caddomiciliarfamilia.co_fat_cad_domiciliar
                JOIN tb_fat_cidadao_pec tfcp
                        ON tfcp.co_seq_fat_cidadao_pec = caddomiciliarfamilia.co_fat_cidadao_pec 
                JOIN denominador_hipertensos dh 
                        ON dh.chave_paciente = tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento
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
                dh.chave_paciente,
                tdt.dt_registro AS dt_ultima_consulta,
                unidadeatendimentorecente.nu_cnes AS estabelecimento_cnes_atendimento,
                unidadeatendimentorecente.no_unidade_saude AS estabelecimento_nome_atendimento,
                equipeatendimentorecente.nu_ine AS equipe_ine_atendimento,
                equipeatendimentorecente.no_equipe AS equipe_nome_atendimento,
                prof.no_profissional AS profissional_nome_atendimento,
                ROW_NUMBER() OVER (PARTITION BY dh.chave_paciente ORDER BY tdt.dt_registro DESC) = 1  AS ultimo_atendimento
        FROM tb_fat_atendimento_individual atendimento  
        JOIN tb_dim_tempo tdt 
                ON atendimento.co_dim_tempo = tdt.co_seq_dim_tempo
        JOIN tb_fat_cidadao_pec tfcp
                ON tfcp.co_seq_fat_cidadao_pec = atendimento.co_fat_cidadao_pec 
        JOIN denominador_hipertensos dh 
                ON dh.chave_paciente = tfcp.no_cidadao||tfcp.co_dim_tempo_nascimento
        LEFT JOIN tb_dim_equipe equipeatendimentorecente 
                ON equipeatendimentorecente.co_seq_dim_equipe = atendimento.co_dim_equipe_1
        LEFT JOIN tb_dim_profissional prof
                ON prof.co_seq_dim_profissional = atendimento.co_dim_profissional_1
        LEFT JOIN tb_dim_unidade_saude unidadeatendimentorecente 
                ON unidadeatendimentorecente.co_seq_dim_unidade_saude = atendimento.co_dim_unidade_saude_1                             
)
        SELECT 
                CASE
            WHEN date_part('month'::text, current_date) >= 1::double precision AND date_part('month'::text, current_date) <= 4::double precision THEN concat(date_part('year'::text, current_date), '.Q1')
            WHEN date_part('month'::text, current_date) >= 5::double precision AND date_part('month'::text, current_date) <= 8::double precision THEN concat(date_part('year'::text, current_date), '.Q2')
            WHEN date_part('month'::text, current_date) >= 9::double precision AND date_part('month'::text, current_date) <= 12::double precision THEN concat(date_part('year'::text, current_date), '.Q3')
            ELSE NULL::text
        END AS quadrimestre_atual,
        CASE
            WHEN ap.dt_afericao_pressao_mais_recente <=
            CASE
                WHEN date_part('month'::text, current_date) >= 1::double precision AND date_part('month'::text, current_date) <= 4::double precision THEN concat(date_part('year'::text, current_date), '-04-30')
                WHEN date_part('month'::text, current_date) >= 5::double precision AND date_part('month'::text, current_date) <= 8::double precision THEN concat(date_part('year'::text, current_date), '-08-31')
                WHEN date_part('month'::text, current_date) >= 9::double precision AND date_part('month'::text, current_date) <= 12::double precision THEN concat(date_part('year'::text, current_date), '-12-31')
                ELSE NULL::text
            END::date AND ap.dt_afericao_pressao_mais_recente >= (
            CASE
                WHEN date_part('month'::text, current_date) >= 1::double precision AND date_part('month'::text, current_date) <= 4::double precision THEN concat(date_part('year'::text, current_date), '-04-30')
                WHEN date_part('month'::text, current_date) >= 5::double precision AND date_part('month'::text, current_date) <= 8::double precision THEN concat(date_part('year'::text, current_date), '-08-31')
                WHEN date_part('month'::text, current_date) >= 9::double precision AND date_part('month'::text, current_date) <= 12::double precision THEN concat(date_part('year'::text, current_date), '-12-31')
                ELSE NULL::text
            END::date - '6 months'::interval) THEN true
            ELSE false
        END AS realizou_afericao_ultimos_6_meses,
        ap.dt_afericao_pressao_mais_recente,
        CASE
            WHEN ch.dt_consulta_mais_recente <=
            CASE
                WHEN date_part('month'::text, current_date) >= 1::double precision AND date_part('month'::text, current_date) <= 4::double precision THEN concat(date_part('year'::text, current_date), '-04-30')
                WHEN date_part('month'::text, current_date) >= 5::double precision AND date_part('month'::text, current_date) <= 8::double precision THEN concat(date_part('year'::text, current_date), '-08-31')
                WHEN date_part('month'::text, current_date) >= 9::double precision AND date_part('month'::text, current_date) <= 12::double precision THEN concat(date_part('year'::text, current_date), '-12-31')
                ELSE NULL::text
            END::date AND ch.dt_consulta_mais_recente >= (
            CASE
                WHEN date_part('month'::text, current_date) >= 1::double precision AND date_part('month'::text, current_date) <= 4::double precision THEN concat(date_part('year'::text, current_date), '-04-30')
                WHEN date_part('month'::text, current_date) >= 5::double precision AND date_part('month'::text, current_date) <= 8::double precision THEN concat(date_part('year'::text, current_date), '-08-31')
                WHEN date_part('month'::text, current_date) >= 9::double precision AND date_part('month'::text, current_date) <= 12::double precision THEN concat(date_part('year'::text, current_date), '-12-31')
                ELSE NULL::text
            END::date - '6 months'::interval) THEN true
            ELSE false
        END AS realizou_consulta_ultimos_6_meses,
    ch.dt_consulta_mais_recente,
    dh.co_seq_fat_cidadao_pec,
    dh.cidadao_cpf,
    dh.cidadao_cns,
    dh.cidadao_nome,
    dh.cidadao_nome_social,
    dh.cidadao_sexo,
    dh.dt_nascimento,
    ar.estabelecimento_cnes_atendimento,
    cir.estabelecimento_cnes_cadastro,
    ar.estabelecimento_nome_atendimento,
    cir.estabelecimento_nome_cadastro,
    ar.equipe_ine_atendimento,
    cir.equipe_ine_cadastro,
    ap.equipe_ine_procedimento,
    ar.equipe_nome_atendimento,
    cir.equipe_nome_cadastro,
    ap.equipe_nome_procedimento,
    cir.acs_nome_cadastro,
    vdr.acs_nome_visita,
    ar.profissional_nome_atendimento,
    ap.profissional_nome_procedimento,
    dh.possui_hipertensao_autorreferida,
    dh.possui_hipertensao_diagnosticada,
    cir.data_ultimo_cadastro,
    ar.dt_ultima_consulta,
    dh.se_faleceu,
    cir.se_mudou, 
    cir.co_fat_familia_territorio,
    dh.cidadao_telefone,
    cir.cidadao_celular,
    cir.cidadao_situacao_trabalho,
    cir.cidadao_povo_comunidade_tradicional,
    cir.cidadao_identidade_genero,
    cir.cidadao_raca_cor,
    cir.cidadao_plano_saude_privado,
    vu.numero_visitas_ubs_ultimos_12_meses,
    now() as criacao_data
FROM denominador_hipertensos dh
LEFT JOIN afericao_pressao ap 
        ON ap.chave_paciente = dh.chave_paciente
LEFT JOIN consulta_hipertensao ch
        ON ch.chave_paciente = dh.chave_paciente
LEFT JOIN cadastro_individual_recente cir 
        ON cir.chave_paciente = dh.chave_paciente
        AND cir.ultimo_cadastro_individual IS TRUE 
LEFT JOIN visita_domiciliar_recente vdr 
        ON vdr.chave_paciente = dh.chave_paciente
        AND vdr.ultima_visita_domiciliar IS TRUE 
LEFT JOIN cadastro_domiciliar_recente cdr 
        ON cdr.chave_paciente = dh.chave_paciente
        AND cdr.ultimo_cadastro_domiciliar_familia IS TRUE
LEFT JOIN atendimento_recente ar 
        ON ar.chave_paciente = dh.chave_paciente
        AND ar.ultimo_atendimento IS TRUE
LEFT JOIN visitas_ubs_12_meses vu on dh.chave_paciente = vu.chave_paciente