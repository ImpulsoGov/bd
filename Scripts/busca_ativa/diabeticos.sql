SELECT
        CASE
            WHEN date_part('quarter'::text, CURRENT_DATE) = 1::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '.Q1')
            WHEN date_part('quarter'::text, CURRENT_DATE) = 2::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '.Q2')
            WHEN date_part('quarter'::text, CURRENT_DATE) = 3::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '.Q3')
            ELSE NULL::text
        END AS quadrimestre_atual,
        CASE
            WHEN v1.dt_afericao_pressao_mais_recente <=
            CASE
                WHEN date_part('quarter'::text, CURRENT_DATE) = 1::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-04-30')
                WHEN date_part('quarter'::text, CURRENT_DATE) = 2::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-08-31')
                WHEN date_part('quarter'::text, CURRENT_DATE) = 3::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-12-31')
                ELSE NULL::text
            END::date AND v1.dt_afericao_pressao_mais_recente >= (
            CASE
                WHEN date_part('quarter'::text, CURRENT_DATE) = 1::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-04-30')
                WHEN date_part('quarter'::text, CURRENT_DATE) = 2::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-08-31')
                WHEN date_part('quarter'::text, CURRENT_DATE) = 3::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-12-31')
                ELSE NULL::text
            END::date - '180 days'::interval) THEN true
            ELSE false
        END AS realizou_afericao_ultimos_6_meses,
    v1.dt_afericao_pressao_mais_recente,
        CASE
            WHEN v1.dt_consulta_mais_recente <=
            CASE
                WHEN date_part('quarter'::text, CURRENT_DATE) = 1::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-04-30')
                WHEN date_part('quarter'::text, CURRENT_DATE) = 2::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-08-31')
                WHEN date_part('quarter'::text, CURRENT_DATE) = 3::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-12-31')
                ELSE NULL::text
            END::date AND v1.dt_consulta_mais_recente >= (
            CASE
                WHEN date_part('quarter'::text, CURRENT_DATE) = 1::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-04-30')
                WHEN date_part('quarter'::text, CURRENT_DATE) = 2::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-08-31')
                WHEN date_part('quarter'::text, CURRENT_DATE) = 3::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-12-31')
                ELSE NULL::text
            END::date - '180 days'::interval) THEN true
            ELSE false
        END AS realizou_consulta_ultimos_6_meses,
    v1.dt_consulta_mais_recente,
    v1.cidadao_cpf,
    v1.cidadao_cns,
    v1.cidadao_nome,
    v1.cidadao_nome_social,
    v1.cidadao_sexo,
    v1.dt_nascimento,
    v1.estabelecimento_cnes_atendimento,
    v1.estabelecimento_cnes_cadastro,
    v1.estabelecimento_nome_atendimento,
    v1.estabelecimento_nome_cadastro,
    v1.equipe_ine_atendimento,
    v1.equipe_ine_cadastro,
    v1.equipe_nome_atendimento,
    v1.equipe_nome_cadastro,
    v1.acs_nome_cadastro,
    v1.acs_nome_visita,
    v1.possui_hipertensao_autoreferida,
    v1.possui_hipertensao_diagnosticada,
    v1.data_ultimo_cadastro,
    v1.dt_ultima_consulta,
    v1.nu_ano
   FROM ( SELECT row_number() OVER (PARTITION BY tfcp.no_cidadao, tdt.dt_registro ORDER BY tfcp.no_cidadao DESC) AS r,
            ( SELECT tempo.dt_registro
                   FROM tb_fat_proced_atend_proced fichaproced
                     JOIN tb_dim_procedimento proced ON fichaproced.co_dim_procedimento = proced.co_seq_dim_procedimento
                     JOIN tb_dim_tempo tempo ON fichaproced.co_dim_tempo = tempo.co_seq_dim_tempo
                     JOIN tb_dim_cbo cbo ON fichaproced.co_dim_cbo = cbo.co_seq_dim_cbo AND (cbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text]))
                  WHERE fichaproced.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec AND (proced.co_proced::text = ANY (ARRAY['0301100039'::character varying::text, 'ABPG033'::character varying::text]))
                  ORDER BY fichaproced.co_dim_tempo DESC
                 LIMIT 1) AS dt_afericao_pressao_mais_recente,
            ( SELECT max(tempo.dt_registro) AS max
                   FROM tb_fat_atendimento_individual atendimento
                     JOIN tb_dim_tempo tempo ON atendimento.co_dim_tempo = tempo.co_seq_dim_tempo
                     JOIN tb_fat_atd_ind_problemas problemas ON atendimento.co_seq_fat_atd_ind = problemas.co_fat_atd_ind
                     JOIN tb_dim_cbo cbo ON problemas.co_dim_cbo_1 = cbo.co_seq_dim_cbo AND (cbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text]))
                     LEFT JOIN tb_dim_ciap ciap ON problemas.co_dim_ciap = ciap.co_seq_dim_ciap 
                     LEFT JOIN tb_dim_cid cid ON problemas.co_dim_cid = cid.co_seq_dim_cid 
                  WHERE atendimento.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec
                  AND (ciap.nu_ciap::text = ANY (ARRAY['K86'::character varying::text, 'K87'::character varying::text, 'ABP005'::character varying::text]))
                  AND (cid.nu_cid::text = ANY (ARRAY['I10'::character varying::text, 'I11'::character varying::text, 'I110'::character varying::text, 'I119'::character varying::text, 'I12'::character varying::text, 'I120'::character varying::text, 'I129'::character varying::text, 'I13'::character varying::text, 'I130'::character varying::text, 'I131'::character varying::text, 'I132'::character varying::text, 'I139'::character varying::text, 'I15'::character varying::text, 'I150'::character varying::text, 'I151'::character varying::text, 'I152'::character varying::text, 'I158'::character varying::text, 'I159'::character varying::text, 'O10'::character varying::text, 'O100'::character varying::text, 'O101'::character varying::text, 'O102'::character varying::text, 'O103'::character varying::text, 'O104'::character varying::text, 'O109'::character varying::text, 'O11'::character varying::text]))
                  ) AS dt_consulta_mais_recente,
            tfcp.nu_cpf_cidadao AS cidadao_cpf,
            tfcp.nu_cns AS cidadao_cns,
            tfcp.no_cidadao AS cidadao_nome,
            tfcp.no_social_cidadao AS cidadao_nome_social,
            tds.ds_sexo AS cidadao_sexo,
            tdt.dt_registro AS dt_nascimento,
            NULLIF(unidadeatendimentorecente.nu_cnes::text, '-'::text) AS estabelecimento_cnes_atendimento,
            NULLIF(unidadecadastrorecente.nu_cnes::text, '-'::text) AS estabelecimento_cnes_cadastro,
            NULLIF(unidadeatendimentorecente.no_unidade_saude::text, 'Não informado'::text) AS estabelecimento_nome_atendimento,
            COALESCE(NULLIF(unidadecadastrorecente.no_unidade_saude::text, 'Não informado'::text), unidadeatendimentorecente.no_unidade_saude::text) AS estabelecimento_nome_cadastro,
            NULLIF(equipeatendimentorecente.nu_ine::text, '-'::text) AS equipe_ine_atendimento,
            NULLIF(equipeacadastrorecente.nu_ine::text, '-'::text) AS equipe_ine_cadastro,
            NULLIF(equipeatendimentorecente.no_equipe::text, 'SEM EQUIPE'::text) AS equipe_nome_atendimento,
            NULLIF(equipeatendimentorecente.no_equipe::text, 'SEM EQUIPE'::text) AS equipe_nome_cadastro,
            NULLIF(acscadastrorecente.no_profissional::text, 'SEM EQUIPE'::text) AS acs_nome_cadastro,
            NULLIF(acsvisitarecente.no_profissional::text, 'SEM EQUIPE'::text) AS acs_nome_visita,
            (( SELECT cadastro.st_hipertensao_arterial
                   FROM tb_fat_cad_individual cadastro
                     JOIN tb_dim_tempo tempo ON cadastro.co_dim_tempo = tempo.co_seq_dim_tempo
                  WHERE cadastro.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec AND tempo.nu_ano <> 3000
                  ORDER BY tempo.dt_registro DESC
                 LIMIT 1)) = 1 AS possui_hipertensao_autoreferida,
            (( SELECT count(*) AS count
                   FROM tb_fat_atendimento_individual atendimento
                     JOIN tb_dim_tempo tempo ON atendimento.co_dim_tempo = tempo.co_seq_dim_tempo
                     JOIN tb_fat_atd_ind_problemas problemas ON atendimento.co_seq_fat_atd_ind = problemas.co_fat_atd_ind
                     JOIN tb_dim_cbo cbo ON problemas.co_dim_cbo_1 = cbo.co_seq_dim_cbo AND (cbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text]))
                     LEFT JOIN tb_dim_ciap ciap ON problemas.co_dim_ciap = ciap.co_seq_dim_ciap
                     LEFT JOIN tb_dim_cid cid ON problemas.co_dim_cid = cid.co_seq_dim_cid
                  WHERE atendimento.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec AND ((ciap.nu_ciap::text = ANY (ARRAY['K86'::character varying::text, 'K87'::character varying::text, 'ABP005'::character varying::text])) OR (cid.nu_cid::text = ANY (ARRAY['I10'::character varying::text, 'I11'::character varying::text, 'I110'::character varying::text, 'I119'::character varying::text, 'I12'::character varying::text, 'I120'::character varying::text, 'I129'::character varying::text, 'I13'::character varying::text, 'I130'::character varying::text, 'I131'::character varying::text, 'I132'::character varying::text, 'I139'::character varying::text, 'I15'::character varying::text, 'I150'::character varying::text, 'I151'::character varying::text, 'I152'::character varying::text, 'I158'::character varying::text, 'I159'::character varying::text, 'O10'::character varying::text, 'O100'::character varying::text, 'O101'::character varying::text, 'O102'::character varying::text, 'O103'::character varying::text, 'O104'::character varying::text, 'O109'::character varying::text, 'O11'::character varying::text]))) AND tempo.nu_ano <> 3000)) > 0 AS possui_hipertensao_diagnosticada,
            ( SELECT max(tempo.dt_registro) AS max
                   FROM tb_fat_cad_individual cadastro
                     JOIN tb_dim_tempo tempo ON cadastro.co_dim_tempo = tempo.co_seq_dim_tempo
                  WHERE cadastro.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec) AS data_ultimo_cadastro,
            ( SELECT max(tempo.dt_registro) AS max
                   FROM tb_fat_atendimento_individual atendimento
                     JOIN tb_dim_tempo tempo ON atendimento.co_dim_tempo = tempo.co_seq_dim_tempo
                  WHERE atendimento.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec) AS dt_ultima_consulta,
            tfcp.st_faleceu AS se_faleceu,
            cidadaoterritoriorecente.st_mudou_se,
            tdt.nu_ano
           FROM tb_fat_cidadao_pec tfcp
             JOIN tb_dim_tempo tdt ON tfcp.co_dim_tempo_nascimento = tdt.co_seq_dim_tempo
             JOIN tb_dim_identidade_genero tidg ON tfcp.co_dim_identidade_genero = tidg.co_seq_dim_identidade_genero
             JOIN tb_dim_sexo tds ON tfcp.co_dim_sexo = tds.co_seq_dim_sexo
             LEFT JOIN tb_fat_cad_individual tfcirecente ON tfcirecente.co_seq_fat_cad_individual = (( SELECT cadastroindividual.co_seq_fat_cad_individual
                   FROM tb_fat_cad_individual cadastroindividual
                  WHERE (cadastroindividual.co_fat_cidadao_pec IN ( SELECT tfccadastro.co_seq_fat_cidadao_pec
                           FROM tb_fat_cidadao_pec tfccadastro
                          WHERE tfccadastro.no_cidadao::text = tfcp.no_cidadao::text AND tfccadastro.co_dim_tempo_nascimento = tfcp.co_dim_tempo_nascimento))
                  ORDER BY cadastroindividual.co_dim_tempo DESC
                 LIMIT 1))
             LEFT JOIN tb_fat_atendimento_individual tfairecente ON tfairecente.co_seq_fat_atd_ind = (( SELECT atendimentoindividual.co_seq_fat_atd_ind
                   FROM tb_fat_atendimento_individual atendimentoindividual
                  WHERE (atendimentoindividual.co_fat_cidadao_pec IN ( SELECT tfcatendimento.co_seq_fat_cidadao_pec
                           FROM tb_fat_cidadao_pec tfcatendimento
                          WHERE tfcatendimento.no_cidadao::text = tfcp.no_cidadao::text AND tfcatendimento.co_dim_tempo_nascimento = tfcp.co_dim_tempo_nascimento))
                  ORDER BY atendimentoindividual.co_dim_tempo DESC
                 LIMIT 1))
             LEFT JOIN tb_fat_visita_domiciliar tfvdrecente ON tfvdrecente.co_seq_fat_visita_domiciliar = (( SELECT visitadomiciliar.co_seq_fat_visita_domiciliar
                   FROM tb_fat_visita_domiciliar visitadomiciliar
                  WHERE (visitadomiciliar.co_fat_cidadao_pec IN ( SELECT tfcvisita.co_seq_fat_cidadao_pec
                           FROM tb_fat_cidadao_pec tfcvisita
                          WHERE tfcvisita.no_cidadao::text = tfcp.no_cidadao::text AND tfcvisita.co_dim_tempo_nascimento = tfcp.co_dim_tempo_nascimento))
                  ORDER BY visitadomiciliar.co_dim_tempo DESC
                 LIMIT 1))
             LEFT JOIN tb_dim_equipe equipeatendimentorecente ON equipeatendimentorecente.co_seq_dim_equipe = tfairecente.co_dim_equipe_1
             LEFT JOIN tb_dim_equipe equipeacadastrorecente ON equipeacadastrorecente.co_seq_dim_equipe = tfcirecente.co_dim_equipe
             LEFT JOIN tb_fat_cidadao_territorio cidadaoterritoriorecente ON cidadaoterritoriorecente.co_fat_cad_individual = tfcirecente.co_seq_fat_cad_individual
             LEFT JOIN tb_dim_unidade_saude unidadeatendimentorecente ON unidadeatendimentorecente.co_seq_dim_unidade_saude = tfairecente.co_dim_unidade_saude_1
             LEFT JOIN tb_dim_unidade_saude unidadecadastrorecente ON unidadecadastrorecente.co_seq_dim_unidade_saude = tfcirecente.co_dim_unidade_saude
             LEFT JOIN tb_dim_profissional acsvisitarecente ON acsvisitarecente.co_seq_dim_profissional = tfvdrecente.co_dim_profissional
             LEFT JOIN tb_dim_profissional acscadastrorecente ON acscadastrorecente.co_seq_dim_profissional = tfcirecente.co_dim_profissional
             LEFT JOIN tb_dim_tempo acstempovisitarecente ON tfvdrecente.co_dim_tempo = acstempovisitarecente.co_seq_dim_tempo
          ORDER BY tfcp.no_cidadao) v1
  WHERE v1.possui_hipertensao_diagnosticada OR v1.possui_hipertensao_autoreferida