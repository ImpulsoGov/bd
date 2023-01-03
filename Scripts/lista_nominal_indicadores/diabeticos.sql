SELECT 
	CASE
        WHEN extract(quarter from current_date) = 1 THEN concat(extract(year from current_date),'.Q1')
        WHEN extract(quarter from current_date) = 2 THEN concat(extract(year from current_date),'.Q2')
        WHEN extract(quarter from current_date) = 3 THEN concat(extract(year from current_date),'.Q3')
    END AS quadrimestre_atual,
    case 
    	when v1.dt_solicitacao_hemoglobina_glicada_mais_recente between (CASE
        WHEN extract(quarter from current_date) = 1 THEN concat(extract(year from current_date),'-04-30')
        WHEN extract(quarter from current_date) = 2 THEN concat(extract(year from current_date),'-08-31')
        WHEN extract(quarter from current_date) = 3 THEN concat(extract(year from current_date),'-12-31')
    	end)::date and ((CASE
        WHEN extract(quarter from current_date) = 1 THEN concat(extract(year from current_date),'-04-30')
        WHEN extract(quarter from current_date) = 2 THEN concat(extract(year from current_date),'-08-31')
        WHEN extract(quarter from current_date) = 3 THEN concat(extract(year from current_date),'-12-31')
    	end)::date -'180 days'::interval)
    	then true
    	else false
    end as realizou_afericao_ultimos_6_meses, 
    case 
    	when v1.dt_consulta_mais_recente between (CASE
        WHEN extract(quarter from current_date) = 1 THEN concat(extract(year from current_date),'-04-30')
        WHEN extract(quarter from current_date) = 2 THEN concat(extract(year from current_date),'-08-31')
        WHEN extract(quarter from current_date) = 3 THEN concat(extract(year from current_date),'-12-31')
    	end)::date and ((CASE
        WHEN extract(quarter from current_date) = 1 THEN concat(extract(year from current_date),'-04-30')
        WHEN extract(quarter from current_date) = 2 THEN concat(extract(year from current_date),'-08-31')
        WHEN extract(quarter from current_date) = 3 THEN concat(extract(year from current_date),'-12-31')
    	end)::date -'180 days'::interval)
    	then true
    	else false
    end as realizou_consulta_ultimos_6_meses,*
from (
    select
    	/* Retorna data da aferição de pressão mais recente */
        (
            SELECT tempo.dt_registro FROM tb_fat_proced_atend_proced fichaproced
            INNER JOIN tb_dim_procedimento proced ON fichaproced.co_dim_procedimento = proced.co_seq_dim_procedimento
            INNER JOIN tb_dim_tempo tempo ON fichaproced.co_dim_tempo = tempo.co_seq_dim_tempo
            INNER JOIN tb_dim_cbo cbo ON fichaproced.co_dim_cbo = cbo.co_seq_dim_cbo AND cbo.nu_cbo like any (array['2251%','2252%','2253%','2231%','2235%'])
            WHERE co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec
            AND proced.co_proced IN ('0202010503','ABEX008')
            ORDER BY fichaproced.co_dim_tempo DESC LIMIT 1
        ) as dt_solicitacao_hemoglobina_glicada_mais_recente,
        (
            SELECT max(tempo.dt_registro) FROM tb_fat_atendimento_individual atendimento
            INNER JOIN tb_dim_tempo tempo ON atendimento.co_dim_tempo = tempo.co_seq_dim_tempo
            INNER JOIN tb_fat_atd_ind_problemas problemas ON atendimento.co_seq_fat_atd_ind = problemas.co_fat_atd_ind
            INNER JOIN tb_dim_cbo cbo ON problemas.co_dim_cbo_1 = cbo.co_seq_dim_cbo AND cbo.nu_cbo like any (array['2251%','2252%','2253%','2231%','2235%'])
            LEFT JOIN tb_dim_ciap ciap ON problemas.co_dim_ciap = ciap.co_seq_dim_ciap AND ciap.nu_ciap IN ('T89','T90')
            LEFT JOIN tb_dim_cid cid ON problemas.co_dim_cid = cid.co_seq_dim_cid 
            AND cid.nu_cid IN ('E10','E100','E101','E102','E103','E104','E105','E106','E107','E108','E109','E11','E110','E111','E112','E113','E114','E115','E116','E117', 
            'E118','E119','E12','E120','E121','E122','E123','E124','E125','E126','E127','E128','E129','E13','E130','E131','E132','E133','E134','E135','E136','E137','E138', 
            'E139','E14','E140','E141','E142','E143','E144','E145','E146','E147','E148','E149','O240','O241','O242','O243')
            WHERE atendimento.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec
        ) as dt_consulta_mais_recente,
        /* CPF do usuário */
        tfcp.nu_cpf_cidadao as cidadao_cpf,
        /* Número CNS do usuário */
        tfcp.nu_cns as cidadao_cns,
        /* Nome do usuário */
        tfcp.no_cidadao as cidadao_nome,
        /* Sexo do usuário */
        tds.ds_sexo as cidadao_sexo,
        /* Data de nascimento do usuário */
        tdt.dt_registro as dt_nascimento,
        /* Retorna código da unidade na Ficha de atendimento individual recente (tb_fat_atendimento_individual) */
        NULLIF(unidadeatendimentorecente.nu_cnes, '-') AS estabelecimento_cnes_atendimento,
        /* Retorna código da unidade na Ficha de cadastro individual recente (tb_fat_cad_individual) */
        NULLIF(unidadecadastrorecente.nu_cnes, '-') AS estabelecimento_cnes_cadastro,
        /* Retorna nome da unidade na Ficha de atendimento individual recente (tb_fat_atendimento_individual) */
        NULLIF(unidadeatendimentorecente.no_unidade_saude, 'Não informado') AS estabelecimento_nome_atendimento,
        /* Retorna nome da unidade na Ficha de cadastro individual recente (tb_fat_cad_individual) */
        COALESCE(NULLIF(unidadecadastrorecente.no_unidade_saude, 'Não informado'), unidadeatendimentorecente.no_unidade_saude) AS estabelecimento_nome_cadastro,
        /* Retorna código da equipe na Ficha de atendimento individual recente (tb_fat_atendimento_individual) */
        NULLIF(equipeatendimentorecente.nu_ine, '-') AS equipe_ine_atendimento,
        /* Retorna código da equipe na Ficha de cadastro individual recente (tb_fat_cad_individual) */
        NULLIF(equipeacadastrorecente.nu_ine, '-') AS equipe_ine_cadastro,
        /* Retorna nome da equipe na Ficha de atendimento individual recente (tb_fat_atendimento_individual) */
        NULLIF(equipeatendimentorecente.no_equipe, 'SEM EQUIPE') AS equipe_nome_atendimento,
        /* Retorna nome da equipe na Ficha de atendimento individual recente (tb_fat_atendimento_individual) */
        NULLIF(equipeatendimentorecente.no_equipe, 'SEM EQUIPE') AS equipe_nome_cadastro,
        /* Retorna nome do ACS na Ficha de cadastro individual recente (tb_fat_cad_individual) */
        NULLIF(acscadastrorecente.no_profissional, 'SEM EQUIPE') AS acs_nome_cadastro,
        /* Retorna nome do ACS na  Ficha de Visita Domiciliar (tb_fat_visita_domiciliar) */
        NULLIF(acsvisitarecente.no_profissional, 'SEM EQUIPE') AS acs_nome_visita,
        /* Indica True se há registro de hipertensão autorreferida na Ficha de Cadastro Individual e False se não*/
        (
            SELECT count(*) FROM tb_fat_cad_individual cadastro WHERE co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec
            AND st_diabete  = 1
        ) > 0 AS possui_diabetes_autoreferida,
        /* Indica True se há registro de hipertensão diagnostica na Ficha de Atendimento Individual e False se não*/
        (
            SELECT count(*) FROM tb_fat_atendimento_individual atendimento
            INNER JOIN tb_fat_atd_ind_problemas problemas ON atendimento.co_seq_fat_atd_ind = problemas.co_fat_atd_ind
            INNER JOIN tb_dim_cbo cbo ON problemas.co_dim_cbo_1 = cbo.co_seq_dim_cbo AND cbo.nu_cbo like any (array['2251%','2252%','2253%','2231%','2235%'])
            LEFT JOIN tb_dim_ciap ciap ON problemas.co_dim_ciap = ciap.co_seq_dim_ciap AND ciap.nu_ciap IN ('T89','T90')
            LEFT JOIN tb_dim_cid cid ON problemas.co_dim_cid = cid.co_seq_dim_cid 
            AND cid.nu_cid IN ('E10','E100','E101','E102','E103','E104','E105','E106','E107','E108','E109','E11','E110','E111','E112','E113','E114','E115','E116','E117', 
            'E118','E119','E12','E120','E121','E122','E123','E124','E125','E126','E127','E128','E129','E13','E130','E131','E132','E133','E134','E135','E136','E137','E138', 
            'E139','E14','E140','E141','E142','E143','E144','E145','E146','E147','E148','E149','O240','O241','O242','O243')
            WHERE atendimento.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec
        ) > 0 as possui_diabetes_diagnosticada
    FROM tb_fat_cidadao_pec tfcp
    INNER JOIN tb_dim_tempo tdt on tfcp.co_dim_tempo_nascimento = tdt.co_seq_dim_tempo
    INNER JOIN tb_dim_sexo tds on tfcp.co_dim_sexo  = tds.co_seq_dim_sexo  
    LEFT JOIN tb_fat_cad_individual tfcirecente ON tfcirecente.co_seq_fat_cad_individual = (( SELECT cadastroindividual.co_seq_fat_cad_individual
                                   FROM tb_fat_cad_individual cadastroindividual
                                  WHERE (cadastroindividual.co_fat_cidadao_pec IN ( 
                                      SELECT tfccadastro.co_seq_fat_cidadao_pec
                                      FROM tb_fat_cidadao_pec tfccadastro
                                      WHERE tfccadastro.no_cidadao = tfcp.no_cidadao AND tfccadastro.co_dim_tempo_nascimento = tfcp.co_dim_tempo_nascimento
                                  ))
                                  ORDER BY cadastroindividual.co_dim_tempo DESC LIMIT 1))
 	LEFT JOIN tb_fat_atendimento_individual tfairecente ON tfairecente.co_seq_fat_atd_ind = (( SELECT atendimentoindividual.co_seq_fat_atd_ind
                                   FROM tb_fat_atendimento_individual atendimentoindividual
                                  WHERE (atendimentoindividual.co_fat_cidadao_pec IN ( 
                                        SELECT tfcatendimento.co_seq_fat_cidadao_pec
                                        FROM tb_fat_cidadao_pec tfcatendimento
                                        WHERE tfcatendimento.no_cidadao = tfcp.no_cidadao AND tfcatendimento.co_dim_tempo_nascimento = tfcp.co_dim_tempo_nascimento
                                  ))
                                  ORDER BY atendimentoindividual.co_dim_tempo DESC LIMIT 1))
  	LEFT JOIN tb_fat_visita_domiciliar tfvdrecente ON tfvdrecente.co_seq_fat_visita_domiciliar = (( SELECT visitadomiciliar.co_seq_fat_visita_domiciliar
                                   FROM tb_fat_visita_domiciliar visitadomiciliar
                                  WHERE (visitadomiciliar.co_fat_cidadao_pec IN ( 
                                    SELECT tfcvisita.co_seq_fat_cidadao_pec
                                    FROM tb_fat_cidadao_pec tfcvisita
                                    WHERE tfcvisita.no_cidadao = tfcp.no_cidadao AND tfcvisita.co_dim_tempo_nascimento = tfcp.co_dim_tempo_nascimento
                                  ))
                                  ORDER BY visitadomiciliar.co_dim_tempo DESC LIMIT 1))
	LEFT JOIN tb_dim_equipe equipeatendimentorecente ON equipeatendimentorecente.co_seq_dim_equipe = tfairecente.co_dim_equipe_1
	LEFT JOIN tb_dim_equipe equipeacadastrorecente ON equipeacadastrorecente.co_seq_dim_equipe = tfcirecente.co_dim_equipe
	LEFT JOIN tb_dim_unidade_saude unidadeatendimentorecente ON unidadeatendimentorecente.co_seq_dim_unidade_saude = tfairecente.co_dim_unidade_saude_1
	LEFT JOIN tb_dim_unidade_saude unidadecadastrorecente ON unidadecadastrorecente.co_seq_dim_unidade_saude = tfcirecente.co_dim_unidade_saude
	LEFT JOIN tb_dim_profissional acsvisitarecente ON acsvisitarecente.co_seq_dim_profissional = tfvdrecente.co_dim_profissional
	LEFT JOIN tb_dim_profissional acscadastrorecente ON acscadastrorecente.co_seq_dim_profissional = tfcirecente.co_dim_profissional
	LEFT JOIN tb_dim_tempo acstempovisitarecente ON tfvdrecente.co_dim_tempo = acstempovisitarecente.co_seq_dim_tempo					
    ORDER BY tfcp.no_cidadao
) v1
where (v1.possui_diabetes_diagnosticada is true or v1.possui_diabetes_autoreferida is true)

