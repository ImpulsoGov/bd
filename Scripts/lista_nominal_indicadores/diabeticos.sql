SELECT 
    CASE
        WHEN extract(month from v1.dt_solicitacao_hemoglobina_glicada_mais_recente) between 1 and 4 THEN concat(extract(year from v1.dt_solicitacao_hemoglobina_glicada_mais_recente),'.Q1')
        WHEN extract(month from v1.dt_solicitacao_hemoglobina_glicada_mais_recente) between 5 and 8 THEN concat(extract(year from v1.dt_solicitacao_hemoglobina_glicada_mais_recente),'.Q2')
        WHEN extract(month from v1.dt_solicitacao_hemoglobina_glicada_mais_recente) between 9 and 12 THEN concat(extract(year from v1.dt_solicitacao_hemoglobina_glicada_mais_recente),'.Q3')
    END AS quadrimestre,
    *
FROM(
    SELECT
        (
            SELECT tempo.dt_registro FROM tb_fat_atd_ind_procedimentos fichaproced
            INNER JOIN tb_dim_procedimento proced ON fichaproced.co_dim_procedimento_solicitado = proced.co_seq_dim_procedimento
            INNER JOIN tb_dim_tempo tempo ON fichaproced.co_dim_tempo = tempo.co_seq_dim_tempo
            INNER JOIN tb_dim_cbo cbo ON fichaproced.co_dim_cbo_1 = cbo.co_seq_dim_cbo AND cbo.nu_cbo like any (array['2251%','2252%','2253%','2231%','2235%'])
            WHERE co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec
            AND proced.co_proced IN ('0202010503','ABEX008')
            ORDER BY fichaproced.co_dim_tempo DESC LIMIT 1
        ) as dt_solicitacao_hemoglobina_glicada_mais_recente,
        tfcp.nu_cpf_cidadao,
        tfcp.nu_cns,
        tfcp.no_cidadao,
        tdt.dt_registro as dt_nascimento,
        (
            SELECT count(*) FROM tb_fat_cad_individual cadastro WHERE co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec
            AND st_diabete = 1
        ) > 0 AS possui_diabetes_autoreferida,
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
    ORDER BY tfcp.no_cidadao
) v1;

