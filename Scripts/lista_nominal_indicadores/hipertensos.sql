SELECT 
    CASE
        WHEN extract(month from v1.dt_afericao_pressao_mais_recente) between 1 and 4 THEN concat(extract(year from v1.dt_afericao_pressao_mais_recente),'.Q1')
        WHEN extract(month from v1.dt_afericao_pressao_mais_recente) between 5 and 8 THEN concat(extract(year from v1.dt_afericao_pressao_mais_recente),'.Q2')
        WHEN extract(month from v1.dt_afericao_pressao_mais_recente) between 9 and 12 THEN concat(extract(year from v1.dt_afericao_pressao_mais_recente),'.Q3')
    END AS quadrimestre,
    *
FROM(
    SELECT
        (
            SELECT tempo.dt_registro FROM tb_fat_proced_atend_proced fichaproced
            INNER JOIN tb_dim_procedimento proced ON fichaproced.co_dim_procedimento = proced.co_seq_dim_procedimento
            INNER JOIN tb_dim_tempo tempo ON fichaproced.co_dim_tempo = tempo.co_seq_dim_tempo
            INNER JOIN tb_dim_cbo cbo ON fichaproced.co_dim_cbo = cbo.co_seq_dim_cbo AND cbo.nu_cbo like any (array['2251%','2252%','2253%','2231%'])
            WHERE co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec
            AND proced.co_proced IN ('0301100039','ABPG033')
            ORDER BY fichaproced.co_dim_tempo DESC LIMIT 1
        ) as dt_afericao_pressao_mais_recente,
        tfcp.nu_cpf_cidadao,
        tfcp.nu_cns,
        tfcp.no_cidadao,
        tdt.dt_registro as dt_nascimento,
        (
            SELECT count(*) FROM tb_fat_cad_individual cadastro WHERE co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec
            AND st_hipertensao_arterial = 1
        ) > 0 AS possui_hipertensao_autoreferida,
        (
            SELECT count(*) FROM tb_fat_atendimento_individual atendimento
            INNER JOIN tb_fat_atd_ind_problemas problemas ON atendimento.co_seq_fat_atd_ind = problemas.co_fat_atd_ind
            INNER JOIN tb_dim_cbo cbo ON problemas.co_dim_cbo_1 = cbo.co_seq_dim_cbo AND cbo.nu_cbo like any (array['2251%','2252%','2253%','2231%'])
            LEFT JOIN tb_dim_ciap ciap ON problemas.co_dim_ciap = ciap.co_seq_dim_ciap AND ciap.nu_ciap IN ('K86','K87')
            LEFT JOIN tb_dim_cid cid ON problemas.co_dim_cid = cid.co_seq_dim_cid 
            AND cid.nu_cid IN ('I10','I11','I110','I119','I12','I120','I129','I13','I130','I131','I132','I139',
            'I15','I150','I151','I152','I158','I159','O10','O100','O101','O102','O103','O104','O109','O11')
            WHERE atendimento.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec
            
        ) > 0 as possui_hipertensao_diagnosticada
    FROM tb_fat_cidadao_pec tfcp
    INNER JOIN tb_dim_tempo tdt on tfcp.co_dim_tempo_nascimento = tdt.co_seq_dim_tempo
    ORDER BY tfcp.no_cidadao
) v1;

