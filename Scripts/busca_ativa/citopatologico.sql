select 
tdt.dt_registro as data_exame,
CASE
    WHEN extract(month from tdt.dt_registro) between 1 and 4 THEN concat(extract(year from tdt.dt_registro),'.Q1')
    WHEN extract(month from tdt.dt_registro) between 5 and 8 THEN concat(extract(year from tdt.dt_registro),'.Q2')
    ELSE concat(extract(year from tdt.dt_registro),'.Q3')
END AS quadrimestre,
tdcbo.nu_cbo as numero_cbo,
tdcbo.no_cbo as cbo,
tde.nu_ine as ine,
tde.no_equipe as equipe,
tdprof.no_profissional as profissional,
tfci.co_seq_fat_cad_individual as id_cadastro_individual,
tfcp.nu_cpf_cidadao as cidadao_cpf,
tfcp.nu_cns as cidadao_cns,
tfcp.no_cidadao as cidadao
from tb_fat_proced_atend_proced tfpap
inner join tb_dim_procedimento tdp on tfpap.co_dim_procedimento = tdp.co_seq_dim_procedimento 
inner join tb_fat_cidadao_pec tfcp on tfpap.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec 
inner join tb_dim_tempo tdt on tfpap.co_dim_tempo = tdt.co_seq_dim_tempo
inner join tb_dim_profissional tdprof on tfpap.co_dim_profissional = tdprof.co_seq_dim_profissional
inner join tb_dim_equipe tde on tfpap.co_dim_equipe = tde.co_seq_dim_equipe
inner join tb_dim_cbo tdcbo on tfpap.co_dim_cbo = tdcbo.co_seq_dim_cbo 
left join tb_fat_cad_individual tfci on tfpap.co_fat_cidadao_pec = tfci.co_fat_cidadao_pec
where tdp.co_proced in ('0201020033','ABPG010');