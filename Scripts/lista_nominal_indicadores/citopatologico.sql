CREATE OR REPLACE VIEW esus_355350_tapirai_sp_20221214."_lista_citopatologico"
AS WITH relatorio AS (
         SELECT tfcp.no_cidadao AS nome_paciente,
            tfcp.nu_cpf_cidadao AS cidadao_cpf,
            tfcp.nu_cns AS cidadao_cns,
            tfci.co_seq_fat_cad_individual AS id_cadastro_individual,
            tds.ds_sexo AS paciente_sexo,
            tfpap.dt_nascimento AS paciente_data_nascimento,
            (CURRENT_DATE - tfpap.dt_nascimento) / 365 AS paciente_idade,
            tdt.dt_registro AS data_exame,
            max(tdt.dt_registro) OVER (PARTITION BY tfcp.no_cidadao, tfpap.dt_nascimento, tdt.dt_registro) AS data_ultimo_exame,
            max(tdt.dt_registro) OVER (PARTITION BY tfcp.no_cidadao, tfpap.dt_nascimento, tdt.dt_registro) + 1095 AS data_proximo_exame,
            (max(tdt.dt_registro) OVER (PARTITION BY tfcp.no_cidadao, tfpap.dt_nascimento, tdt.dt_registro) + 1095 - tfpap.dt_nascimento) / 365 AS idade_proximo_exame,
            tdprof.no_profissional AS profissional_nome,
            tdprof.nu_cns AS profissional_cns,
            tdcbo.no_cbo AS profissional_cbo,
            tde.nu_ine AS equipe_ine,
            tde.no_equipe AS equipe_nome,
            tdus.nu_cnes AS estabelecimento_cnes,
            tdus.no_unidade_saude AS estabelecimento_nome
           FROM esus_355350_tapirai_sp_20221214.tb_fat_proced_atend_proced tfpap
             JOIN esus_355350_tapirai_sp_20221214.tb_dim_procedimento tdp ON tfpap.co_dim_procedimento = tdp.co_seq_dim_procedimento
             JOIN esus_355350_tapirai_sp_20221214.tb_fat_cidadao_pec tfcp ON tfpap.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec
             JOIN esus_355350_tapirai_sp_20221214.tb_dim_tempo tdt ON tfpap.co_dim_tempo = tdt.co_seq_dim_tempo
             JOIN esus_355350_tapirai_sp_20221214.tb_dim_sexo tds ON tds.co_seq_dim_sexo = tfcp.co_dim_sexo
             JOIN esus_355350_tapirai_sp_20221214.tb_dim_profissional tdprof ON tfpap.co_dim_profissional = tdprof.co_seq_dim_profissional
             JOIN esus_355350_tapirai_sp_20221214.tb_dim_equipe tde ON tfpap.co_dim_equipe = tde.co_seq_dim_equipe
             JOIN esus_355350_tapirai_sp_20221214.tb_dim_cbo tdcbo ON tfpap.co_dim_cbo = tdcbo.co_seq_dim_cbo
             JOIN esus_355350_tapirai_sp_20221214.tb_dim_unidade_saude tdus ON tfpap.co_dim_unidade_saude = tdus.co_seq_dim_unidade_saude
             LEFT JOIN esus_355350_tapirai_sp_20221214.tb_fat_cad_individual tfci ON tfpap.co_fat_cidadao_pec = tfci.co_fat_cidadao_pec
          WHERE (tdp.co_proced::text = ANY (ARRAY['0201020033'::character varying::text, 'ABPG010'::character varying::text])) AND (tdcbo.nu_cbo::text ~~ ANY (ARRAY['%2235%'::text, '%2251%'::text, '%2252%'::text, '%2253%'::text, '%2231%'::text])) AND tfcp.st_faleceu <> 1
        )
 SELECT tb.nome_paciente,
    tb.cidadao_cpf,
    tb.cidadao_cns,
    tb.id_cadastro_individual,
    tb.paciente_sexo,
    tb.paciente_data_nascimento,
    tb.paciente_idade,
    tb.data_ultimo_exame,
        CASE
            WHEN (CURRENT_DATE - tb.data_ultimo_exame) <= 1095 THEN true
            ELSE false
        END AS realizou_exame_ultimos_36_meses,
        CASE
            WHEN date_part('month'::text, tb.data_exame) >= 1::double precision AND date_part('month'::text, tb.data_exame) <= 4::double precision THEN concat(date_part('year'::text, tb.data_exame), '.Q1')
            WHEN date_part('month'::text, tb.data_exame) >= 5::double precision AND date_part('month'::text, tb.data_exame) <= 8::double precision THEN concat(date_part('year'::text, tb.data_exame), '.Q2')
            ELSE concat(date_part('year'::text, tb.data_exame), '.Q3')
        END AS quadrimestre_realizou_exame,
    tb.profissional_nome,
    tb.profissional_cns,
    tb.profissional_cbo,
    tb.equipe_ine,
    tb.estabelecimento_cnes,
    tb.estabelecimento_nome,
    tb.data_proximo_exame,
    tb.idade_proximo_exame,
        CASE
            WHEN date_part('month'::text, tb.data_proximo_exame) >= 1::double precision AND date_part('month'::text, tb.data_proximo_exame) <= 4::double precision THEN concat(date_part('year'::text, tb.data_proximo_exame), '.Q1')
            WHEN date_part('month'::text, tb.data_proximo_exame) >= 5::double precision AND date_part('month'::text, tb.data_proximo_exame) <= 8::double precision THEN concat(date_part('year'::text, tb.data_proximo_exame), '.Q2')
            ELSE concat(date_part('year'::text, tb.data_proximo_exame), '.Q3')
        END AS quadrimestre_proximo_exame
   FROM relatorio tb
  WHERE tb.idade_proximo_exame >= 25 AND tb.idade_proximo_exame <= 64 OR tb.paciente_idade >= 25 AND tb.paciente_idade <= 64;
