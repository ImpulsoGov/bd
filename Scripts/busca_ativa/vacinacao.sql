SELECT * FROM(
    SELECT
    CASE
        WHEN extract(month from tdt.dt_registro + interval '1 year') between 1 and 4 THEN concat(extract(year from tdt.dt_registro + interval '1 year'),'.Q1')
        WHEN extract(month from tdt.dt_registro + interval '1 year') between 5 and 8 THEN concat(extract(year from tdt.dt_registro + interval '1 year'),'.Q2')
        ELSE concat(extract(year from tdt.dt_registro + interval '1 year'),'.Q3')
    END AS quadrimestre,
    extract(year from tdt.dt_registro + interval '1 year') AS quadrimestre_ano,
    tfcp.nu_cns, 
    tfcp.nu_cpf_cidadao, 
    tfcp.no_cidadao, 
    tdt.dt_registro as data_nascimento,
    (tdt.dt_registro + interval '1 year')::date as data_aniversario_1_ano,
    (
    SELECT count(*) FROM tb_fat_vacinacao vacinacao
    INNER JOIN tb_fat_vacinacao_vacina vacina on vacina.co_fat_vacinacao = vacinacao.co_seq_fat_vacinacao
    INNER JOIN tb_dim_imunobiologico imunobiologico on vacina.co_dim_imunobiologico = imunobiologico.co_seq_dim_imunobiologico
    INNER JOIN tb_dim_dose_imunobiologico dose on vacina.co_dim_dose_imunobiologico = dose.co_seq_dim_dose_imunobiologico
    WHERE vacinacao.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec and imunobiologico.nu_identificador = '22' and dose.nu_identificador = '3'
    ) > 0 as possui_terceira_dose_poliomelite_inativada,
    (
    SELECT count(*) FROM tb_fat_vacinacao vacinacao
    INNER JOIN tb_fat_vacinacao_vacina vacina on vacina.co_fat_vacinacao = vacinacao.co_seq_fat_vacinacao
    INNER JOIN tb_dim_imunobiologico imunobiologico on vacina.co_dim_imunobiologico = imunobiologico.co_seq_dim_imunobiologico
    INNER JOIN tb_dim_dose_imunobiologico dose on vacina.co_dim_dose_imunobiologico = dose.co_seq_dim_dose_imunobiologico
    WHERE vacinacao.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec and imunobiologico.nu_identificador = '42' and dose.nu_identificador = '3'
    ) > 0 as possui_terceira_dose_pentavalente_celular,
    (
    SELECT count(*) FROM tb_fat_vacinacao vacinacao
    INNER JOIN tb_fat_vacinacao_vacina vacina on vacina.co_fat_vacinacao = vacinacao.co_seq_fat_vacinacao
    INNER JOIN tb_dim_imunobiologico imunobiologico on vacina.co_dim_imunobiologico = imunobiologico.co_seq_dim_imunobiologico
    INNER JOIN tb_dim_dose_imunobiologico dose on vacina.co_dim_dose_imunobiologico = dose.co_seq_dim_dose_imunobiologico
    WHERE vacinacao.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec and imunobiologico.nu_identificador = '29' and dose.nu_identificador = '3'
    ) > 0 as possui_terceira_dose_pentavalente_acelular,
    (
    SELECT count(*) FROM tb_fat_vacinacao vacinacao
    INNER JOIN tb_fat_vacinacao_vacina vacina on vacina.co_fat_vacinacao = vacinacao.co_seq_fat_vacinacao
    INNER JOIN tb_dim_imunobiologico imunobiologico on vacina.co_dim_imunobiologico = imunobiologico.co_seq_dim_imunobiologico
    INNER JOIN tb_dim_dose_imunobiologico dose on vacina.co_dim_dose_imunobiologico = dose.co_seq_dim_dose_imunobiologico
    WHERE vacinacao.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec and imunobiologico.nu_identificador = '43' and dose.nu_identificador = '3'
    ) > 0 as possui_terceira_dose_hexavalente,
    (
    SELECT count(*) FROM tb_fat_vacinacao vacinacao
    INNER JOIN tb_fat_vacinacao_vacina vacina on vacina.co_fat_vacinacao = vacinacao.co_seq_fat_vacinacao
    INNER JOIN tb_dim_imunobiologico imunobiologico on vacina.co_dim_imunobiologico = imunobiologico.co_seq_dim_imunobiologico
    INNER JOIN tb_dim_dose_imunobiologico dose on vacina.co_dim_dose_imunobiologico = dose.co_seq_dim_dose_imunobiologico
    WHERE vacinacao.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec and imunobiologico.nu_identificador = '9' and dose.nu_identificador = '3'
    ) > 0 as possui_terceira_dose_hepatite_b
    FROM tb_fat_cidadao_pec tfcp
    INNER JOIN tb_dim_tempo tdt on tfcp.co_dim_tempo_nascimento = tdt.co_seq_dim_tempo
) v1 WHERE v1.quadrimestre_ano >= 2020;

