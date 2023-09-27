SELECT COUNT(DISTINCT usuario_cns_criptografado)
FROM (
    SELECT usuario_cns_criptografado, realizacao_periodo_data_inicio
    FROM dados_publicos.siasus_raas_psicossocial_disseminacao
    WHERE quantidade_apresentada > 0 AND unidade_geografica_id_sus = '280030'
    UNION
    SELECT usuario_cns_criptografado, realizacao_periodo_data_inicio
    FROM dados_publicos.siasus_bpa_i_disseminacao
    WHERE quantidade_apresentada > 0 
        AND (
            estabelecimento_tipo_id_sigtap = '70'
            OR (
                estabelecimento_id_cnes = '0002186'
                AND profissional_ocupacao_id_cbo IN ('251510', '225133')
                )
            )
        )
) foo
WHERE 
    realizacao_periodo_data_inicio > '2020-12-31'::date;