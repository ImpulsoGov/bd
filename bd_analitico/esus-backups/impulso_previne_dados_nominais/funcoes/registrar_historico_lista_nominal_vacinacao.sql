CREATE OR REPLACE PROCEDURE impulso_previne_dados_nominais.registrar_historico_lista_nominal_vacinacao()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    r record;
BEGIN
    FOR r IN (
        SELECT table_schema 
        FROM configuracoes.municipios_transmissoes_ativas
        WHERE table_name = 'lista_nominal_vacinacao'
    ) 
    LOOP
        BEGIN
            EXECUTE format(
                'INSERT INTO impulso_previne_dados_nominais.lista_nominal_vacinacao_historico (municipio_id_sus, chave_cidadao, cidadao_nome, cidadao_cpf, cidadao_cns, cidadao_sexo, dt_nascimento, cidadao_nome_responsavel, cidadao_cns_responsavel, cidadao_cpf_responsavel, cidadao_idade_meses_atual, cidadao_idade_meses_inicio_quadri, cidadao_idade_meses_fim_quadri, se_faleceu, co_seq_fat_vacinacao, co_seq_fat_vacinacao_vacina, tipo_ficha, codigo_vacina, nome_vacina, dose_vacina, data_registro_vacina, estabelecimento_cnes_aplicacao_vacina, estabelecimento_nome_aplicacao_vacina, equipe_ine_aplicacao_vacina, equipe_nome_aplicacao_vacina, profissional_nome_aplicacao_vacina, data_ultimo_cadastro_individual, estabelecimento_cnes_cadastro, estabelecimento_nome_cadastro, equipe_ine_cadastro, equipe_nome_cadastro, acs_nome_cadastro, estabelecimento_cnes_atendimento, estabelecimento_nome_atendimento, equipe_ine_atendimento, equipe_nome_atendimento, data_ultimo_atendimento_individual, data_ultima_vista_domiciliar, acs_nome_visita, criacao_data, atualizacao_data)
                (
                    with municipios_ultimas_transmissoes as (
                        SELECT 	
                            tb2.municipio_id_sus,
                            max(tb2.criacao_data) as ultima_transmissao
                        FROM impulso_previne_dados_nominais.lista_nominal_vacinacao_historico tb2 
                        GROUP BY tb2.municipio_id_sus
                    )	
                    SELECT tb1.municipio_id_sus, chave_cidadao, cidadao_nome, cidadao_cpf, cidadao_cns, cidadao_sexo, dt_nascimento, cidadao_nome_responsavel, cidadao_cns_responsavel, cidadao_cpf_responsavel, cidadao_idade_meses_atual, cidadao_idade_meses_inicio_quadri, cidadao_idade_meses_fim_quadri, se_faleceu, co_seq_fat_vacinacao, co_seq_fat_vacinacao_vacina, tipo_ficha, codigo_vacina, nome_vacina, dose_vacina, data_registro_vacina, estabelecimento_cnes_aplicacao_vacina, estabelecimento_nome_aplicacao_vacina, equipe_ine_aplicacao_vacina, equipe_nome_aplicacao_vacina, profissional_nome_aplicacao_vacina, data_ultimo_cadastro_individual, estabelecimento_cnes_cadastro, estabelecimento_nome_cadastro, equipe_ine_cadastro, equipe_nome_cadastro, acs_nome_cadastro, estabelecimento_cnes_atendimento, estabelecimento_nome_atendimento, equipe_ine_atendimento, equipe_nome_atendimento, data_ultimo_atendimento_individual, data_ultima_vista_domiciliar, acs_nome_visita, criacao_data, atualizacao_data 
                    FROM %s.lista_nominal_vacinacao tb1
                    LEFT JOIN municipios_ultimas_transmissoes tb2 ON tb1.municipio_id_sus = tb2.municipio_id_sus
                    WHERE tb1.criacao_data::date > ( CASE WHEN tb2.ultima_transmissao::date IS NULL THEN current_date - ''1 day ''::interval ELSE tb2.ultima_transmissao::date END)
                )',r.table_schema);
        END;
    END loop;
END;
$procedure$
;
