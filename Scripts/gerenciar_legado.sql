-- Renomear tabela

SET ROLE engenheiras;
CREATE VIEW dados_publicos.cnes_equipes_min AS (
    SELECT *
    FROM dados_publicos.scnes_equipes_min
);

GRANT SELECT, INSERT, TRIGGER
ON TABLE dados_publicos.cnes_equipes_min
TO etl;
GRANT SELECT
ON TABLE dados_publicos.cnes_equipes_min
TO
    analistas,
    saude_mental_dbt,
    saude_mental_sms_aracaju,
    agp_aplicacoes,
    agp_admin,
    territorios_saudaveis_analistas
;

--select * from pg_class where relname = 'scnes_equipes_min'


-- Renomear coluna
SELECT *
--    table_schema,
--    table_name,
--    column_name,
--    column_default
FROM information_schema.COLUMNS
WHERE
    column_name = 'id'
AND column_default IS NULL
ORDER BY table_schema, table_name, column_name;
;
 WHERE table_schema = 'dados_publicos'
   AND table_name   = 'scnes_vinculos_disseminacao';


CREATE OR REPLACE PROCEDURE extensoes.renomear_coluna(
    schema_nome text,
    tabela_nome text,
    coluna_nome_antigo text,
    coluna_nome_novo text
)
LANGUAGE plpgsql
AS $procedure$
    DECLARE
        coluna_tipo varchar;
    BEGIN
        coluna_tipo := (
            SELECT data_type
            FROM information_schema.COLUMNS
            WHERE
                table_schema = schema_nome
            AND table_name = tabela_nome
            AND column_name = coluna_nome_antigo
        );
        EXECUTE
            'ALTER TABLE '
            || quote_ident(schema_nome)
            || '.'
            || quote_ident(tabela_nome)
            || ' RENAME COLUMN '
            || quote_ident(coluna_nome_antigo)
            || ' TO '
            || quote_ident(coluna_nome_novo)
            || ';'
        ;
        EXECUTE
            'ALTER TABLE '
            || quote_ident(schema_nome)
            || '.'
            || quote_ident(tabela_nome)
            || ' ADD COLUMN '
            || quote_ident(coluna_nome_antigo)
            || ' '
            || quote_ident(coluna_tipo)
            || ' GENERATED ALWAYS AS ('
            || quote_ident(coluna_nome_novo)
            || ') STORED;'
        ;
    END;
$procedure$;
COMMENT ON PROCEDURE extensoes.renomear_coluna IS '
Renomeia uma coluna de uma tabela específica de maneira segura
em relação a objetos cuja definição dependa do nome antigo da coluna.
Para isso, uma coluna virtual com o nome antigo da coluna renomeada
é criada, apresentando exatamente os mesmos dados existentes na coluna
original e sem ocupar espaço extra em memória.
';


DROP VIEW dados_publicos.sihsus_aih_reduzida_disseminacao;
CREATE OR REPLACE VIEW dados_publicos.sihsus_aih_reduzida_disseminacao AS (
SELECT
    id,
    unidade_geografica_id,
    periodo_id,
    gestao_unidade_geografica_id_sus,
    leito_especialidade_id_sigtap,
    estabelecimento_id_cnpj
        AS estabelecimento_cnpj,
    aih_id_sihsus,
    usuario_residencia_cep,
    usuario_residencia_municipio_id_sus,
    usuario_nascimento_data
        AS usuario_data_nascimento,
    usuario_sexo_id_sigtap,
    uti_diarias,
    uti_tipo_id_sihsus,
    unidade_intermediaria_diarias,
    acompanhante_diarias,
    diarias,
    procedimento_solicitado_id_sigtap,
    procedimento_realizado_id_sigtap,
    valor_servicos_hospitalares,
    valor_servicos_profissionais,
    valor_total,
    valor_uti,
    valor_total_dolar,
    aih_data_inicio,
    aih_data_fim,
    condicao_principal_id_cid10,
    aih_tipo_id_sihsus,
    condicao_secundaria_id_cid10,
    desfecho_motivo_id_sihsus,
    estabelecimento_natureza_id_scnes
        AS estabelecimento_natureza_id_cnes,
    estabelecimento_natureza_juridica_id_scnes
        AS estabelecimento_natureza_juridica_id_cnes,
    gestao_condicao_id_sihsus,
    exame_vdrl,
    unidade_geografica_id_sus,
    usuario_idade_tipo_id_sigtap,
    usuario_idade,
    permanencia_duracao,
    obito,
    usuario_nacionalidade_id_sigtap,
    carater_atendimento_id_sihsus,
    usuario_homonimo,
    usuario_filhos_quantidade,
    usuario_instrucao_id_sihsus,
    condicao_notificacao_id_cid10,
    usuario_contraceptivo_principal_id_sihsus,
    usuario_contraceptivo_secundario_id_sihsus,
    gestacao_risco,
    usuario_id_pre_natal,
    remessa_aih_id_sequencial_longa_permanencia
        AS remessa_sequencial_aih5_id_sihsus,
    usuario_ocupacao_id_cbo2002
        AS usuario_ocupacao_id_cbo,
    usuario_atividade_id_cnae,
    usuario_vinculo_previdencia_id_sihsus,
    autorizacao_gestor_motivo_id_sihsus,
    autorizacao_gestor_tipo_id_sihsus,
    autorizacao_gestor_id_cpf,
    autorizacao_gestor_data,
    estabelecimento_id_scnes
        AS estabelecimento_id_cnes,
    mantenedora_id_cnpj
        AS mantenedora_cnpj,
    infeccao_hospitalar,
    condicao_associada_id_cid10,
    condicao_obito_id_cid10,
    complexidade_id_sihsus,
    financiamento_tipo_id_sigtap,
    financiamento_subtipo_id_sigtap,
    regra_contratual_id_scnes
        AS regra_contratual_id_cnes,
    usuario_raca_cor_id_sihsus,
    usuario_etnia_id_sus,
    remessa_aih_id_sequencial
        AS remessa_sequencial_id_sihsus,
    remessa_id_sihsus,
    cns_ausente_justificativa_auditor,
    cns_ausente_justificativa_estabelecimento,
    valor_servicos_hospitalares_complemento_federal,
    valor_servicos_profissionais_complemento_federal,
    valor_servicos_hospitalares_complemento_local,
    valor_servicos_profissionais_complemento_local,
    valor_unidade_neonatal,
    unidade_neonatal_tipo_id_sihsus,
    condicao_secundaria_1_id_cid10,
    condicao_secundaria_2_id_cid10,
    condicao_secundaria_3_id_cid10,
    condicao_secundaria_4_id_cid10,
    condicao_secundaria_5_id_cid10,
    condicao_secundaria_6_id_cid10,
    condicao_secundaria_7_id_cid10,
    condicao_secundaria_8_id_cid10,
    condicao_secundaria_9_id_cid10,
    condicao_secundaria_1_tipo_id_sihsus,
    condicao_secundaria_2_tipo_id_sihsus,
    condicao_secundaria_3_tipo_id_sihsus,
    condicao_secundaria_4_tipo_id_sihsus,
    condicao_secundaria_5_tipo_id_sihsus,
    condicao_secundaria_6_tipo_id_sihsus,
    condicao_secundaria_7_tipo_id_sihsus,
    condicao_secundaria_8_tipo_id_sihsus,
    condicao_secundaria_9_tipo_id_sihsus,
    periodo_data_inicio,
    criacao_data,
    atualizacao_data
FROM dados_publicos._sihsus_aih_reduzida_disseminacao
);
ALTER VIEW dados_publicos.sihsus_aih_reduzida_disseminacao OWNER TO engenheiras;
GRANT SELECT ON dados_publicos.sihsus_aih_reduzida_disseminacao
TO
    analistas,
    saude_mental_admin,
    saude_mental_analistas,
    saude_mental_dbt,
    saude_mental_painel,
    saude_mental_integracao,
    saude_mental_sms_aracaju
;