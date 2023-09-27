CREATE OR REPLACE FUNCTION dados_publicos.gravar_historico()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
    DECLARE
        data timestamptz;
        captura_id uuid;
        pg_transaction_id bigint;
        pg_snapshot_id txid_snapshot;
    BEGIN
        IF (TG_OP = 'INSERT' OR TG_OP = 'UPDATE') THEN
            data := current_timestamp;
            captura_id := uuid_generate_v4();
            pg_transaction_id := txid_current();
            pg_snapshot_id := txid_current_snapshot();
            WITH contagem_registros AS (
                SELECT
                    periodo_id,
                    unidade_geografica_id,
                    count(*) AS quantidade_registrada
                FROM novos_registros
                GROUP BY
                    periodo_id,
                    unidade_geografica_id
            )
            INSERT INTO configuracoes.capturas_historico(
                id,
                operacao_id,
                periodo_id,
                unidade_geografica_id,
                data,
                parametros,
                quantidade_registros,
                pg_transaction_id,
                pg_snapshot_id
            )
            SELECT DISTINCT ON (periodo_id, unidade_geografica_id)
                captura_id AS id,
                operacao.id AS operacao_id,
                periodo_id,
                unidade_geografica_id,
                data,
                operacao.parametros,
                contagem_registros.quantidade_registrada,
                pg_transaction_id,
                pg_snapshot_id
            FROM novos_registros
            LEFT JOIN contagem_registros
            USING (
                periodo_id,
                unidade_geografica_id
            )
            LEFT JOIN configuracoes.capturas_operacoes operacao
            ON 
                operacao.ativa
            AND operacao.tabela_destino = (
                TG_TABLE_SCHEMA || '.' || TG_TABLE_NAME
            )
            ORDER BY periodo_id, unidade_geografica_id;
        END IF;
        RETURN NULL; -- result is ignored since this is an AFTER TRIGGER
    EXCEPTION
        WHEN not_null_violation THEN
            RAISE WARNING 
                'Histórico de captura não registrado devido a um erro interno.'
            USING HINT = (
                'Cheque se a operação foi devidamente definida na tabela '
                '`configuracoes.capturas_operacoes`.'
            );
            RETURN NULL;
        WHEN undefined_column THEN
            RAISE WARNING 
                'Histórico de captura não registrado pois pelo menos uma das '
                'colunas `periodo_id` ou `unidade_geografica_id` está ausente.'
            USING HINT = (
                'Reformule a estrutura da tabela inserida para conter todos os '
                'campos obrigatórios.'
            );
            RETURN NULL;
    END;
$function$
;


INSERT INTO configuracoes._capturas_historico
SELECT DISTINCT ON (
        operacao_id,
        periodo_id,
        unidade_geografica_id,
        data
    )
    *
FROM configuracoes.capturas_historico
ORDER BY 
    operacao_id,
    periodo_id,
    unidade_geografica_id,
    data
;


CREATE OR REPLACE FUNCTION
    dados_publicos.adicionar_gatilhos_gravacao_historico()
RETURNS event_trigger
LANGUAGE plpgsql AS
$$
DECLARE
    tabela_nome text;
BEGIN
    FOR tabela_nome IN (
        SELECT 
            DISTINCT tablename
        FROM pg_catalog.pg_tables
        WHERE 
            schemaname = 'dados_publicos'
        AND tableowner = current_user
    ) LOOP
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'dados_publicos'
            AND table_name = tabela_nome
            AND column_name = 'unidade_geografica_id'
        ) OR NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'dados_publicos'
            AND table_name = tabela_nome
            AND column_name = 'periodo_id'
        ) THEN
            RAISE WARNING 
                'Campo(s) `unidade_geografica_id` e/ou `periodo_id` não '
                'existe(m) na tabela `dados_publicos.%`. Pulando.',
                quote_ident(tabela_nome)
            ;
        ELSE
            RAISE NOTICE
                'Removendo gatilhos de gravação de histórico já existente na '
                'tabela `dados_publicos.%`, se houver',
                quote_ident(tabela_nome)
            ;
            EXECUTE format(
                'DROP TRIGGER IF EXISTS gravar_historico on dados_publicos.%I;',
                tabela_nome
            );
            EXECUTE format(
                'DROP TRIGGER IF EXISTS gravar_historico_insercao '
                'ON dados_publicos.%I;',
                tabela_nome
            );
            EXECUTE format(
                'DROP TRIGGER IF EXISTS gravar_historico_atualizacao '
                'ON dados_publicos.%I;',
                tabela_nome
            );
            RAISE NOTICE 
                'Adicionando gatilhos de gravação de histórico na tabela '
                '`dados_publicos.%`...',
                quote_ident(tabela_nome)
            ;
            EXECUTE format(
                'CREATE TRIGGER gravar_historico_insercao '
                'AFTER INSERT ON dados_publicos.%I '
                'REFERENCING NEW TABLE AS novos_registros '
                'FOR EACH STATEMENT '
                'EXECUTE FUNCTION dados_publicos.gravar_historico();',
                tabela_nome
            );
            EXECUTE format(
                'CREATE TRIGGER gravar_historico_atualizacao '
                'AFTER UPDATE ON dados_publicos.%I '
                'REFERENCING NEW TABLE AS novos_registros '
                'FOR EACH STATEMENT '
                'EXECUTE FUNCTION dados_publicos.gravar_historico();',
                tabela_nome
            );
            RAISE NOTICE 'OK!';
        END IF;
    END LOOP;
END;
$$;


COMMENT ON FUNCTION dados_publicos.adicionar_gatilhos_gravacao_historico() IS 
'Cria ou atualiza os gatilhos de gravação de histórico das capturas nas '
'tabelas do schema `dados_publicos`.'
;


CREATE EVENT TRIGGER adicionar_gatilhos_gravacao_historico
ON ddl_command_end
WHEN TAG IN ('CREATE TABLE', 'CREATE TABLE AS')
EXECUTE FUNCTION dados_publicos.adicionar_gatilhos_gravacao_historico();


COMMENT ON EVENT TRIGGER adicionar_gatilhos_gravacao_historico IS 
'Chama a função `dados_publicos.adicionar_gatilhos_gravacao_historico()` a '
'vez que uma nova tabela é criada, garantindo que as novas tabelas tenham os '
'gatilhos apropriados de gravação de capturas.'
;


-- Testes:
--INSERT INTO dados_publicos._sisab_cadastros_parametro_cnes_ine_equipes_validas
--SELECT 
--    municipio_id_sus,
--    cnes_id,
--    cnes_nome,
--    ine_id,
--    parametro,
--    periodo_id,
--    periodo_codigo,
--    unidade_geografica_id,
--    now() AS criacao_data,
--    now() AS atualizacao_data
--FROM dados_publicos._sisab_cadastros_parametro_cnes_ine_equipes_validas
--WHERE municipio_id_sus IN ('330330', '280030');
--
-- Substitua a data/hora por uma data/hora recente
--SELECT * FROM configuracoes.capturas_historico WHERE data > '2022-04-20T10:35:00-03:00'::timestamptz;
--
--DELETE FROM dados_publicos._sisab_cadastros_parametro_cnes_ine_equipes_validas
--WHERE criacao_data > '2022-04-20T10:35:00-03:00'::timestamptz;

DELETE FROM configuracoes.capturas_historico WHERE operacao_id = 'c84c1917-4f57-4592-a974-50a81b3ed6d5';
SELECT * FROM configuracoes.capturas_agendamentos WHERE operacao_id = 'c84c1917-4f57-4592-a974-50a81b3ed6d5';
SELECT * FROM pg_catalog.pg_tables;