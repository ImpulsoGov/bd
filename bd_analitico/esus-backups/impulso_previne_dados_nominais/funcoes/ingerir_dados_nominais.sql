CREATE OR REPLACE PROCEDURE impulso_previne_dados_nominais.ingerir_dados_nominais()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    resultado record;
    nome_esquema text;
    nome_tabela text;
    tabela_existe boolean;
BEGIN
    FOR resultado IN (
        -- Consulta todos os códigos de todas as tabelas destino 
        SELECT codigo, tabela_destino 
        FROM configuracoes.versionamento_codigo_tabelas_consolidadas
        WHERE parametro_ativo
        AND painel_nome IN ('Citopatológico', 'Gestantes', 'Diabéticos', 'Hipertensos') )
    loop
        -- Separando o nome do esquema e da tabela
        nome_esquema := substring(resultado.tabela_destino FROM '([^\.]+)');
        nome_tabela := substring(resultado.tabela_destino FROM '\.(.+)');
        EXECUTE format('SELECT EXISTS (
                          SELECT 1
                          FROM information_schema.tables
                          WHERE table_schema = %L
                          AND table_name = %L
                       )', nome_esquema, nome_tabela) INTO tabela_existe;
        IF NOT tabela_existe THEN 
            EXECUTE format(
                'CREATE TABLE %I AS %s',
                resultado.tabela_destino, resultado.codigo
            );
            CONTINUE;
        END IF;
        EXECUTE format(
        'TRUNCATE TABLE %s;
         INSERT INTO %s (%s);',
        resultado.tabela_destino, resultado.tabela_destino, resultado.codigo);
    END LOOP;
END;
$procedure$
;