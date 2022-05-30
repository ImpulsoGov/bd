/******************************************************************************

          INTEGRAÇÃO DOS BANCOS ANALÍTICO E DE PRODUÇÃO

 ******************************************************************************/


CREATE EXTENSION IF NOT EXISTS postgres_fdw WITH SCHEMA pg_catalog;

CREATE SERVER producao
FOREIGN DATA WRAPPER postgres_fdw
OPTIONS (
    host :IMPULSOBD_PRODUCAO_HOST,
    port :IMPULSOBD_PRODUCAO_PORTA,
    dbname 'postgres'
);


CREATE USER MAPPING
FOR saude_mental_integracao
SERVER producao
OPTIONS (
    user 'saude_mental_integracao',
    password :IMPULSOBD_SENHA_SM_INTEGRACAO
);

CREATE ROLE saude_mental_integracao;

RESET ROLE;
DROP SCHEMA IF EXISTS _saude_mental_producao CASCADE;
CREATE SCHEMA _saude_mental_producao;
IMPORT FOREIGN SCHEMA saude_mental
-- LIMIT TO (tabela_1, tabela_2, ...)
FROM SERVER producao
INTO _saude_mental_producao;


GRANT USAGE ON SCHEMA saude_mental TO saude_mental_integracao;
GRANT SELECT ON ALL TABLES IN SCHEMA saude_mental TO saude_mental_integracao;
GRANT ALL PRIVILEGES ON SCHEMA _saude_mental_producao TO saude_mental_integracao;
GRANT INSERT, DELETE ON ALL TABLES IN SCHEMA _saude_mental_producao TO saude_mental_integracao;
GRANT USAGE ON FOREIGN SERVER producao TO saude_mental_integracao;
GRANT EXECUTE ON ALL ROUTINES IN SCHEMA _saude_mental_producao TO saude_mental_integracao;
GRANT saude_mental_integracao TO postgres;
SET ROLE saude_mental_integracao;


CREATE OR REPLACE PROCEDURE 
    _saude_mental_producao.sincronizar_tabela(tabela text)
LANGUAGE plpgsql
AS $$
BEGIN
    EXECUTE format('DELETE FROM _saude_mental_producao.%I;', tabela);
    EXECUTE format(
        'INSERT INTO _saude_mental_producao.%I SELECT * FROM saude_mental.%I;',
        tabela,
        tabela
    );
END;
$$;
COMMENT ON PROCEDURE _saude_mental_producao.sincronizar_tabela IS 
    'Copia os dados de uma tabela ou consulta do schema `saude_mental` para '
    'o seu espelho no schema `_saude_mental_producao`; o que efetivamente '
    'corresponde a transferir os dados para o banco de dados de produção.'
;


CREATE OR REPLACE PROCEDURE 
    _saude_mental_producao.sincronizar_tabelas()
LANGUAGE plpgsql
AS $$
DECLARE
    tabela text;
BEGIN
    FOR tabela IN 
        SELECT table_name FROM information_schema.tables 
        WHERE table_schema = '_saude_mental_producao'
    LOOP
        CALL _saude_mental_producao.sincronizar_tabela(tabela);
    END LOOP;
END;
$$;

COMMENT ON PROCEDURE _saude_mental_producao.sincronizar_tabelas()
IS '
Envia ao banco de produção os resultados das consultas preparadas para servir o 
painel de dados de saúde mental.
';

