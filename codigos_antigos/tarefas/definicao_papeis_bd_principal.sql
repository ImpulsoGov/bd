/* ****************************************************************************
 * 
 *         DEFINE OS PAPÉIS E PERMISSÕES NO BANCO
 * 
 * 
 ******************************************************************************/


/* 
 * OBSERVAÇÕES:
 * 
 * O correto funcionamento deste script depende da definição
 * prévia das seguintes variáveis:
 * 
 *   - `IMPULSOBD_SENHA_ETL`
 *   - `IMPULSOBD_SENHA_SM_INTEGRACAO`
 *   - `IMPULSOBD_SENHA_IP_INTEGRACAO`
 *   - `IMPULSOBD_SENHA_IP_APLICACOES`
 *   - `IMPULSOBD_SENHA_AGP_INTEGRACAO`
 *   - `IMPULSOBD_SENHA_AGP_APLICACOES`
 *   - `IMPULSOBD_SENHA_TS_INTEGRACAO`
 *   - `IMPULSOBD_SENHA_TUTORIAL`
 * 
 * Essas variáveis devem ser fornecidas por meio de uma chamada prévia do
 * cliente psql, com o comando `\set`, no formato `\set VARIAVEL 'valor'`.
 * 
 * Além das variáveis, todos os schemas previstos devem ter sido criados
 * antes de rodar este script; caso contrário, serão levantados erros.
 */


/* -------------------------------------------------------------------------- */

-- Define os níveis de permissão básicos disponíveis

CREATE ROLE
    base
WITH
    NOSUPERUSER
    NOINHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
;
GRANT base TO postgres WITH ADMIN OPTION;

CREATE ROLE
    tutorial
WITH
    NOSUPERUSER
    NOINHERIT
    LOGIN
    NOREPLICATION
    NOBYPASSRLS
    ENCRYPTED PASSWORD :IMPULSOBD_SENHA_TUTORIAL
CONNECTION LIMIT 10
;
GRANT tutorial TO postgres;
GRANT CONNECT ON DATABASE postgres TO tutorial;

CREATE USER etl
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    NOINHERIT
    LOGIN
    NOREPLICATION
    NOBYPASSRLS
    ENCRYPTED PASSWORD :IMPULSOBD_SENHA_ETL
CONNECTION LIMIT 10
;
GRANT etl TO postgres;

CREATE ROLE
    analistas
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE base
;
GRANT analistas TO postgres WITH ADMIN OPTION;

CREATE ROLE
    engenheiras
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE base, analistas
;
GRANT engenheiras TO postgres WITH ADMIN OPTION;

CREATE ROLE
    projuto_admin
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE analistas
;
GRANT projuto_admin TO postgres WITH ADMIN OPTION;

CREATE ROLE
    projuto_integracao
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE base
;
GRANT projuto_integracao TO postgres WITH ADMIN OPTION;
    

/* -------------------------------------------------------------------------- */

-- Define os níveis de permissão relacionados a projetos e produtos


-- Saúde Mental

CREATE ROLE
    saude_mental_analistas
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE analistas
;
GRANT saude_mental_analistas TO postgres WITH ADMIN OPTION;

CREATE ROLE 
    saude_mental_admin
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE projuto_admin
    ADMIN saude_mental_analistas
;
GRANT saude_mental_admin TO postgres WITH ADMIN OPTION;

CREATE ROLE 
    saude_mental_integracao
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    LOGIN
    NOREPLICATION
    NOBYPASSRLS
    ENCRYPTED PASSWORD :IMPULSOBD_SENHA_SM_INTEGRACAO
    IN ROLE projuto_integracao
;
GRANT saude_mental_integracao TO postgres;


-- Impulso Previne

CREATE ROLE 
    impulso_previne_analistas
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE analistas
;
GRANT impulso_previne_analistas TO postgres WITH ADMIN OPTION;

CREATE ROLE 
    impulso_previne_admin
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE base, analistas, projuto_admin
    ADMIN impulso_previne_analistas
;
GRANT impulso_previne_admin TO postgres WITH ADMIN OPTION;

CREATE ROLE 
    impulso_previne_aplicacoes
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    LOGIN
    NOREPLICATION
    NOBYPASSRLS
    ENCRYPTED PASSWORD :IMPULSOBD_SENHA_IP_APLICACOES
    IN ROLE base
;
GRANT impulso_previne_aplicacoes TO postgres;
GRANT USAGE
ON SCHEMA impulso_previne
TO impulso_previne_aplicacoes;
GRANT SELECT
ON ALL TABLES
IN SCHEMA impulso_previne TO impulso_previne_aplicacoes;

CREATE ROLE 
    impulso_previne_integracao
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    LOGIN
    NOREPLICATION
    NOBYPASSRLS
    ENCRYPTED PASSWORD :IMPULSOBD_SENHA_IP_INTEGRACAO
    IN ROLE projuto_integracao
;
GRANT impulso_previne_integracao TO postgres;


-- AGP

CREATE ROLE 
    agp_analistas
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE analistas
;
GRANT agp_analistas TO postgres WITH ADMIN OPTION;

CREATE ROLE 
    agp_admin
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE projuto_admin
    ADMIN agp_analistas
;
GRANT agp_admin TO postgres WITH ADMIN OPTION;

CREATE ROLE 
    agp_integracao
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    LOGIN
    NOREPLICATION
    NOBYPASSRLS
    ENCRYPTED PASSWORD :IMPULSOBD_SENHA_AGP_INTEGRACAO
    IN ROLE projuto_integracao
;
GRANT agp_integracao TO postgres;

CREATE ROLE 
    agp_aplicacoes
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    LOGIN
    NOREPLICATION
    NOBYPASSRLS
    ENCRYPTED PASSWORD :IMPULSOBD_SENHA_AGP_APLICACOES
    IN ROLE base
;
GRANT agp_aplicacoes TO postgres;
GRANT USAGE
ON SCHEMA agp
TO agp_aplicacoes;
GRANT SELECT
ON ALL TABLES
IN SCHEMA agp, dados_publicos, listas_de_codigos TO agp_aplicacoes;


-- Territórios Saudáveis

CREATE ROLE 
    territorios_saudaveis_analistas
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE base, analistas
;
GRANT territorios_saudaveis_analistas TO postgres WITH ADMIN OPTION;

CREATE ROLE 
    territorios_saudaveis_admin
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE base, analistas, projuto_admin
    ADMIN territorios_saudaveis_analistas
;
GRANT territorios_saudaveis_admin TO postgres WITH ADMIN OPTION;

CREATE ROLE 
    territorios_saudaveis_integracao
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    LOGIN
    NOREPLICATION
    NOBYPASSRLS
    ENCRYPTED PASSWORD :IMPULSOBD_SENHA_TS_INTEGRACAO
    IN ROLE base
;
GRANT territorios_saudaveis_integracao TO postgres;

/* -------------------------------------------------------------------------- */

-- Define os níveis de permissão por município


-- Santa Filomena (PE)

CREATE ROLE 
    pe_santa_filomena_analistas
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE base, analistas
;
GRANT pe_santa_filomena_analistas TO postgres WITH ADMIN OPTION;

CREATE ROLE 
    pe_santa_filomena_admin
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE base, analistas
    ADMIN pe_santa_filomena_analistas
;
GRANT pe_santa_filomena_admin TO postgres WITH ADMIN OPTION;


-- São Gonçalo do Abaeté (MG)

CREATE ROLE 
    mg_sao_goncalo_abaete_analistas
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE base, analistas
;
GRANT mg_sao_goncalo_abaete_analistas TO postgres WITH ADMIN OPTION;

CREATE ROLE 
    mg_sao_goncalo_abaete_admin
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE base, analistas
    ADMIN mg_sao_goncalo_abaete_analistas
;
GRANT mg_sao_goncalo_abaete_admin TO postgres WITH ADMIN OPTION;


-- Itapetininga (SP)

CREATE ROLE 
    sp_itapetininga_analistas
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE base, analistas
;
GRANT sp_itapetininga_analistas TO postgres WITH ADMIN OPTION;

CREATE ROLE 
    sp_itapetininga_admin
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE base, analistas
    ADMIN sp_itapetininga_analistas
;
GRANT sp_itapetininga_admin TO postgres WITH ADMIN OPTION;


-- Juquitiba (SP)

CREATE ROLE 
    sp_juquitiba_analistas
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE base, analistas
;
GRANT sp_juquitiba_analistas TO postgres WITH ADMIN OPTION;

CREATE ROLE 
    sp_juquitiba_admin
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE base, analistas
    ADMIN sp_juquitiba_analistas
;
GRANT sp_juquitiba_admin TO postgres WITH ADMIN OPTION;


-- Piraju (SP)

CREATE ROLE 
    sp_piraju_analistas
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE base, analistas
;
GRANT sp_piraju_analistas TO postgres WITH ADMIN OPTION;

CREATE ROLE 
    sp_piraju_admin
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE base, analistas
    ADMIN sp_piraju_analistas
;
GRANT sp_piraju_admin TO postgres WITH ADMIN OPTION;


-- São Lucas do Rio Verde (MT)

CREATE ROLE 
    mt_sao_lucas_rio_verde_analistas
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE base, analistas
;
GRANT mt_sao_lucas_rio_verde_analistas TO postgres WITH ADMIN OPTION;

CREATE ROLE 
    mt_sao_lucas_rio_verde_admin
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE base, analistas
    ADMIN mt_sao_lucas_rio_verde_analistas
;
GRANT mt_sao_lucas_rio_verde_admin TO postgres WITH ADMIN OPTION;


-- Niquelândia (GO)

CREATE ROLE 
    go_niquelandia_analistas
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE base, analistas
;
GRANT go_niquelandia_analistas TO postgres WITH ADMIN OPTION;

CREATE ROLE 
    go_niquelandia_admin
WITH
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    INHERIT
    NOLOGIN
    NOREPLICATION
    NOBYPASSRLS
    IN ROLE base, analistas
    ADMIN go_niquelandia_analistas
;
GRANT go_niquelandia_admin TO postgres WITH ADMIN OPTION;



/* ------------------------------------------------------------------------- */

-- Define permissões de conexão ao banco

GRANT CONNECT
ON DATABASE postgres
TO base;

GRANT CONNECT
ON DATABASE principal
TO base, etl;

GRANT CONNECT
ON DATABASE "esus-backups"
TO analistas;

/* ------------------------------------------------------------------------- */

-- Permissões especiais

-- Criar rotinas com linguagem procedural

GRANT USAGE
ON LANGUAGE plpgsql
TO 
    engenheiras,
    projuto_admin
;

-- Criar conexões com outros bancos de dados PostgreSQL
GRANT USAGE
ON FOREIGN DATA WRAPPER postgres_fdw
TO 
    engenheiras,
    projuto_admin,
    projuto_integracao
;


/* ------------------------------------------------------------------------- */

-- Define permissões de uso por schema

-- Schemas de uso geral

GRANT USAGE
ON SCHEMA listas_de_codigos
TO base, etl;

GRANT USAGE
ON SCHEMA dados_publicos
TO analistas, etl;

GRANT USAGE
ON SCHEMA configuracoes
TO engenheiras, etl;

GRANT CREATE
ON SCHEMA
    configuracoes,
    dados_publicos,
    listas_de_codigos
TO
    engenheiras
;

-- Extensões

GRANT USAGE
ON SCHEMA
    extensoes,
    postgis
TO base;
    
GRANT USAGE
ON SCHEMA
    cron
TO
    engenheiras,
    projuto_admin,
    projuto_integracao
;

-- Schemas dos projetos

-- Saúde mental

GRANT CREATE, USAGE
ON SCHEMA saude_mental
TO saude_mental_analistas;

GRANT USAGE
ON SCHEMA _saude_mental_producao
TO saude_mental_integracao;

GRANT CREATE
ON SCHEMA _saude_mental_producao
TO saude_mental_admin;

-- Impulso Previne

GRANT CREATE, USAGE
ON SCHEMA impulso_previne
TO impulso_previne_analistas;

GRANT USAGE
ON SCHEMA _impulso_previne_producao
TO impulso_previne_integracao;

GRANT CREATE
ON SCHEMA _impulso_previne_producao
TO impulso_previne_admin;

-- AGP

GRANT CREATE, USAGE
ON SCHEMA agp
TO agp_analistas;

GRANT USAGE
ON SCHEMA _agp_producao
TO agp_integracao;

GRANT CREATE
ON SCHEMA _agp_producao
TO agp_admin;

-- Territórios Saudáveis

GRANT CREATE, USAGE
ON SCHEMA territorios_saudaveis
TO territorios_saudaveis_analistas;

GRANT USAGE
ON SCHEMA _territorios_saudaveis_producao
TO territorios_saudaveis_integracao;

GRANT CREATE
ON SCHEMA _territorios_saudaveis_producao
TO territorios_saudaveis_admin;



/* -------------------------------------------------------------------------- */

-- Define as permissões em nível de tabelas

-- Tabelas de uso comum

GRANT SELECT, REFERENCES
ON ALL TABLES
IN SCHEMA listas_de_codigos
TO base, etl;

GRANT SELECT
ON ALL TABLES
IN SCHEMA dados_publicos
TO analistas, etl;

GRANT SELECT
ON ALL TABLES
IN SCHEMA configuracoes
TO etl;

GRANT INSERT
ON configuracoes.capturas_historico
TO etl;

GRANT ALL PRIVILEGES
ON ALL TABLES
IN SCHEMA
    configuracoes,
    listas_de_codigos,
    dados_publicos
TO engenheiras;

-- Extensões

GRANT ALL PRIVILEGES
ON ALL TABLES
IN SCHEMA
    extensoes,
    cron,
    postgis
TO engenheiras;

GRANT SELECT
ON ALL TABLES
IN SCHEMA
    extensoes,
    postgis
TO base;

GRANT SELECT
ON ALL TABLES
IN SCHEMA cron
TO
    projuto_admin,
    projuto_integracao
;

GRANT INSERT, UPDATE
ON ALL TABLES
IN SCHEMA cron
TO projuto_admin;

-- Tabelas dos projetos

-- Saúde mental

GRANT SELECT
ON ALL TABLES
IN SCHEMA saude_mental
TO saude_mental_analistas;

GRANT ALL PRIVILEGES
ON ALL TABLES
IN SCHEMA
    saude_mental,
    _saude_mental_producao
TO saude_mental_admin;

GRANT SELECT
ON ALL TABLES IN _saude_mental_producao
TO saude_mental_integracao;

-- Impulso Previne

GRANT SELECT
ON ALL TABLES
IN SCHEMA impulso_previne
TO impulso_previne_analistas;

GRANT ALL PRIVILEGES
ON ALL TABLES
IN SCHEMA 
    impulso_previne,
    _impulso_previne_producao
TO impulso_previne_admin;

GRANT SELECT
ON ALL TABLES IN _impulso_previne_producao
TO impulso_previne_integracao;

-- AGP

GRANT SELECT
ON ALL TABLES
IN SCHEMA agp
TO agp_analistas;

GRANT ALL PRIVILEGES
ON ALL TABLES
IN SCHEMA 
    agp,
    _agp_producao
TO agp_admin;

GRANT SELECT
ON ALL TABLES
IN SCHEMA _agp_producao
TO agp_integracao;

-- Territórios Saudáveis

GRANT SELECT
ON ALL TABLES
IN SCHEMA territorios_saudaveis
TO territorios_saudaveis_analistas;

GRANT ALL PRIVILEGES
ON ALL TABLES
IN SCHEMA
    territorios_saudaveis,
    _territorios_saudaveis_producao
TO territorios_saudaveis_admin;

GRANT SELECT
ON ALL TABLES
IN SCHEMA _territorios_saudaveis_producao
TO territorios_saudaveis_integracao;

/* -------------------------------------------------------------------------- */

-- Permissões em nível de funções, procedimenos e outras rotinas

GRANT EXECUTE
ON ALL FUNCTIONS
IN SCHEMA listas_de_codigos
TO base, etl;

GRANT EXECUTE
ON ALL ROUTINES
IN SCHEMA
    configuracoes,
    dados_publicos,
    listas_de_codigos
TO engenheiras;


-- Extensões

GRANT EXECUTE
ON ALL FUNCTIONS
IN SCHEMA
    extensoes,
    postgis
TO base, etl;

GRANT EXECUTE
ON ALL ROUTINES
IN SCHEMA
    extensoes,
    postgis,
    cron
TO engenheiras;

GRANT EXECUTE
ON ALL FUNCTIONS
IN SCHEMA cron
TO
    projuto_admin,
    projuto_integracao
;

-- Projetos

-- Saúde Mental

GRANT EXECUTE
ON ALL FUNCTIONS
IN SCHEMA saude_mental
TO saude_mental_analistas;

GRANT EXECUTE
ON ALL ROUTINES
IN SCHEMA
    saude_mental,
    _saude_mental_producao
TO saude_mental_admin;

GRANT EXECUTE
ON ALL ROUTINES
IN SCHEMA _saude_mental_producao
TO saude_mental_integracao;

-- Impulso Previne

GRANT EXECUTE
ON ALL FUNCTIONS
IN SCHEMA impulso_previne
TO impulso_previne_analistas;

GRANT EXECUTE
ON ALL ROUTINES
IN SCHEMA
    impulso_previne,
    _impulso_previne_producao
TO impulso_previne_admin;

GRANT EXECUTE
ON ALL ROUTINES
IN SCHEMA _impulso_previne_producao
TO impulso_previne_integracao;

-- AGP

GRANT EXECUTE
ON ALL FUNCTIONS
IN SCHEMA agp
TO agp_analistas;

GRANT EXECUTE
ON ALL ROUTINES
IN SCHEMA
    agp,
    _agp_producao
TO agp_admin;

GRANT EXECUTE
ON ALL ROUTINES
IN SCHEMA _agp_producao
TO agp_integracao;

-- Territórios Saudáveis

GRANT EXECUTE
ON ALL FUNCTIONS
IN SCHEMA territorios_saudaveis
TO territorios_saudaveis_analistas;

GRANT EXECUTE
ON ALL ROUTINES
IN SCHEMA
    territorios_saudaveis,
    _territorios_saudaveis_producao
TO territorios_saudaveis_admin;

GRANT EXECUTE
ON ALL ROUTINES
IN SCHEMA _territorios_saudaveis_producao
TO territorios_saudaveis_integracao;

