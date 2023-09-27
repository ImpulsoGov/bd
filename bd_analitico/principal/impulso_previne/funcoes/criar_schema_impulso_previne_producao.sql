CREATE OR REPLACE PROCEDURE impulso_previne.criar_schema_impulso_previne_producao()
 LANGUAGE plpgsql
AS $procedure$
BEGIN 
	DROP SCHEMA IF EXISTS 
			_impulso_previne_producao
	CASCADE;
	CREATE SCHEMA
			_impulso_previne_producao;
	RESET ROLE;
END;
$procedure$
;
