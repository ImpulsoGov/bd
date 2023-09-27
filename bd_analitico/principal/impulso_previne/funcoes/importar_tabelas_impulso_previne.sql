CREATE OR REPLACE PROCEDURE impulso_previne.importar_tabelas_impulso_previne()
 LANGUAGE plpgsql
AS $procedure$
BEGIN 
	IMPORT FOREIGN SCHEMA impulso_previne 
	FROM SERVER impulso_previne_producao INTO _impulso_previne_producao;
	RESET ROLE;
END;
$procedure$
;
