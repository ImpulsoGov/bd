CREATE OR REPLACE PROCEDURE impulso_previne_dados_nominais.registrar_historico_dados_nominais()
 LANGUAGE plpgsql
AS $procedure$
begin
	call impulso_previne_dados_nominais.registrar_historico_lista_nominal_diabeticos();
	COMMIT;
	call impulso_previne_dados_nominais.registrar_historico_lista_nominal_gestantes();
	COMMIT;
	call impulso_previne_dados_nominais.registrar_historico_lista_nominal_hipertensos();
	COMMIT;
	call impulso_previne_dados_nominais.registrar_historico_lista_nominal_citopatologico();
	COMMIT;
END;
$procedure$
;
