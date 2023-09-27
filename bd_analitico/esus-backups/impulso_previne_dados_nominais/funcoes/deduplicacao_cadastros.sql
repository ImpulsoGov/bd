CREATE OR REPLACE PROCEDURE impulso_previne_dados_nominais.deduplicacao_cadastros()
 LANGUAGE plpgsql
AS $procedure$
begin
	call impulso_previne_dados_nominais.gestantes_duplicadas_por_erro_grafia();
	COMMIT;
	call impulso_previne_dados_nominais.gestantes_duplicadas_por_erro_data_nascimento();
	COMMIT;
END;
$procedure$
;
