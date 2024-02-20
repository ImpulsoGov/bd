CREATE OR REPLACE PROCEDURE impulso_previne_dados_nominais.registrar_historico_lista_nominal_vacinacao()
 LANGUAGE plpgsql
AS $procedure$
begin
	--refresh materialized view impulso_previne_dados_nominais.eventos_vacinacao
	
	insert into impulso_previne_dados_nominais.lista_nominal_vacinacao_historico 
	select * from impulso_previne_dados_nominais.eventos_vacinacao;
END;
$procedure$
;
