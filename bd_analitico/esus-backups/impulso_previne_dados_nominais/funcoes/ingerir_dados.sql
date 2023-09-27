CREATE OR REPLACE PROCEDURE impulso_previne_dados_nominais.ingerir_dados(tabela_destino text, codigo text)
 LANGUAGE plpgsql
AS $procedure$
BEGIN
    EXECUTE format(
         'INSERT INTO %s (%s)',
        tabela_destino, codigo);
END;
$procedure$
;
