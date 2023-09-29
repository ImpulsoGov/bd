CREATE OR REPLACE PROCEDURE impulso_previne.ingerir_dados_capitacao_ponderada_cadastros()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    resultado record;
   	nome_esquema text;
   	nome_tabela text;
   	tabela_existe boolean;
BEGIN
    FOR resultado IN (
    	 --  Consulta todas os códigos de todas as  tabelas destino 
        SELECT codigo, tabela_destino 
        FROM impulso_previne.versionamento_codigo_tabelas_consolidadas
        where parametro_ativo
        and painel_nome = 'Capitação Ponderada - Cadastros'
    )
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
		if not tabela_existe then 
		EXECUTE format(
            'CREATE TABLE %s
             AS %s',
            resultado.tabela_destino, resultado.codigo
        );
       continue;
       END IF;
        EXECUTE format(
            'TRUNCATE TABLE %s;
             INSERT INTO %s
              (%s);',
            resultado.tabela_destino, resultado.tabela_destino, resultado.codigo
        );
    END LOOP;
END;
$procedure$
;
