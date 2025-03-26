DO $$  
DECLARE  
    r RECORD;  
BEGIN  
    -- Criar a tabela se ela não existir
    CREATE TEMP TABLE validacao_lista_nominal_citopatologico (
        schema_nome TEXT,  
        table_name text,
        cont_linhas BIGINT,
        cont_chaves_nominais_distintas BIGINT,
        denominador_cito BIGINT,  
        numerador_cito BIGINT  
    );

    -- Itera sobre todas as tabelas 'lista_nominal_citopatologico' nos schemas com prefixo 'dados_nominais_'
    FOR r IN   
        SELECT table_schema, table_name   
        FROM information_schema.tables   
        WHERE table_schema LIKE 'dados_nominais_%'   
        AND table_name in ('lista_nominal_citopatologico' ,'lista_nominal_citopatologico_obsoleta') 
    LOOP  
        -- Executa a query para cada tabela e insere os resultados na tabela de validação
        EXECUTE format(
            'WITH duplicados AS (
                SELECT 
                    %L AS schema_nome,
                    %L AS table_name,
                    COUNT(1) AS cont_linhas,
                    COUNT(DISTINCT chave_mulher) AS cont_chaves_nominais_distintas
                FROM %I.%I
            )
            , dem_num AS (
                SELECT
                    %L AS schema_nome,
                    %L AS table_name,
                    COUNT(DISTINCT l.chave_mulher) AS denominador_cito,
                    COUNT(DISTINCT CASE WHEN l.realizou_exame_ultimos_36_meses IS TRUE 
                                        THEN l.chave_mulher
                                    END) AS numerador_cito
                FROM %I.%I l
                GROUP BY 1
            )
            INSERT INTO validacao_lista_nominal_citopatologico (schema_nome,table_name,cont_linhas,cont_chaves_nominais_distintas, denominador_cito, numerador_cito)
            SELECT 
                tb1.schema_nome,
				tb1.table_name,
				tb2.cont_linhas,
				tb2.cont_chaves_nominais_distintas,
                tb1.denominador_cito,
                tb1.numerador_cito
            FROM dem_num tb1
			JOIN duplicados tb2 ON tb1.schema_nome = tb2.schema_nome AND tb1.table_name = tb2.table_name
			;',
            r.table_schema,r.table_name, r.table_schema, r.table_name,
            r.table_schema,r.table_name, r.table_schema, r.table_name
        );  
    END LOOP;  
end; $$;




SELECT table_schema, table_name   
FROM information_schema.tables  
WHERE table_name = 'temp_transmissor_validacao_listas_nominais'  


-- Consulta os resultados tabulados 
WITH 
	tabela_pivotada as (
		SELECT 
		    schema_nome as schema_municipio,
		    SUM(CASE 
		            WHEN table_name = 'lista_nominal_citopatologico_obsoleta' THEN cont_linhas 
		            ELSE 0 
		        END) AS cont_linhas_antes_da_alteracao,
		    SUM(CASE 
		            WHEN table_name = 'lista_nominal_citopatologico' THEN cont_linhas 
		            ELSE 0 
		        END) AS cont_linhas_depois_da_alteracao,
		    SUM(CASE 
		            WHEN table_name = 'lista_nominal_citopatologico_obsoleta' THEN cont_chaves_nominais_distintas 
		            ELSE 0 
		        END) AS cont_chaves_nominais_distintas_antes_da_alteracao,
		    SUM(CASE 
		            WHEN table_name = 'lista_nominal_citopatologico' THEN cont_chaves_nominais_distintas 
		            ELSE 0 
		        END) AS cont_chaves_nominais_distintas_depois_da_alteracao,
		    SUM(CASE 
		            WHEN table_name = 'lista_nominal_citopatologico_obsoleta' THEN denominador_cito 
		            ELSE 0 
		        END) AS denominador_cito_antes_da_alteracao,
		    SUM(CASE 
		            WHEN table_name = 'lista_nominal_citopatologico' THEN denominador_cito 
		            ELSE 0 
		        END) AS denominador_cito_depois_da_alteracao,
		    SUM(CASE 
		            WHEN table_name = 'lista_nominal_citopatologico_obsoleta' THEN numerador_cito 
		            ELSE 0 
		        END) AS numerador_cito_antes_da_alteracao,
		    SUM(CASE 
		            WHEN table_name = 'lista_nominal_citopatologico' THEN numerador_cito 
		            ELSE 0 
		        END) AS numerador_cito_depois_da_alteracao
		FROM validacao_lista_nominal_citopatologico
		GROUP BY schema_nome
		ORDER BY schema_nome
)
SELECT 
	schema_municipio,
	cont_linhas_antes_da_alteracao,
	cont_linhas_depois_da_alteracao,
	(cont_linhas_antes_da_alteracao - cont_linhas_depois_da_alteracao) as cont_linhas_cito_variacao,
	cont_chaves_nominais_distintas_antes_da_alteracao,
	cont_chaves_nominais_distintas_depois_da_alteracao,
	(cont_chaves_nominais_distintas_antes_da_alteracao - cont_chaves_nominais_distintas_depois_da_alteracao) as cont_chaves_nominais_distintas_cito_variacao,
	denominador_cito_antes_da_alteracao,
	denominador_cito_depois_da_alteracao,
	(denominador_cito_antes_da_alteracao - denominador_cito_depois_da_alteracao) as denominador_cito_variacao,
	numerador_cito_antes_da_alteracao,
	numerador_cito_depois_da_alteracao,
	(numerador_cito_antes_da_alteracao - numerador_cito_depois_da_alteracao) as numerador_cito_variacao
FROM tabela_pivotada

