DO $$  
DECLARE  
    r RECORD;  
BEGIN  
    -- Criar a tabela se ela não existir
    CREATE TEMP TABLE validacao_lista_nominal_hipertensos (
        table_schema TEXT,  
        table_name text,
        cont_linhas BIGINT,
        cont_chaves_nominais_distintas BIGINT,
        hipertensao_denominador BIGINT,  
        cidadaos_autorreferidos BIGINT,
        cidadaos_diagnosticados BIGINT,
        numerador_hipertensao BIGINT,
        cidadaos_com_consulta BIGINT,
        cidadaos_com_consulta_sem_diagnotico BIGINT
    );

    -- Itera sobre todas as tabelas 'lista_nominal_hipertensos' nos schemas com prefixo 'dados_nominais_'
    FOR r IN   
        SELECT table_schema, table_name   
        FROM information_schema.tables   
        WHERE table_schema LIKE 'dados_nominais_%'   
        AND table_name in ('lista_nominal_hipertensos' ,'lista_nominal_hipertensos_obsoleta') 
    LOOP  
        -- Executa a query para cada tabela e insere os resultados na tabela de validação
        EXECUTE format(
		'WITH duplicados AS (
		    SELECT 
			%L AS table_schema,
			%L AS table_name,
			COUNT(1) AS cont_linhas,
			COUNT(DISTINCT cidadao_nome||dt_nascimento) AS cont_chaves_nominais_distintas
		    FROM %I.%I
		    GROUP BY 1
		)
		-- Checar denominador e numerador
		, dem_num AS (
			SELECT
			%L AS table_schema,
			%L AS table_name,
			count(DISTINCT l.cidadao_nome||l.dt_nascimento) AS hipertensao_denominador,
			count(DISTINCT CASE WHEN l.possui_hipertensao_autorreferida IS TRUE THEN l.cidadao_nome||l.dt_nascimento END) AS cidadaos_autorreferidos,
			count(DISTINCT CASE WHEN l.possui_hipertensao_diagnosticada IS TRUE THEN l.cidadao_nome||l.dt_nascimento END) AS cidadaos_diagnosticados,
			count(DISTINCT CASE 
				            WHEN l.realizou_consulta_ultimos_6_meses IS TRUE AND realizou_afericao_ultimos_6_meses IS TRUE 
				                THEN l.cidadao_nome||l.dt_nascimento 
				        END) AS numerador_hipertensao,
			count(DISTINCT CASE WHEN l.dt_consulta_mais_recente IS NOT NULL THEN l.cidadao_nome||l.dt_nascimento END) AS cidadaos_com_consulta,
			count(DISTINCT CASE WHEN l.dt_consulta_mais_recente IS NOT NULL AND l.possui_hipertensao_diagnosticada IS FALSE THEN l.cidadao_nome||l.dt_nascimento END) AS cidadaos_com_consulta_sem_diagnotico
		    FROM  %I.%I l
		    GROUP BY 1
		)
		INSERT INTO validacao_lista_nominal_hipertensos (
			table_schema,
			table_name,
			cont_linhas,
			cont_chaves_nominais_distintas, 
			hipertensao_denominador, 
			cidadaos_autorreferidos,
			cidadaos_diagnosticados,
			numerador_hipertensao,
			cidadaos_com_consulta,
			cidadaos_com_consulta_sem_diagnotico
		)
		SELECT 
		      tb1.table_schema,
		      tb1.table_name,
		      tb2.cont_linhas,
		      tb2.cont_chaves_nominais_distintas,
		      tb1.hipertensao_denominador,
		      tb1.cidadaos_autorreferidos,
		      tb1.cidadaos_diagnosticados,
		      tb1.numerador_hipertensao,
		      tb1.cidadaos_com_consulta,
		      tb1.cidadaos_com_consulta_sem_diagnotico
		FROM dem_num tb1
		JOIN duplicados tb2 ON tb1.table_schema = tb2.table_schema AND tb1.table_name = tb2.table_name;',
            r.table_schema,r.table_name, r.table_schema, r.table_name,
            r.table_schema,r.table_name, r.table_schema, r.table_name
        );  
    END LOOP;  
end; $$;


WITH 
	tabela_pivotada AS (
		SELECT 
		    table_schema AS schema_municipio,
		    SUM(CASE 
		            WHEN table_name = 'lista_nominal_hipertensos_obsoleta' THEN cont_linhas 
		            ELSE 0 
		        END) AS cont_linhas_antes_da_alteracao,
		    SUM(CASE 
		            WHEN table_name = 'lista_nominal_hipertensos' THEN cont_linhas 
		            ELSE 0 
		        END) AS cont_linhas_depois_da_alteracao,
		    SUM(CASE 
		            WHEN table_name = 'lista_nominal_hipertensos_obsoleta' THEN cont_chaves_nominais_distintas 
		            ELSE 0 
		        END) AS cont_chaves_nominais_distintas_antes_da_alteracao,
		    SUM(CASE 
		            WHEN table_name = 'lista_nominal_hipertensos' THEN cont_chaves_nominais_distintas 
		            ELSE 0 
		        END) AS cont_chaves_nominais_distintas_depois_da_alteracao,
		    SUM(CASE 
		            WHEN table_name = 'lista_nominal_hipertensos_obsoleta' THEN hipertensao_denominador 
		            ELSE 0 
		        END) AS hipertensao_denominador_antes_da_alteracao,
		    SUM(CASE 
		            WHEN table_name = 'lista_nominal_hipertensos' THEN hipertensao_denominador 
		            ELSE 0 
		        END) AS hipertensao_denominador_depois_da_alteracao,
		    SUM(CASE 
		            WHEN table_name = 'lista_nominal_hipertensos_obsoleta' THEN numerador_hipertensao 
		            ELSE 0 
		        END) AS numerador_hipertensao_antes_da_alteracao,
		    SUM(CASE 
		            WHEN table_name = 'lista_nominal_hipertensos' THEN numerador_hipertensao 
		            ELSE 0 
		        END) AS numerador_hipertensao_depois_da_alteracao,
		    SUM(CASE 
		            WHEN table_name = 'lista_nominal_hipertensos_obsoleta' THEN cidadaos_autorreferidos 
		            ELSE 0 
		        END) AS cidadaos_autorreferidos_antes_da_alteracao,
		    SUM(CASE 
		            WHEN table_name = 'lista_nominal_hipertensos' THEN cidadaos_autorreferidos 
		            ELSE 0 
		        END) AS cidadaos_autorreferidos_depois_da_alteracao,
		    SUM(CASE 
		            WHEN table_name = 'lista_nominal_hipertensos_obsoleta' THEN cidadaos_diagnosticados 
		            ELSE 0 
		        END) AS cidadaos_diagnosticados_antes_da_alteracao,
		    SUM(CASE 
		            WHEN table_name = 'lista_nominal_hipertensos' THEN cidadaos_diagnosticados 
		            ELSE 0 
		        END) AS cidadaos_diagnosticados_depois_da_alteracao,
		    SUM(CASE 
		            WHEN table_name = 'lista_nominal_hipertensos_obsoleta' THEN cidadaos_com_consulta 
		            ELSE 0 
		        END) AS cidadaos_com_consulta_antes_da_alteracao,
		    SUM(CASE 
		            WHEN table_name = 'lista_nominal_hipertensos' THEN cidadaos_com_consulta 
		            ELSE 0 
		        END) AS cidadaos_com_consulta_depois_da_alteracao,
		    SUM(CASE 
		            WHEN table_name = 'lista_nominal_hipertensos_obsoleta' THEN cidadaos_com_consulta_sem_diagnotico 
		            ELSE 0 
		        END) AS cidadaos_com_consulta_sem_diagnotico_antes_da_alteracao,
		    SUM(CASE 
		            WHEN table_name = 'lista_nominal_hipertensos' THEN cidadaos_com_consulta_sem_diagnotico 
		            ELSE 0 
		        END) AS cidadaos_com_consulta_sem_diagnotico_depois_da_alteracao
		FROM validacao_lista_nominal_hipertensos
		GROUP BY table_schema
		ORDER BY table_schema
	)
SELECT 
	schema_municipio,
	cont_linhas_antes_da_alteracao,
	cont_linhas_depois_da_alteracao,
	(cont_linhas_antes_da_alteracao - cont_linhas_depois_da_alteracao) AS cont_linhas_hipertensao_variacao,
	cont_chaves_nominais_distintas_antes_da_alteracao,
	cont_chaves_nominais_distintas_depois_da_alteracao,
	(cont_chaves_nominais_distintas_antes_da_alteracao - cont_chaves_nominais_distintas_depois_da_alteracao) AS cont_chaves_nominais_distintas_hipertensao_variacao,
	hipertensao_denominador_antes_da_alteracao,
	hipertensao_denominador_depois_da_alteracao,
	(hipertensao_denominador_antes_da_alteracao - hipertensao_denominador_depois_da_alteracao) AS hipertensao_denominador_variacao,
	numerador_hipertensao_antes_da_alteracao,
	numerador_hipertensao_depois_da_alteracao,
	(numerador_hipertensao_antes_da_alteracao - numerador_hipertensao_depois_da_alteracao) AS numerador_hipertensao_variacao,
	cidadaos_autorreferidos_antes_da_alteracao,
	cidadaos_autorreferidos_depois_da_alteracao,
	(cidadaos_autorreferidos_antes_da_alteracao - cidadaos_autorreferidos_depois_da_alteracao) AS cidadaos_autorreferidos_variacao,
	cidadaos_diagnosticados_antes_da_alteracao,
	cidadaos_diagnosticados_depois_da_alteracao,
	(cidadaos_diagnosticados_antes_da_alteracao - cidadaos_diagnosticados_depois_da_alteracao) AS cidadaos_diagnosticados_variacao,
	cidadaos_com_consulta_antes_da_alteracao,
	cidadaos_com_consulta_depois_da_alteracao,
	(cidadaos_com_consulta_antes_da_alteracao - cidadaos_com_consulta_depois_da_alteracao) AS cidadaos_com_consulta_variacao,
	cidadaos_com_consulta_sem_diagnotico_antes_da_alteracao,
	cidadaos_com_consulta_sem_diagnotico_depois_da_alteracao,
	(cidadaos_com_consulta_sem_diagnotico_antes_da_alteracao - cidadaos_com_consulta_sem_diagnotico_depois_da_alteracao) AS cidadaos_com_consulta_sem_diagnotico_variacao
FROM tabela_pivotada;

