/* Cria a extensao postgres_fdw; */
DROP EXTENSION IF EXISTS 
postgres_fdw
CASCADE;
CREATE EXTENSION postgres_fdw;

/* Cria conexao com o banco da impulso gov */
DROP SERVER IF EXISTS 
impulsogov
CASCADE;
CREATE SERVER impulsogov
	FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (host '35.239.239.250', dbname 'esus-backups',port '5432');

/* Utiliza um usuario com permiss?es restritas para o municipio acessar nosso banco */
CREATE USER MAPPING FOR postgres
	SERVER impulsogov
    OPTIONS (user 'impulsolandia_transmissor', password '*XXXXXXXXXXXXXXXXX');

/* Cria index para tabela tb_fat_cidadao_pec a fim de melhorar performace da criacaoo da view */
DROP INDEX IF exists
idx_impulso_cidadaopec_nomenascimento 
CASCADE;
CREATE INDEX idx_impulso_cidadaopec_nomenascimento ON public.tb_fat_cidadao_pec USING btree (no_cidadao, co_dim_tempo_nascimento);

/* Cria o schema impulsogov_trasmissor que recebera as tabelas importadas */
CREATE OR REPLACE FUNCTION public.criar_schema() RETURNS void
AS $$
BEGIN 
	DROP SCHEMA IF EXISTS 
			impulsogov_trasmissor
	CASCADE;
	CREATE SCHEMA
			impulsogov_trasmissor;
END;
$$ LANGUAGE plpgsql;

/* Importa tabelas que receberao os dados transmitidos para o schema impulsogov_trasmissor */
CREATE OR REPLACE FUNCTION public.importar_tabelas() RETURNS void
AS $$
BEGIN 
	IMPORT FOREIGN SCHEMA configuracoes LIMIT TO (transmissor_historico,transmissor_parametros)
	FROM SERVER impulsogov INTO impulsogov_trasmissor;
	IMPORT FOREIGN SCHEMA dados_nominais_impulsolandia
	FROM SERVER impulsogov INTO impulsogov_trasmissor;
END;
$$ LANGUAGE plpgsql;


/* Cria funcaoo procedimental para transferir os dados da view de lista nominal de gestantes para a tabela importada da impulso */
CREATE OR REPLACE FUNCTION public.transmitir_lista_nominal_gestantes() returns void
AS $$
DECLARE
    var_view_codigo text;
   	var_tabela_nome text;
  	var_tabela_campos text;
  	var_projuto_nome text;
  	erro_mensagem text;
  	erro_contexto text;
  	check_parametros bool;
  	check_out_transmissao bool;
begin 
	SELECT True, view_codigo,tabela_nome,tabela_campos,projuto_nome into check_parametros,var_view_codigo,var_tabela_nome,var_tabela_campos,var_projuto_nome
	FROM impulsogov_trasmissor.transmissor_parametros tb1
    WHERE tb1.parametro_ativo = TRUE and projuto_nome in ('Impulso Previne - Dados Nominais') and tabela_nome in ('lista_nominal_gestantes')
    ORDER BY view_versao DESC LIMIT 1;
   	SELECT True into check_out_transmissao
	FROM impulsogov_trasmissor.transmissor_historico
	where projuto_nome = var_projuto_nome and tabela_nome = var_tabela_nome and execucao_data_hora::date = current_date and municipio_id_sus = '351570';
	if check_out_transmissao is not True and check_parametros is True then 
	   	EXECUTE format('DROP MATERIALIZED VIEW IF EXISTS %s CASCADE;',var_tabela_nome);
		EXECUTE format('CREATE MATERIALIZED VIEW %s as %s;',var_tabela_nome,var_view_codigo);
		EXECUTE format('DELETE FROM impulsogov_trasmissor.%s;',var_tabela_nome);
	    EXECUTE format('INSERT INTO impulsogov_trasmissor.%s (%s)
						(SELECT %s FROM %s)',var_tabela_nome,var_tabela_campos,var_tabela_campos,var_tabela_nome);
		EXECUTE format('UPDATE impulsogov_trasmissor.%s
						SET municipio_id_sus = ''351570'';',var_tabela_nome);
		EXECUTE format('INSERT INTO impulsogov_trasmissor.transmissor_historico
						(execucao_data_hora,municipio_id_sus, mensagem, registros,tabela_nome,projuto_nome )
						VALUES(CURRENT_TIMESTAMP,''351570'', ''Trasmiss?o realizada com sucesso'', (select count (*) from %s), ''%s'', ''%s'');',var_tabela_nome,var_tabela_nome,var_projuto_nome);			
	END IF;
	EXCEPTION
	        WHEN OTHERS THEN
	            GET STACKED DIAGNOSTICS
	                erro_mensagem = MESSAGE_TEXT,
	                erro_contexto = PG_EXCEPTION_CONTEXT;
	            INSERT INTO impulsogov_trasmissor.transmissor_historico (
	            	execucao_data_hora,
	            	mensagem,
	            	municipio_id_sus,
	            	erro_contexto,
	            	projuto_nome,
	            	tabela_nome
	            )
	            VALUES (
	            	now(),
	                erro_mensagem,
	                '351570',
	                erro_contexto,
	                'Impulso Previne - Dados Nominais',
	                'lista_nominal_gestantes'
	            );
	            RAISE NOTICE '
	                Algo de errado ocorreu enquanto tentava transmistir os dados entre tabelas. 
					Consulta a tabela `impulsogov_trasmissor.transmissor_historico` para mais informa??es sobre o ocorrido.
	            ';	
END;
$$ LANGUAGE plpgsql;

/* Cria funcaoo procedimental para transferir os dados da view de lista nominal de hipertensos para a tabela importada da impulso */
CREATE OR REPLACE FUNCTION public.transmitir_lista_nominal_hipertensos() returns void
AS $$
DECLARE
    var_view_codigo text;
   	var_tabela_nome text;
  	var_tabela_campos text;
  	var_projuto_nome text;
  	erro_mensagem text;
  	erro_contexto text;
  	check_parametros bool;
  	check_out_transmissao bool;
begin 
	SELECT True, view_codigo,tabela_nome,tabela_campos,projuto_nome into check_parametros,var_view_codigo,var_tabela_nome,var_tabela_campos,var_projuto_nome
	FROM impulsogov_trasmissor.transmissor_parametros tb1
    WHERE tb1.parametro_ativo = TRUE and projuto_nome in ('Impulso Previne - Dados Nominais') and tabela_nome in ('lista_nominal_hipertensos')
    ORDER BY view_versao DESC LIMIT 1;
   	SELECT True into check_out_transmissao
	FROM impulsogov_trasmissor.transmissor_historico
	where projuto_nome = var_projuto_nome and tabela_nome = var_tabela_nome and execucao_data_hora::date = current_date and municipio_id_sus = '351570';
	if check_out_transmissao is not True and check_parametros is True then 
	   	EXECUTE format('DROP MATERIALIZED VIEW IF EXISTS %s CASCADE;',var_tabela_nome);
		EXECUTE format('CREATE MATERIALIZED VIEW %s as %s;',var_tabela_nome,var_view_codigo);
		EXECUTE format('DELETE FROM impulsogov_trasmissor.%s;',var_tabela_nome);
	    EXECUTE format('INSERT INTO impulsogov_trasmissor.%s (%s)
						(SELECT %s FROM %s)',var_tabela_nome,var_tabela_campos,var_tabela_campos,var_tabela_nome);
		EXECUTE format('UPDATE impulsogov_trasmissor.%s
						SET municipio_id_sus = ''351570'';',var_tabela_nome);
		EXECUTE format('INSERT INTO impulsogov_trasmissor.transmissor_historico
						(execucao_data_hora,municipio_id_sus, mensagem, registros,tabela_nome,projuto_nome )
						VALUES(CURRENT_TIMESTAMP,''351570'', ''Trasmiss?o realizada com sucesso'', (select count (*) from %s), ''%s'', ''%s'');',var_tabela_nome,var_tabela_nome,var_projuto_nome);	
	END IF;
	EXCEPTION
	        WHEN OTHERS THEN
	            GET STACKED DIAGNOSTICS
	                erro_mensagem = MESSAGE_TEXT,
	                erro_contexto = PG_EXCEPTION_CONTEXT;
	            INSERT INTO impulsogov_trasmissor.transmissor_historico (
	            	execucao_data_hora,
	            	mensagem,
	            	municipio_id_sus,
	            	erro_contexto,
	            	projuto_nome,
	            	tabela_nome
	            )
	            VALUES (
	            	now(),
	                erro_mensagem,
	                '351570',
	                erro_contexto,
	                'Impulso Previne - Dados Nominais',
	                'lista_nominal_hipertensos'
	            );
	            RAISE NOTICE '
	                Algo de errado ocorreu enquanto tentava transmistir os dados entre tabelas. 
					Consulta a tabela `impulsogov_trasmissor.transmissor_historico` para mais informa??es sobre o ocorrido.
	            ';		
	END;
$$ LANGUAGE plpgsql;

/* Cria funcao procedimental para transferir os dados da view de lista nominal de diabeticos para a tabela importada da impulso */
CREATE OR REPLACE FUNCTION public.transmitir_lista_nominal_diabeticos() returns void
AS $$
DECLARE
    var_view_codigo text;
   	var_tabela_nome text;
  	var_tabela_campos text;
  	var_projuto_nome text;
  	erro_mensagem text;
  	erro_contexto text;
  	check_parametros bool;
  	check_out_transmissao bool;
begin 
	SELECT True, view_codigo,tabela_nome,tabela_campos,projuto_nome into check_parametros,var_view_codigo,var_tabela_nome,var_tabela_campos,var_projuto_nome
	FROM impulsogov_trasmissor.transmissor_parametros tb1
    WHERE tb1.parametro_ativo = TRUE and projuto_nome in ('Impulso Previne - Dados Nominais') and tabela_nome in ('lista_nominal_diabeticos')
    ORDER BY view_versao DESC LIMIT 1;
   	SELECT True into check_out_transmissao
	FROM impulsogov_trasmissor.transmissor_historico
	where projuto_nome = var_projuto_nome and tabela_nome = var_tabela_nome and execucao_data_hora::date = current_date and municipio_id_sus = '351570';
	if check_out_transmissao is not True and check_parametros is True then 
	   	EXECUTE format('DROP MATERIALIZED VIEW IF EXISTS %s CASCADE;',var_tabela_nome);
		EXECUTE format('CREATE MATERIALIZED VIEW %s as %s;',var_tabela_nome,var_view_codigo);
		EXECUTE format('DELETE FROM impulsogov_trasmissor.%s;',var_tabela_nome);
	    EXECUTE format('INSERT INTO impulsogov_trasmissor.%s (%s)
						(SELECT %s FROM %s)',var_tabela_nome,var_tabela_campos,var_tabela_campos,var_tabela_nome);
		EXECUTE format('UPDATE impulsogov_trasmissor.%s
						SET municipio_id_sus = ''351570'';',var_tabela_nome);
		EXECUTE format('INSERT INTO impulsogov_trasmissor.transmissor_historico
						(execucao_data_hora,municipio_id_sus, mensagem, registros,tabela_nome,projuto_nome )
						VALUES(CURRENT_TIMESTAMP,''351570'', ''Trasmiss?o realizada com sucesso'', (select count (*) from %s), ''%s'', ''%s'');',var_tabela_nome,var_tabela_nome,var_projuto_nome);
	END IF;
	EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                erro_mensagem = MESSAGE_TEXT,
                erro_contexto = PG_EXCEPTION_CONTEXT;
            INSERT INTO impulsogov_trasmissor.transmissor_historico (
            	execucao_data_hora,
            	mensagem,
            	municipio_id_sus,
            	erro_contexto,
            	projuto_nome,
            	tabela_nome
            )
            VALUES (
            	now(),
                erro_mensagem,
                '351570',
                erro_contexto,
                'Impulso Previne - Dados Nominais',
                'lista_nominal_diabeticos'
            );
            RAISE NOTICE '
                Algo de errado ocorreu enquanto tentava transmistir os dados entre tabelas. 
				Consulta a tabela `impulsogov_trasmissor.transmissor_historico` para mais informa??es sobre o ocorrido.
            ';		
END;
$$ LANGUAGE plpgsql;

/* Cria funcaoo procedimental para transferir os dados da view de lista nominal de citopatologicos para a tabela importada da impulso */
CREATE OR REPLACE FUNCTION public.transmitir_lista_nominal_citopatologico() returns void
AS $$
DECLARE
    var_view_codigo text;
   	var_tabela_nome text;
  	var_tabela_campos text;
  	var_projuto_nome text;
  	erro_mensagem text;
  	erro_contexto text;
  	check_parametros bool;
  	check_out_transmissao bool;
begin 
	SELECT True, view_codigo,tabela_nome,tabela_campos,projuto_nome into check_parametros,var_view_codigo,var_tabela_nome,var_tabela_campos,var_projuto_nome
	FROM impulsogov_trasmissor.transmissor_parametros tb1
    WHERE tb1.parametro_ativo = TRUE and projuto_nome in ('Impulso Previne - Dados Nominais') and tabela_nome in ('lista_nominal_citopatologico')
    ORDER BY view_versao DESC LIMIT 1;
   	SELECT True into check_out_transmissao
	FROM impulsogov_trasmissor.transmissor_historico
	where projuto_nome = var_projuto_nome and tabela_nome = var_tabela_nome and execucao_data_hora::date = current_date and municipio_id_sus = '351570';
	if check_out_transmissao is not True and check_parametros is True then 
	   	EXECUTE format('DROP MATERIALIZED VIEW IF EXISTS %s CASCADE;',var_tabela_nome);
		EXECUTE format('CREATE MATERIALIZED VIEW %s as %s;',var_tabela_nome,var_view_codigo);
		EXECUTE format('DELETE FROM impulsogov_trasmissor.%s;',var_tabela_nome);
	    EXECUTE format('INSERT INTO impulsogov_trasmissor.%s (%s)
						(SELECT %s FROM %s)',var_tabela_nome,var_tabela_campos,var_tabela_campos,var_tabela_nome);
		EXECUTE format('UPDATE impulsogov_trasmissor.%s
						SET municipio_id_sus = ''351570'';',var_tabela_nome);
		EXECUTE format('INSERT INTO impulsogov_trasmissor.transmissor_historico
						(execucao_data_hora,municipio_id_sus, mensagem, registros,tabela_nome,projuto_nome )
						VALUES(CURRENT_TIMESTAMP,''351570'', ''Trasmiss?o realizada com sucesso'', (select count (*) from %s), ''%s'', ''%s'');',var_tabela_nome,var_tabela_nome,var_projuto_nome);
		
	END IF;
	EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                erro_mensagem = MESSAGE_TEXT,
                erro_contexto = PG_EXCEPTION_CONTEXT;
            INSERT INTO impulsogov_trasmissor.transmissor_historico (
            	execucao_data_hora,
            	mensagem,
            	municipio_id_sus,
            	erro_contexto,
            	projuto_nome,
            	tabela_nome
            )
            VALUES (
            	now(),
                erro_mensagem,
                '351570',
                erro_contexto,
                'Impulso Previne - Dados Nominais',
                'lista_nominal_citopatologico'
            );
            RAISE NOTICE '
                Algo de errado ocorreu enquanto tentava transmistir os dados entre tabelas. 
				Consulta a tabela `impulsogov_trasmissor.transmissor_historico` para mais informa??es sobre o ocorrido.
            ';		
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION public.transmitir_lista_nominal_vacinacao() returns void
AS $$
DECLARE
    var_view_codigo text;
   	var_tabela_nome text;
  	var_tabela_campos text;
  	var_projuto_nome text;
  	erro_mensagem text;
  	erro_contexto text;
  	check_parametros bool;
  	check_out_transmissao bool;
begin 
	SELECT True, view_codigo,tabela_nome,tabela_campos,projuto_nome into check_parametros,var_view_codigo,var_tabela_nome,var_tabela_campos,var_projuto_nome
	FROM impulsogov_trasmissor.transmissor_parametros tb1
    WHERE tb1.parametro_ativo = TRUE and projuto_nome in ('Impulso Previne - Dados Nominais') and tabela_nome in ('lista_nominal_vacinacao')
    ORDER BY view_versao DESC LIMIT 1;
   	SELECT True into check_out_transmissao
	FROM impulsogov_trasmissor.transmissor_historico
	where projuto_nome = var_projuto_nome and tabela_nome = var_tabela_nome and execucao_data_hora::date = current_date and municipio_id_sus = '351570';
	if check_out_transmissao is not True and check_parametros is True then 
	   	EXECUTE format('DROP MATERIALIZED VIEW IF EXISTS %s CASCADE;',var_tabela_nome);
		EXECUTE format('CREATE MATERIALIZED VIEW %s as %s;',var_tabela_nome,var_view_codigo);
		EXECUTE format('DELETE FROM impulsogov_trasmissor.%s;',var_tabela_nome);
	    EXECUTE format('INSERT INTO impulsogov_trasmissor.%s (%s)
						(SELECT %s FROM %s)',var_tabela_nome,var_tabela_campos,var_tabela_campos,var_tabela_nome);
		EXECUTE format('UPDATE impulsogov_trasmissor.%s
						SET municipio_id_sus = ''351570'';',var_tabela_nome);
		EXECUTE format('INSERT INTO impulsogov_trasmissor.transmissor_historico
						(execucao_data_hora,municipio_id_sus, mensagem, registros,tabela_nome,projuto_nome )
						VALUES(CURRENT_TIMESTAMP,''351570'', ''Trasmiss?o realizada com sucesso'', (select count (*) from %s), ''%s'', ''%s'');',var_tabela_nome,var_tabela_nome,var_projuto_nome);

	END IF;
	EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                erro_mensagem = MESSAGE_TEXT,
                erro_contexto = PG_EXCEPTION_CONTEXT;
            INSERT INTO impulsogov_trasmissor.transmissor_historico (
            	execucao_data_hora,
            	mensagem,
            	municipio_id_sus,
            	erro_contexto,
            	projuto_nome,
            	tabela_nome
            )
            VALUES (
            	now(),
                erro_mensagem,
                '351570',
                erro_contexto,
                'Impulso Previne - Dados Nominais',
                'lista_nominal_vacinacao'
            );
            RAISE NOTICE '
                Algo de errado ocorreu enquanto tentava transmistir os dados entre tabelas. 
				Consulta a tabela `impulsogov_trasmissor.transmissor_historico` para mais informa??es sobre o ocorrido.
            ';		
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION public.transmitir_relatorio_mensal_indicadores() returns void
AS $$
DECLARE
    var_view_codigo text;
   	var_tabela_nome text;
  	var_tabela_campos text;
  	var_projuto_nome text;
  	erro_mensagem text;
  	erro_contexto text;
  	check_parametros bool;
  	check_out_transmissao bool;
begin 
	SELECT True, view_codigo,tabela_nome,tabela_campos,projuto_nome into check_parametros,var_view_codigo,var_tabela_nome,var_tabela_campos,var_projuto_nome
	FROM impulsogov_trasmissor.transmissor_parametros tb1
    WHERE tb1.parametro_ativo = TRUE and projuto_nome in ('Impulso Previne - Dados Nominais') and tabela_nome in ('relatorio_mensal_indicadores')
    ORDER BY view_versao DESC LIMIT 1;
   	SELECT True into check_out_transmissao
	FROM impulsogov_trasmissor.transmissor_historico
	where projuto_nome = var_projuto_nome and tabela_nome = var_tabela_nome and execucao_data_hora::date = current_date and municipio_id_sus = '351570';
	if check_out_transmissao is not True and check_parametros is True then 
	   	EXECUTE format('DROP MATERIALIZED VIEW IF EXISTS %s CASCADE;',var_tabela_nome);
		EXECUTE format('CREATE MATERIALIZED VIEW %s as %s;',var_tabela_nome,var_view_codigo);
		EXECUTE format('DELETE FROM impulsogov_trasmissor.%s;',var_tabela_nome);
	    EXECUTE format('INSERT INTO impulsogov_trasmissor.%s (%s)
						(SELECT %s FROM %s)',var_tabela_nome,var_tabela_campos,var_tabela_campos,var_tabela_nome);
		EXECUTE format('UPDATE impulsogov_trasmissor.%s
						SET municipio_id_sus = ''351570'';',var_tabela_nome);
		EXECUTE format('INSERT INTO impulsogov_trasmissor.transmissor_historico
						(execucao_data_hora,municipio_id_sus, mensagem, registros,tabela_nome,projuto_nome )
						VALUES(CURRENT_TIMESTAMP,''351570'', ''Trasmiss?o realizada com sucesso'', (select count (*) from %s), ''%s'', ''%s'');',var_tabela_nome,var_tabela_nome,var_projuto_nome);
	END IF;
	EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                erro_mensagem = MESSAGE_TEXT,
                erro_contexto = PG_EXCEPTION_CONTEXT;
            INSERT INTO impulsogov_trasmissor.transmissor_historico (
            	execucao_data_hora,
            	mensagem,
            	municipio_id_sus,
            	erro_contexto,
            	projuto_nome,
            	tabela_nome
            )
            VALUES (
            	now(),
                erro_mensagem,
                '351570',
                erro_contexto,
                'Impulso Previne - Dados Nominais',
                'relatorio_mensal_indicadores'
            );
            RAISE NOTICE '
                Algo de errado ocorreu enquanto tentava transmistir os dados entre tabelas. 
				Consulta a tabela `impulsogov_trasmissor.transmissor_historico` para mais informa??es sobre o ocorrido.
            ';		
END;
$$ LANGUAGE plpgsql;