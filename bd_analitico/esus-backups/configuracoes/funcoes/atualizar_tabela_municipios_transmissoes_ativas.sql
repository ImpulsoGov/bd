CREATE OR REPLACE PROCEDURE configuracoes.atualizar_tabela_municipios_transmissoes_ativas()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    r record;
begin
	TRUNCATE configuracoes.municipios_transmissoes_ativas CASCADE;
	FOR r IN (
	SELECT table_name, table_schema  
	FROM information_schema.tables
	WHERE table_schema  like 'dados_nominais_%' and 
	table_name like any(array['%lista_nominal_gestantes%','%lista_nominal_hipertensos%','%lista_nominal_diabeticos%','%relatorio_mensal_indicadores%', '%lista_nominal_citopatologico%','%lista_nominal_vacinacao%']) 
    ) 
	loop
		EXECUTE format('INSERT INTO configuracoes.municipios_transmissoes_ativas
 		(SELECT ''%s'' as table_name, ''%s'' as table_schema, tb1.municipio_id_sus, min(tb2.execucao_data_hora) as primeira_transmissao 
		from %s.%s tb1
		join configuracoes.transmissor_historico tb2 on tb1.municipio_id_sus=tb2.municipio_id_sus
		where tb2.tabela_nome = ''%s''
		group by tb1.municipio_id_sus)',r.table_name,r.table_schema,r.table_schema,r.table_name,r.table_name);
	end loop;
END;	
$procedure$
;
