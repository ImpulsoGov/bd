CREATE OR REPLACE PROCEDURE impulso_previne_dados_nominais.gestantes_duplicadas_por_erro_data_nascimento()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    res_municipio_id_sus varchar;
	res_gestante_nome varchar;
  	res_gestante_data_de_nascimento date;
	res_gestante_documento_cpf varchar;
	res_gestante_documento_cns varchar;
	res_gestante_dum date;
  	res_periodo_data_transmissao_ultima date;
  	res_gestante_dpp date;
  	res_equipe_ine varchar;
  	res_equipe_nome varchar;
  	res_estabelecimento_cnes varchar;
  	res_estabelecimento_nome varchar;
  	res_acs_nome varchar;
BEGIN
	TRUNCATE impulso_previne_dados_nominais.lista_nominal_gestantes_duplicadas_por_erro_data_nascimento;
	FOR res_municipio_id_sus,
		res_gestante_nome,
		res_gestante_data_de_nascimento,
		res_gestante_documento_cpf,
		res_gestante_documento_cns,
		res_gestante_dum,
		res_periodo_data_transmissao_ultima,
		res_gestante_dpp,
	  	res_equipe_ine,
	  	res_equipe_nome,
	  	res_estabelecimento_cnes,
	  	res_estabelecimento_nome,
	  	res_acs_nome IN (
		SELECT 
			municipio_id_sus,
			gestante_nome,
			gestante_data_de_nascimento,
			gestante_documento_cpf,
			gestante_documento_cns,
			gestacao_data_dum ,
			criacao_data  as periodo_data_transmissao_ultima,
			gestacao_data_dpp ,
			equipe_ine_cad_individual as equipe_ine,
			equipe_nome_cad_individual as equipe_nome,
			estabelecimento_cnes_cad_indivual as estabelecimento_cnes,
			estabelecimento_nome_cad_individual as estabelecimento_nome,
			acs_cad_individual as acs_nome
		FROM impulso_previne_dados_nominais.lista_nominal_gestantes_unificada tb1
		where gestante_nome in (SELECT gestante_nome FROM impulso_previne_dados_nominais.lista_nominal_gestantes_unificada 
							WHERE 
								tb1.municipio_id_sus = municipio_id_sus 
								and tb1.gestante_nome = gestante_nome 
								AND tb1.gestante_data_de_nascimento != gestante_data_de_nascimento
								and (tb1.gestante_documento_cpf = gestante_documento_cpf 
									or tb1.gestante_documento_cns = gestante_documento_cns
									or tb1.gestacao_data_dum = gestacao_data_dum)
							GROUP BY gestante_nome,municipio_id_sus,gestante_data_de_nascimento)				
		ORDER BY gestante_nome,municipio_id_sus,gestante_data_de_nascimento desc
    ) 
	LOOP
		INSERT INTO impulso_previne_dados_nominais.lista_nominal_gestantes_duplicadas_por_erro_data_nascimento
		(municipio_id_sus, gestante_nome, gestante_data_de_nascimento,gestante_documento_cpf,
		gestante_documento_cns,periodo_data_transmissao,gestante_dum,gestante_dpp,equipe_ine,equipe_nome,
		estabelecimento_cnes,estabelecimento_nome,acs_nome)
		VALUES( 
			res_municipio_id_sus,
			res_gestante_nome,
			res_gestante_data_de_nascimento,
			res_gestante_documento_cpf,
			res_gestante_documento_cns,
			res_periodo_data_transmissao_ultima,
			res_gestante_dum,
			res_gestante_dpp,
		  	res_equipe_ine,
		  	res_equipe_nome,
		  	res_estabelecimento_cnes,
		  	res_estabelecimento_nome,
		  	res_acs_nome);
	END LOOP;
END;
$procedure$
;
