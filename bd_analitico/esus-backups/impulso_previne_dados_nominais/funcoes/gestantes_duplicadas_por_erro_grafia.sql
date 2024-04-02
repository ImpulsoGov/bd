CREATE OR REPLACE PROCEDURE impulso_previne_dados_nominais.gestantes_duplicadas_por_erro_grafia()
 LANGUAGE plpgsql
AS $procedure$
DECLARE
    res_municipio_id_sus varchar;
	res_gestante_nome varchar;
  	res_gestante_data_de_nascimento date;
  	rel_periodo_data_transmissao date;
  	res_gestante_documento_cpf varchar;
  	res_gestante_documento_cns varchar;
  	res_gestante_dum date;
  	res_gestante_dpp date;
  	res_equipe_ine varchar;
  	res_equipe_nome varchar;
  	res_estabelecimento_cnes varchar;
  	res_estabelecimento_nome varchar;
  	res_acs_nome varchar;
BEGIN
	TRUNCATE impulso_previne_dados_nominais.lista_nominal_gestantes_duplicadas_por_erro_grafia;
	FOR res_municipio_id_sus,
		res_gestante_nome,
		res_gestante_data_de_nascimento,
		rel_periodo_data_transmissao,
		res_gestante_documento_cpf,
		res_gestante_documento_cns,
		res_gestante_dum,
		res_gestante_dpp,
	  	res_equipe_ine,
	  	res_equipe_nome,
	  	res_estabelecimento_cnes,
	  	res_estabelecimento_nome,
	  	res_acs_nome
		IN (
		SELECT 
			tb2.municipio_id_sus,
			tb2.gestante_nome,
			tb2.gestante_data_de_nascimento,
			tb2.criacao_data as periodo_data_transmissao,
			tb2.gestante_documento_cpf,
			tb2.gestante_documento_cns,
			tb2.gestacao_data_dum ,
			tb2.gestacao_data_dpp,
			tb2.equipe_ine_cad_individual as equipe_ine,
			tb2.equipe_nome_cad_individual as equipe_nome,
			tb2.estabelecimento_cnes_cad_indivual as estabelecimento_cnes,
			tb2.estabelecimento_nome_cad_individual as estabelecimento_nome,
			tb2.acs_cad_individual as acs_nome
		FROM impulso_previne_dados_nominais.lista_nominal_gestantes_unificada tb2
    ) 
	loop
		INSERT INTO impulso_previne_dados_nominais.lista_nominal_gestantes_duplicadas_por_erro_grafia
		(municipio_id_sus, gestante_nome, gestante_data_de_nascimento,gestante_documento_cpf,
		gestante_documento_cns,periodo_data_transmissao,gestante_dpp,equipe_ine,equipe_nome,estabelecimento_cnes,estabelecimento_nome,acs_nome)
		(SELECT 
			municipio_id_sus,
			gestante_nome,
			gestante_data_de_nascimento,
			gestante_documento_cpf,
			gestante_documento_cns,
			criacao_data as periodo_data_transmissao,
			gestacao_data_dpp,
			equipe_ine_cad_individual as equipe_ine,
			equipe_nome_cad_individual as equipe_nome,
			estabelecimento_cnes_cad_indivual as estabelecimento_cnes,
			estabelecimento_nome_cad_individual as estabelecimento_nome,
			acs_cad_individual as acs_nome
		FROM impulso_previne_dados_nominais.lista_nominal_gestantes_unificada tb1
		WHERE res_municipio_id_sus = municipio_id_sus 
			AND res_gestante_data_de_nascimento = gestante_data_de_nascimento
			--AND rel_periodo_data_transmissao = criacao_data
			AND impulso_previne_dados_nominais.levenshtein(res_gestante_nome,gestante_nome) > 0
			AND impulso_previne_dados_nominais.levenshtein(res_gestante_nome,tb1.gestante_nome) < 15
			AND (
				SUBSTRING(gestante_nome FROM 1 FOR 1) = SUBSTRING(res_gestante_nome FROM 1 FOR 1) 
				and 
				(
				res_gestante_documento_cpf=gestante_documento_cpf or gestante_documento_cns=res_gestante_documento_cns or gestacao_data_dum = res_gestante_dum or gestacao_data_dpp = res_gestante_dpp
				or (SUBSTRING(gestante_nome FROM 1 FOR 3) = SUBSTRING(res_gestante_nome FROM 1 FOR 3)))
				)
		ORDER BY municipio_id_sus,gestante_nome,gestante_data_de_nascimento,criacao_data desc
	limit 1);
	END LOOP;
END;
$procedure$
;
