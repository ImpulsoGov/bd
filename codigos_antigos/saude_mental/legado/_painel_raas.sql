DROP MATERIALIZED VIEW saude_mental.painel_raas CASCADE;

CREATE MATERIALIZED VIEW saude_mental.painel_raas AS 
SELECT
	ps.unidade_geografica_id_sus AS municipio_id,
	saude_mental.localizar_data(ps.realizacao_periodo_data_inicio) AS competencia_realizacao,
	ps.usuario_cns_criptografado AS usuario_id,
	saude_mental.localizar_data(ps.raas_data_inicio) AS usuario_abertura_raas,
	saude_mental.classificar_binarios(ps.usuario_situacao_rua) AS usuario_situacao_rua,
	ps.quantidade_apresentada AS quantidade_registrada,
	ps.quantidade_aprovada AS quantidade_aprovada,
	servico.servico_descricao AS servico_descricao,
	servico_classificacao.servico_classificacao_descricao AS servico_classificacao_descricao,
	estabelecimento_tipo.estabelecimento_tipo_descricao AS estabelecimento_tipo_descricao,
	saude_mental.limpar_nome_estabelecimento(estabelecimento.estabelecimento_nome) AS estabelecimento_nome,
	estabelecimento.estabelecimento_endereco_logradouro AS estabelecimento_endereco_logradouro,
	estabelecimento.estabelecimento_endereco_numero AS estabelecimento_endereco_numero,
	estabelecimento.estabelecimento_endereco_cep AS estabelecimento_endereco_cep,
	estabelecimento.estabelecimento_latitude AS estabelecimento_latitude,
	estabelecimento.estabelecimento_longitude AS estabelecimento_longitude,
	estabelecimento_atividade.atividade_descricao AS atividade_descricao,
	coalesce(esf_estabelecimento.estabelecimento_nome, 'Sem informação') AS estabelecimento_referencia_nome,
	esf_estabelecimento.estabelecimento_endereco_logradouro AS estabelecimento_referencia_endereco_logradouro,
	esf_estabelecimento.estabelecimento_endereco_numero AS estabelecimento_referencia_endereco_numero,
	esf_estabelecimento.estabelecimento_endereco_cep AS estabelecimento_referencia_endereco_cep,
	esf_estabelecimento.estabelecimento_latitude AS estabelecimento_referencia_latitude,
	esf_estabelecimento.estabelecimento_longitude AS estabelecimento_referencia_longitude,
	procedimento.procedimento_nome AS procedimento_nome,
	sexo.nome AS usuario_sexo,
	raca_cor.nome AS usuario_raca_cor,
	saude_mental.classificar_faixa_etaria(ps.usuario_data_nascimento, ps.realizacao_periodo_data_inicio) AS usuario_faixa_etaria,
	cid.cid_descricao AS cid_descricao,
	cid.cid_grupo_descricao_longa AS cid_grupo_descricao_longa,
	cid.cid_grupo_descricao_curta AS cid_grupo_descricao_curta,
	saude_mental.localizar_data(date_trunc('month', ps.raas_data_inicio)::date) AS raas_competencia_inicio,
	saude_mental.classificar_tempo_no_servico(ps.raas_data_inicio, ps.realizacao_periodo_data_inicio) AS usuario_tempo_no_servico,
	saude_mental.classificar_binarios((date_trunc('month', ps.raas_data_inicio)::date = ps.realizacao_periodo_data_inicio)::bool) AS usuario_novo,
	saude_mental.classificar_binarios(ps.usuario_abuso_substancias) AS usuario_substancias_abusa,
	procedencia.nome AS encaminhamento_origem_descricao,
	local_realizacao.nome AS procedimento_local,
	saude_mental.classificar_linha_idade(estabelecimento.estabelecimento_nome) AS estabelecimento_linha_publico,
	saude_mental.classificar_linha_perfil(estabelecimento.estabelecimento_nome) AS estabelecimento_linha_perfil
FROM dados_publicos.siasus_raas_psicossocial_disseminacao ps
LEFT JOIN listas_de_codigos.sexos sexo
	ON ps.usuario_sexo_id_sigtap = sexo.id_sigtap
LEFT JOIN listas_de_codigos.racas_cores raca_cor
	ON ps.usuario_raca_cor_id_siasus = raca_cor.id_siasus
LEFT JOIN listas_de_codigos.procedencias procedencia
	ON ps.procedencia_id_siasus = procedencia.id_siasus
LEFT JOIN listas_de_codigos.locais_realizacao local_realizacao
	ON ps.local_realizacao_id_siasus = local_realizacao.id_siasus
LEFT JOIN saude_mental.cnes_estabelecimentos estabelecimento
	ON ps.estabelecimento_id_cnes = estabelecimento.estabelecimento_id
LEFT JOIN saude_mental.estabelecimentos_tipos estabelecimento_tipo
	ON ps.estabelecimento_tipo_id_sigtap = estabelecimento_tipo.estabelecimento_tipo_id 
LEFT JOIN saude_mental.estabelecimentos_atividades estabelecimento_atividade
	ON estabelecimento.estabelecimento_atividade_id = estabelecimento_atividade.atividade_id
LEFT JOIN saude_mental.sigtap_procedimentos procedimento
	ON ps.procedimento_id_sigtap = procedimento.procedimento_id
LEFT JOIN saude_mental.servicos servico
	ON ps.servico_id_sigtap = servico.servico_id 
LEFT JOIN saude_mental.servicos_classificacao servico_classificacao
	ON (
		ps.servico_id_sigtap = servico_classificacao.servico_id
		AND ps.servico_classificacao_id_sigtap = servico_classificacao.servico_classificacao_id
	)
LEFT JOIN saude_mental.cnes_estabelecimentos esf_estabelecimento
	ON ps.esf_estabelecimento_id_cnes = esf_estabelecimento.estabelecimento_id
LEFT JOIN saude_mental.cids cid
	ON ps.condicao_principal_id_cid10 = cid.cid_id
;

CREATE INDEX painel_raas_municipio_id_idx
ON saude_mental.painel_raas (municipio_id);

CREATE INDEX painel_raas_competencia_realizacao_idx
ON saude_mental.painel_raas (competencia_realizacao);

CREATE INDEX painel_raas_usuario_id_idx
ON saude_mental.painel_raas (usuario_id);

CREATE INDEX painel_raas_estabelecimento_nome_idx
ON saude_mental.painel_raas (estabelecimento_nome);

CREATE INDEX painel_raas_procedimento_nome_idx
ON saude_mental.painel_raas (procedimento_nome);


GRANT SELECT
ON saude_mental.painel_raas
TO 
    analistas_saude_mental,
    bernardo_saude_mental,
    painel_saude_mental
;

