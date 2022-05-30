SELECT DISTINCT
	municipio_id,
	estabelecimento_nome,
	usuario_id,
	first_value(usuario_abertura_raas) over (
		partitiON BY
			municipio_id,
			estabelecimento_nome,
			usuario_id
		ORDER BY
			municipio_id asc, 
			estabelecimento_nome asc,
			usuario_id asc,
			raas_competencia_inicio DESC
	) AS usuario_abertura_raas,
	first_value(atividade_descricao) over w AS atividade_descricao,
	first_value(cid_descricao) over w AS cid_descricao,
	first_value(cid_grupo_descricao_curta) over w AS cid_grupo_descricao_curta,
	first_value(encaminhamento_origem_descricao) over w AS encaminhamento_origem_descricao,
	first_value(estabelecimento_referencia_nome) over w AS estabelecimento_referencia_nome,
	first_value(usuario_faixa_etaria) over w AS usuario_faixa_etaria,
	first_value(usuario_raca_cor) over w AS usuario_raca_cor,
	first_value(usuario_sexo) over w AS usuario_sexo,
	first_value(usuario_situacao_rua) over w AS usuario_situacao_rua,
	first_value(usuario_substancias_abusa) over w AS usuario_substancias_abusa
FROM saude_mental.painel_raas
WHERE municipio_id = '280030'
window w AS (
	partitiON BY
		municipio_id,
		estabelecimento_nome,
		usuario_id
	ORDER BY
		municipio_id asc, 
		estabelecimento_nome asc,
		usuario_id asc,
		competencia_realizacao DESC
)
limit 10
;

DROP MATERIALIZED VIEW IF EXISTS saude_mental.usuarios_raas_resumo;
CREATE MATERIALIZED VIEW saude_mental.usuarios_raas_resumo AS (
SELECT
	municipio_id,
	estabelecimento_nome,
	usuario_id,
	min(usuario_abertura_raas) AS usuario_abertura_raas,
	min(raas_competencia_inicio) AS raas_competencia_inicio,
	min(competencia_realizacao) AS competencia_primeiro_procedimento,
	(array_agg(atividade_descricao ORDER BY competencia_realizacao))[1] AS atividade_descricao,
	(array_agg(cid_descricao ORDER BY competencia_realizacao))[1] AS cid_descricao,
	(array_agg(cid_grupo_descricao_curta ORDER BY competencia_realizacao))[1] AS cid_grupo_descricao_curta,
	(array_agg(encaminhamento_origem_descricao ORDER BY competencia_realizacao))[1] AS encaminhamento_origem_descricao,
	(array_agg(estabelecimento_referencia_nome ORDER BY competencia_realizacao))[1] AS estabelecimento_referencia_nome,
	(array_agg(usuario_faixa_etaria ORDER BY competencia_realizacao))[1] AS usuario_faixa_etaria,
	(array_agg(usuario_raca_cor ORDER BY competencia_realizacao))[1] AS usuario_raca_cor,
	(array_agg(usuario_sexo ORDER BY competencia_realizacao))[1] AS usuario_sexo,
	(array_agg(usuario_situacao_rua ORDER BY competencia_realizacao))[1] AS usuario_situacao_rua,
	(array_agg(usuario_substancias_abusa ORDER BY competencia_realizacao))[1] AS usuario_substancias_abusa
FROM saude_mental.painel_raas
--WHERE municipio_id = '280030'
GROUP BY (
	municipio_id,
	estabelecimento_nome,
	usuario_id
)
);
GRANT SELECT 
ON saude_mental.usuarios_raas_resumo
TO
	analistas_saude_mental,
	bernardo_saude_mental,
	painel_saude_mental
;



