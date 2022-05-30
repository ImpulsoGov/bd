SELECT 
	municipio.uf_sigla,
	periodo.data_inicio,
	count(procedimento.id)
FROM dados_publicos.siasus_procedimentos_ambulatoriais procedimento
LEFT JOIN listas_de_codigos.periodos periodo
ON procedimento.periodo_id = periodo.id
LEFT JOIN listas_de_codigos.municipios municipio
ON procedimento.unidade_geografica_id = municipio.id
GROUP BY municipio.uf_sigla, periodo.data_inicio;

SELECT 
	municipio.uf_sigla,
	periodo.data_inicio,
	sum(historico.quantidade_registros),
	count(DISTINCT historico.capturado_em)
FROM configuracoes.capturas_historico_consolidado historico
LEFT JOIN listas_de_codigos.periodos periodo
ON historico.periodo_id = periodo.id
LEFT JOIN listas_de_codigos.municipios municipio
ON historico.unidade_geografica_id = municipio.id
WHERE historico.operacao_id = 'f2a62b56-932a-431d-aee5-e3c0af33914f'
GROUP BY municipio.uf_sigla, periodo.data_inicio;

SELECT 
	periodo.data_inicio,
	ag.*
FROM configuracoes.capturas_agendamentos ag
LEFT JOIN listas_de_codigos.periodos periodo
ON ag.periodo_id = periodo.id
WHERE ag.operacao_id = 'f2a62b56-932a-431d-aee5-e3c0af33914f';