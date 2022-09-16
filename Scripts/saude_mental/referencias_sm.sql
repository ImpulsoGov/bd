/**************************************************************************

		PRODUÇÃO DAS REFERÊNCIAS AMBULATORIAIS EM SAÚDE MENTAL


 **************************************************************************/


CREATE OR REPLACE FUNCTION listas_de_codigos.proximo_dia_util(data date)
RETURNS date
LANGUAGE plpgsql
AS $function$
	begin
		data := data + '1 day'::interval;
		-- TODO: pular feriados!
		while (EXTRACT(dow FROM data) = 0) OR (EXTRACT(dow FROM data) = 6)loop
			data := data + '1 day'::interval;
		END loop;
		return data;
	END;
$function$;
COMMENT ON FUNCTION listas_de_codigos.proximo_dia_util 
IS 'Para uma data qualquer, obtém a próxima data que seja um dia de semana.';


CREATE OR REPLACE FUNCTION 
    listas_de_codigos.datas_diferenca_dias_uteis(
        data_inicio date,
        data_fim date
    )
RETURNS int
LANGUAGE plpgsql
AS $function$
	declare
		data date;
		i int;
	begin
		data := data_inicio;
		i := 0;
		-- TODO: pular feriados!
		while (data < data_fim) loop
			data := listas_de_codigos.proximo_dia_util(data);
			i := i + 1;
		END loop;
		return i;
	END;
$function$;
COMMENT ON FUNCTION listas_de_codigos.datas_diferenca_dias_uteis 
IS 
'Calcula a diferença entre um par de datas como o número de dias úteis no 
intervalo. Atualmente, considera como dias úteis todos os dias de segunda a 
sexta-feira (feriados ainda não são levados em conta).'
;


CREATE INDEX IF NOT EXISTS 
    siasus_bpa_i_disseminacao_ocupacao_id_cbo_idx
ON dados_publicos.siasus_bpa_i_disseminacao (
    profissional_ocupacao_id_cbo
);

DROP MATERIALIZED VIEW IF EXISTS 
    saude_mental._procedimentos_referencias_ambulatoriais 
CASCADE;
CREATE MATERIALIZED VIEW saude_mental._procedimentos_referencias_ambulatoriais
AS
SELECT 
    bpa_i.*,
    saude_mental.classificar_faixa_etaria(
        bpa_i.usuario_data_nascimento,
        bpa_i.realizacao_periodo_data_inicio
    ) AS usuario_faixa_etaria
FROM dados_publicos.siasus_bpa_i_disseminacao bpa_i
INNER JOIN
    saude_mental.estabelecimentos_referencia_ambulatorial
    estabelecimentos_referencia
ON 
    bpa_i.estabelecimento_id_cnes
    = estabelecimentos_referencia.estabelecimento_id_cnes
WHERE bpa_i.profissional_ocupacao_id_cbo IN (
	   '251510',  -- psicólogos
	   '225133'  -- psiquiatras
	)
WITH NO DATA;
CREATE INDEX IF NOT EXISTS
    _procedimentos_referencias_faixa_etaria_x_ug_x_competencia_idx
ON saude_mental._procedimentos_referencias_ambulatoriais (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    realizacao_periodo_data_inicio,
    usuario_faixa_etaria
);
CREATE INDEX IF NOT EXISTS
    _procedimentos_referencias_profissional_x_ug_x_competencia_idx
ON saude_mental._procedimentos_referencias_ambulatoriais (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    realizacao_periodo_data_inicio,
    profissional_cns
);


-- Legado; preferir a view referencias_usuarios_perfil
DROP MATERIALIZED VIEW IF EXISTS 
    saude_mental.usuarios_referencias_por_faixa_etaria_ultimo_mes
CASCADE;
CREATE MATERIALIZED VIEW 
    saude_mental.usuarios_referencias_por_faixa_etaria_ultimo_mes
AS
WITH procedimentos_por_faixa_etaria AS (
	SELECT
		unidade_geografica_id,
		unidade_geografica_id_sus,
		realizacao_periodo_data_inicio AS competencia,
		listas_de_codigos.nome_mes(realizacao_periodo_data_inicio) AS nome_mes,
		usuario_faixa_etaria,
		count(DISTINCT usuario_cns_criptografado) FILTER (
            WHERE quantidade_apresentada > 0
		) AS usuarios_unicos_mes
	FROM saude_mental._procedimentos_referencias_ambulatoriais
	GROUP BY
		unidade_geografica_id,
		unidade_geografica_id_sus,
		realizacao_periodo_data_inicio,
		usuario_faixa_etaria
)
SELECT
	DISTINCT ON (
		unidade_geografica_id,
		unidade_geografica_id_sus,
		usuario_faixa_etaria
	)
	*
FROM procedimentos_por_faixa_etaria
ORDER BY 
    unidade_geografica_id,
	unidade_geografica_id_sus,
	usuario_faixa_etaria,
	competencia DESC
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    usuarios_referencias_por_faixa_etaria_ultimo_mes_un
ON saude_mental.usuarios_referencias_por_faixa_etaria_ultimo_mes (
        unidade_geografica_id,
        unidade_geografica_id_sus,
        usuario_faixa_etaria
);


DROP MATERIALIZED VIEW IF EXISTS
    saude_mental.referencias_usuarios_perfil
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.referencias_usuarios_perfil
AS
SELECT
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    realizacao_periodo_data_inicio AS competencia,
    listas_de_codigos.nome_mes(realizacao_periodo_data_inicio) AS nome_mes,
    usuario_faixa_etaria,
    sexo.nome AS usuario_sexo,
    (
        CASE WHEN (condicao_principal_id_cid10 IS NULL) THEN 'Sem informação'
        ELSE coalesce(cid.cid_grupo_descricao_curta, 'Outras condições')
        END
    ) AS cid_grupo_descricao_curta,
    count(DISTINCT usuario_cns_criptografado) FILTER (
        WHERE quantidade_apresentada > 0
    ) AS usuarios_unicos_mes
FROM saude_mental._procedimentos_referencias_ambulatoriais procedimento
FULL JOIN listas_de_codigos.sexos sexo
ON procedimento.usuario_sexo_id_sigtap = sexo.id_sigtap
-- TODO: substituir por tabela de CIDs no schema listas_de_codigos
LEFT JOIN saude_mental.cids cid
ON procedimento.condicao_principal_id_cid10 = cid.cid_id
GROUP BY
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    realizacao_periodo_data_inicio,
    usuario_faixa_etaria,
    sexo.nome,
    (condicao_principal_id_cid10 IS NULL),
    cid.cid_grupo_descricao_curta
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS
    referencias_perfil_usuarios_un
ON saude_mental.referencias_perfil_usuarios (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    competencia,
    usuario_faixa_etaria,
    usuario_sexo,
    cid_grupo_descricao_curta
);



DROP MATERIALIZED VIEW IF EXISTS 
    saude_mental.procedimentos_referencias_resumo
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.procedimentos_referencias_resumo
AS
WITH 
disponibilidade_por_ocupacao_por_estabelecimento AS (
    SELECT
        unidade_geografica_id,
        periodo_id,
        estabelecimento_id_cnes,
        ocupacao_id_cbo,
        sum(
            atendimento_carga_ambulatorial
            + atendimento_carga_outras
        ) / 5 AS horas_disponibilidade_diaria
    FROM dados_publicos.cnes_vinculos_disseminacao vinculos_profissionais
    WHERE atendimento_sus
    GROUP BY 
        unidade_geografica_id,
        periodo_id,
        estabelecimento_id_cnes,
        ocupacao_id_cbo
),
procedimentos_realizados_por_ocupacao_por_estabelecimento AS (
	SELECT
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		estabelecimento_id_cnes,
		profissional_ocupacao_id_cbo AS ocupacao_id_cbo,
		sum(quantidade_apresentada) AS procedimentos_realizados
	FROM saude_mental._procedimentos_referencias_ambulatoriais procedimento
	GROUP BY 
		unidade_geografica_id,
		unidade_geografica_id_sus,
		periodo_id,
		estabelecimento_id_cnes,
		profissional_ocupacao_id_cbo
),
procedimentos_x_disponibilidade AS (
    SELECT
        procedimentos.*,
        disponibilidade.horas_disponibilidade_diaria
    FROM procedimentos_realizados_por_ocupacao_por_estabelecimento procedimentos
    LEFT JOIN disponibilidade_por_ocupacao_por_estabelecimento disponibilidade
    USING (
        unidade_geografica_id,
        periodo_id,
        estabelecimento_id_cnes,
        ocupacao_id_cbo
    )
),
todos_estabelecimentos AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        '0000000' AS estabelecimento_id_cnes,
        ocupacao_id_cbo,
        sum(procedimentos_realizados) AS procedimentos_realizados,
        sum(horas_disponibilidade_diaria) AS horas_disponibilidade_diaria
    FROM procedimentos_x_disponibilidade
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        ocupacao_id_cbo
),
todas_ocupacoes AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        estabelecimento_id_cnes,
        '000000' AS ocupacao_id_cbo,
        sum(procedimentos_realizados) AS procedimentos_realizados,
        sum(horas_disponibilidade_diaria) AS horas_disponibilidade_diaria
    FROM procedimentos_x_disponibilidade
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        estabelecimento_id_cnes
),
todos_estabelecimentos_todas_ocupacoes AS (
    SELECT
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id,
        '0000000'  AS estabelecimento_id_cnes,
        '000000' AS ocupacao_id_cbo,
        sum(procedimentos_realizados) AS procedimentos_realizados,
        sum(horas_disponibilidade_diaria) AS horas_disponibilidade_diaria
    FROM procedimentos_x_disponibilidade
    GROUP BY
        unidade_geografica_id,
        unidade_geografica_id_sus,
        periodo_id
),
procedimentos_x_disponibilidade_com_totais AS (
    SELECT * FROM procedimentos_x_disponibilidade
    UNION
    SELECT * FROM todos_estabelecimentos
    UNION
    SELECT * FROM todas_ocupacoes
    UNION
    SELECT * FROM todos_estabelecimentos_todas_ocupacoes
),
comparacao_entre_meses AS (
    SELECT
		coalesce(
            competencia_atual.unidade_geografica_id,
            competencia_anterior.unidade_geografica_id
        ) AS unidade_geografica_id,
		coalesce(
            competencia_atual.unidade_geografica_id_sus,
            competencia_anterior.unidade_geografica_id_sus
        ) AS unidade_geografica_id_sus,
		sucessao.periodo_data_inicio AS competencia,
		sucessao.periodo_id,
        coalesce(
            competencia_atual.estabelecimento_id_cnes,
            competencia_anterior.estabelecimento_id_cnes
        ) AS estabelecimento_id_cnes,
		coalesce(
            competencia_atual.ocupacao_id_cbo,
            competencia_anterior.ocupacao_id_cbo
        ) AS ocupacao_id_cbo,
		coalesce(
            competencia_atual.procedimentos_realizados,
            0
        ) AS procedimentos_realizados,
		coalesce(
            competencia_anterior.procedimentos_realizados,
            0
        ) AS procedimentos_realizados_anterior,
		round(
			competencia_atual.procedimentos_realizados::numeric 
			/ nullif(
				competencia_atual.horas_disponibilidade_diaria
				* listas_de_codigos.datas_diferenca_dias_uteis(
				    sucessao.periodo_data_inicio,
				    sucessao.proximo_periodo_data_inicio
				),
				0
			)::numeric,
			2
		) AS procedimentos_por_hora,
		round(
			competencia_anterior.procedimentos_realizados::numeric 
			/ nullif(
				competencia_anterior.horas_disponibilidade_diaria
				* listas_de_codigos.datas_diferenca_dias_uteis(
				    sucessao.ultimo_periodo_data_inicio,
                    sucessao.periodo_data_inicio
				),
				0
			)::numeric,
			2
		) AS procedimentos_por_hora_anterior
    FROM procedimentos_x_disponibilidade_com_totais competencia_atual
    LEFT JOIN listas_de_codigos.periodos_sucessao sucessao
    	ON
    	    sucessao.periodo_tipo = 'Mensal'
    	AND competencia_atual.periodo_id = sucessao.periodo_id
    FULL JOIN procedimentos_x_disponibilidade_com_totais competencia_anterior 
    	ON
    	    sucessao.ultimo_periodo_id = competencia_anterior.periodo_id
    	AND competencia_atual.unidade_geografica_id 
    	    = competencia_anterior.unidade_geografica_id
        AND competencia_atual.estabelecimento_id_cnes 
            = competencia_anterior.estabelecimento_id_cnes
    	AND competencia_atual.ocupacao_id_cbo 
    	    = competencia_anterior.ocupacao_id_cbo
    WHERE competencia_atual.procedimentos_realizados IS NOT NULL 
)
SELECT
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    listas_de_codigos.nome_mes(competencia) AS nome_mes,
    competencia,
    coalesce(
        estabelecimento.nome_curto,
        estabelecimento.nome,
        'Todos'
    ) AS estabelecimento,
    coalesce(ocupacao.ocupacao_descricao, 'Todas') AS ocupacao,
    procedimentos_realizados,
    procedimentos_realizados_anterior,
    procedimentos_por_hora,
    procedimentos_por_hora_anterior,
    round(
        100 * procedimentos_por_hora::numeric
        / nullif(procedimentos_por_hora_anterior, 0),
        1
    ) - 100 AS dif_procedimentos_por_hora_anterior,
    (
        coalesce(procedimentos_realizados, 0)
        - coalesce(procedimentos_realizados_anterior, 0)
    ) AS dif_procedimentos_realizados_anterior
FROM comparacao_entre_meses
LEFT JOIN saude_mental.ocupacoes ocupacao
-- TODO: usar versão do schema `listas_de_codigos`
ON comparacao_entre_meses.ocupacao_id_cbo = ocupacao.ocupacao_id
LEFT JOIN listas_de_codigos.estabelecimentos AS estabelecimento
ON comparacao_entre_meses.estabelecimento_id_cnes = estabelecimento.id_scnes
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS 
    procedimentos_referencias_resumo_un 
ON saude_mental.procedimentos_referencias_resumo (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    estabelecimento,
    ocupacao,
    competencia DESC,
    periodo_id
);



DROP MATERIALIZED VIEW IF EXISTS 
    saude_mental.procedimentos_referencias_resumo_ultimo_mes
CASCADE;
CREATE MATERIALIZED VIEW 
    saude_mental.procedimentos_referencias_resumo_ultimo_mes
AS
SELECT
	DISTINCT ON (
		unidade_geografica_id,
		unidade_geografica_id_sus,
		estabelecimento,
		ocupacao
	)
	*
FROM saude_mental.procedimentos_referencias_resumo
ORDER BY
	unidade_geografica_id,
	unidade_geografica_id_sus,
	estabelecimento,
	ocupacao,
	competencia DESC
WITH NO DATA;
CREATE UNIQUE INDEX IF NOT EXISTS 
    procedimentos_referencias_resumo_ultimo_mes_un
ON saude_mental.procedimentos_referencias_resumo_ultimo_mes (
    unidade_geografica_id,
    unidade_geografica_id_sus,
    estabelecimento,
    ocupacao
);


DROP MATERIALIZED VIEW IF EXISTS 
    saude_mental.referencias_procedimentos_por_profissional_por_hora_ultimo_mes
CASCADE;
CREATE MATERIALIZED VIEW
    saude_mental.referencias_procedimentos_por_profissional_por_hora_ultimo_mes
AS
WITH 
procedimentos_referencias AS (
	SELECT
		procedimentos.unidade_geografica_id,
		procedimentos.unidade_geografica_id_sus,
		ultima_competencia_disponivel.periodo_id,
		profissional_ocupacao_id_cbo AS ocupacao_id_cbo,
		profissional_cns AS profissional_id_cns,
		sum(quantidade_apresentada) AS procedimentos_realizados
	FROM saude_mental._procedimentos_referencias_ambulatoriais procedimentos
    INNER JOIN 
        saude_mental._bpa_i_caps_ultima_competencia_disponivel
        ultima_competencia_disponivel
    USING (
        unidade_geografica_id,
        periodo_id
    )
	WHERE profissional_cns IS NOT NULL
	GROUP BY
		procedimentos.unidade_geografica_id,
		procedimentos.unidade_geografica_id_sus,
		ultima_competencia_disponivel.periodo_id,
		profissional_ocupacao_id_cbo,
		profissional_cns
),
horas_contratadas AS (
	SELECT
		vinculo_profissional.unidade_geografica_id,
		ultima_competencia_disponivel.periodo_id,
		vinculo_profissional.profissional_id_cpf_criptografado,
		vinculo_profissional.profissional_id_cns,
		vinculo_profissional.ocupacao_id_cbo,
		vinculo_profissional.profissional_nome,
		sum(
		  vinculo_profissional.atendimento_carga_ambulatorial
		) / 5 AS profissional_carga_ambulatorial_diaria
	FROM dados_publicos.cnes_vinculos_disseminacao vinculo_profissional
    INNER JOIN 
        saude_mental._bpa_i_caps_ultima_competencia_disponivel
        ultima_competencia_disponivel
    USING (
        unidade_geografica_id,
        periodo_id
    )
	INNER JOIN 
	   saude_mental.estabelecimentos_referencia_ambulatorial
	   estabelecimentos_referencia
	USING (
	   estabelecimento_id_cnes
	)
	WHERE 
	   vinculo_profissional.ocupacao_id_cbo IN (
	       '251510',  -- psicólogos
	       '225133'  -- psiquiatras
	   )
	GROUP BY
		vinculo_profissional.unidade_geografica_id,
		ultima_competencia_disponivel.periodo_id,
		vinculo_profissional.ocupacao_id_cbo,
		vinculo_profissional.profissional_id_cpf_criptografado,
		vinculo_profissional.profissional_id_cns,
		vinculo_profissional.profissional_nome
),
procedimentos_por_hora AS (
	SELECT
		procedimentos_referencias.unidade_geografica_id,
		procedimentos_referencias.unidade_geografica_id_sus,
		sucessao.periodo_id,
		sucessao.periodo_data_inicio AS competencia,
		horas_contratadas.ocupacao_id_cbo,
		horas_contratadas.profissional_id_cpf_criptografado,
		horas_contratadas.profissional_nome,
		sum(
            procedimentos_referencias.procedimentos_realizados
        ) AS procedimentos_realizados,
		(
            sum(horas_contratadas.profissional_carga_ambulatorial_diaria) 
            * listas_de_codigos.datas_diferenca_dias_uteis(
                sucessao.periodo_data_inicio,
                sucessao.proximo_periodo_data_inicio
            )
        ) AS profissional_carga_ambulatorial_mensal
	FROM procedimentos_referencias
	LEFT JOIN horas_contratadas
	USING (
		unidade_geografica_id,
		periodo_id,
		ocupacao_id_cbo,
		profissional_id_cns
	)
	LEFT JOIN listas_de_codigos.periodos_sucessao sucessao
		ON  
            sucessao.periodo_tipo = 'Mensal'
        AND procedimentos_referencias.periodo_id = sucessao.periodo_id
	GROUP BY
	  	-- eliminar CNSs duplicados
		procedimentos_referencias.unidade_geografica_id,
		procedimentos_referencias.unidade_geografica_id_sus,
		sucessao.periodo_id,
		sucessao.periodo_data_inicio,
		sucessao.proximo_periodo_data_inicio,
		horas_contratadas.ocupacao_id_cbo,
		horas_contratadas.profissional_id_cpf_criptografado,
		horas_contratadas.profissional_nome
)
SELECT
    unidade_geografica_id,
    unidade_geografica_id_sus,
    periodo_id,
    competencia,
    ocupacao.ocupacao_descricao AS ocupacao,
    profissional_id_cpf_criptografado,
    profissional_nome,
    procedimentos_realizados,
	round(
	   procedimentos_realizados::numeric 
	   / profissional_carga_ambulatorial_mensal,
	   2
	) AS procedimentos_realizados_por_hora
FROM procedimentos_por_hora
-- TODO: trocar por lista de ocupações do schema `listas_de_codigos`
LEFT JOIN saude_mental.ocupacoes ocupacao
ON procedimentos_por_hora.ocupacao_id_cbo = ocupacao.ocupacao_id
WITH NO DATA;
CREATE UNIQUE INDEX
    referencias_procedimentos_por_profissional_por_hora_ultimo_mes_un
ON saude_mental.referencias_procedimentos_por_profissional_por_hora_ultimo_mes (
    unidade_geografica_id,
    profissional_id_cpf_criptografado
);
