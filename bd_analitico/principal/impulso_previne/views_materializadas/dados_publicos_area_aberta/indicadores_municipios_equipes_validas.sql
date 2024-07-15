SELECT sub.municipio_id_ibge,
    sub.municipio_id_sus,
    sub.municipio_nome,
    sub.municipio_uf,
    sub.periodo_codigo,
    sub.periodo_id,
    sub.periodo_data_inicio,
    sub.periodo_data_fim,
    sub.indicador_id,
    sub.indicador_ordem,
    sub.indicador_prioridade,
    sub.indicador_nome,
    sub.indicador_peso,
    sub.indicador_validade_resultado,
    sub.indicador_acoes_por_usuario,
    sub.indicador_numerador,
    sub.indicador_denominador_estimado,
    concat(sub.indicador_denominador_utilizado, '
	', '(', sub.indicador_denominador_informado, ')') AS indicador_denominador_utilizado_informado,
    sub.indicador_denominador_utilizado,
    sub.indicador_denominador_utilizado_tipo,
    round(sub.indicador_denominador_informado_diferenca_utilizado, 2) AS indicador_denominador_informado_diferenca_utilizado,
    concat(round(sub.indicador_denominador_informado_diferenca_utilizado * 100::numeric, 0), '%') AS indicador_denominador_informado_diferenca_utilizado_formatado,
    sub.indicador_nota,
    sub.indicador_nota_porcentagem,
    sub.indicador_meta,
    sub.indicador_diferenca_meta,
    sub.indicador_recomendacao,
    sub.delta,
        CASE
            WHEN sub.delta > 0::numeric THEN concat('+', round(sub.delta * 100::numeric, 0), '%')
            ELSE concat(round(sub.delta * 100::numeric, 0), '%')
        END AS delta_formatado,
    round(sub.indicador_usuarios_100_porcento_meta::numeric, 0) AS indicador_usuarios_100_porcento_meta,
    round(sub.indicador_usuarios_cadastrados_sem_atendimento::numeric, 0) AS indicador_usuarios_cadastrados_sem_atendimento,
    round(sub.indicador_usuarios_cadastrar_para_meta::numeric, 0) AS indicador_usuarios_cadastrar_para_meta,
    rank() OVER (PARTITION BY sub.municipio_id_ibge, sub.periodo_id ORDER BY sub.indicador_prioridade DESC, sub.indicador_classificacao DESC) AS indicador_score,
    CURRENT_TIMESTAMP AS criacao_data,
    CURRENT_TIMESTAMP AS atualizacao_data,
    sub.indicador_denominador_informado
   FROM ( SELECT sub_1.municipio_id_ibge,
            sub_1.municipio_id_sus,
            sub_1.municipio_nome,
            sub_1.municipio_uf,
            sub_1.periodo_codigo,
            sub_1.periodo_id,
            sub_1.periodo_data_inicio,
            sub_1.periodo_data_fim,
            sub_1.indicador_id,
            sub_1.indicador_ordem,
            sub_1.indicador_nome,
            sub_1.indicador_peso,
            sub_1.indicador_meta,
            sub_1.indicador_validade_resultado,
            sub_1.indicador_acoes_por_usuario,
            sub_1.indicador_numerador,
            sub_1.indicador_denominador_estimado,
            sub_1.indicador_denominador_informado,
            sub_1.indicador_denominador_utilizado,
            sub_1.indicador_nota,
            sub_1.indicador_nota_porcentagem,
            sub_1.indicador_prioridade,
            sub_1.indicador_recomendacao,
                CASE
                    WHEN lag(sub_1.indicador_nota_porcentagem) OVER (ORDER BY sub_1.municipio_uf, sub_1.indicador_nome, sub_1.periodo_data_inicio) > 0 THEN round(sub_1.delta / lag(sub_1.indicador_nota_porcentagem) OVER (ORDER BY sub_1.municipio_uf, sub_1.indicador_nome, sub_1.periodo_data_inicio)::numeric, 2)
                    ELSE 0::numeric
                END AS delta,
                CASE
                    WHEN sub_1.indicador_denominador_informado::double precision < (sub_1.indicador_denominador_utilizado * (sub_1.indicador_meta / 100::double precision)) THEN (sub_1.indicador_denominador_informado - sub_1.indicador_numerador)::double precision
                    WHEN sub_1.indicador_denominador_informado::double precision >= (sub_1.indicador_denominador_utilizado * (sub_1.indicador_meta / 100::double precision)) AND sub_1.indicador_numerador::double precision < (sub_1.indicador_denominador_utilizado * (sub_1.indicador_meta / 100::double precision)) THEN sub_1.indicador_denominador_utilizado * (sub_1.indicador_meta / 100::double precision) - sub_1.indicador_numerador::double precision
                    ELSE 0::double precision
                END AS indicador_usuarios_cadastrados_sem_atendimento,
                CASE
                    WHEN sub_1.indicador_nota_porcentagem::double precision < sub_1.indicador_meta THEN sub_1.indicador_meta - sub_1.indicador_nota_porcentagem::double precision
                    ELSE 0::double precision
                END AS indicador_diferenca_meta,
                CASE
                    WHEN sub_1.indicador_denominador_informado::double precision < (sub_1.indicador_denominador_utilizado * (sub_1.indicador_meta / 100::double precision)) THEN sub_1.indicador_denominador_utilizado * (sub_1.indicador_meta / 100::double precision) - sub_1.indicador_denominador_informado::double precision
                    ELSE 0::double precision
                END AS indicador_usuarios_cadastrar_para_meta,
                CASE
                    WHEN sub_1.indicador_denominador_informado::double precision >= sub_1.indicador_denominador_utilizado THEN 'Informado'::text
                    ELSE 'Estimado'::text
                END AS indicador_denominador_utilizado_tipo,
                CASE
                    WHEN sub_1.indicador_denominador_informado::double precision < sub_1.indicador_denominador_utilizado THEN sub_1.indicador_denominador_utilizado::numeric / COALESCE(NULLIF(sub_1.indicador_denominador_informado, 0), 1)::numeric - 1::numeric
                    ELSE 0::numeric
                END AS indicador_denominador_informado_diferenca_utilizado,
                CASE
                    WHEN sub_1.indicador_nota_porcentagem::double precision <= sub_1.indicador_meta THEN sub_1.indicador_meta / 100::double precision * sub_1.indicador_denominador_utilizado - sub_1.indicador_numerador::double precision
                    ELSE 0::double precision
                END AS indicador_usuarios_100_porcento_meta,
                CASE
                    WHEN sub_1.indicador_nota_porcentagem::double precision >= sub_1.indicador_meta THEN (sub_1.indicador_meta - sub_1.indicador_nota_porcentagem::double precision) / 100.0::double precision
                    ELSE 1000::double precision * (1000::double precision * sub_1.indicador_peso * sub_1.indicador_validade_resultado / (COALESCE(NULLIF(
                    CASE
                        WHEN sub_1.indicador_nota_porcentagem::double precision < sub_1.indicador_meta THEN sub_1.indicador_meta / 100::double precision * sub_1.indicador_denominador_utilizado - sub_1.indicador_numerador::double precision
                        ELSE 0::double precision
                    END, 0::double precision), 1::double precision) * sub_1.indicador_acoes_por_usuario)) /
                    CASE
                        WHEN (sub_1.indicador_denominador_estimado - sub_1.indicador_denominador_informado) > 0 THEN (sub_1.indicador_denominador_estimado - sub_1.indicador_denominador_informado)::numeric / 100.0
                        ELSE 1::numeric
                    END::double precision
                END AS indicador_classificacao
           FROM ( SELECT lcm.id_ibge AS municipio_id_ibge,
                    lcm.nome AS municipio_nome,
                    concat(lcm.nome, ' - ', lcm.uf_sigla) AS municipio_uf,
                    lcm.id_sus AS municipio_id_sus,
                    sim.periodo_codigo,
                    p.id AS periodo_id,
                    p.data_inicio AS periodo_data_inicio,
                    p.data_fim AS periodo_data_fim,
                    ir.id AS indicador_id,
                    ir.ordem AS indicador_ordem,
                    ir.nome AS indicador_nome,
                    ir.peso AS indicador_peso,
                    ir.meta AS indicador_meta,
                    ips.validade_resultado AS indicador_validade_resultado,
                    ips.acoes_por_usuario AS indicador_acoes_por_usuario,
                    sim.numerador AS indicador_numerador,
                    sim.nota_porcentagem AS indicador_nota_porcentagem,
                    sim.denominador_estimado AS indicador_denominador_estimado,
                    sim.denominador_informado AS indicador_denominador_informado,
                    ir2."recomendação" AS indicador_recomendacao,
                        CASE
                            WHEN p.data_inicio >= '2022-01-01'::date THEN sim.denominador_utilizado::numeric::double precision
                            ELSE
                            CASE
                                WHEN sim.denominador_informado::numeric >= round(0.85 * sim.denominador_estimado::numeric, 0) THEN sim.denominador_informado
                                ELSE sim.denominador_estimado
                            END::double precision
                        END AS indicador_denominador_utilizado,
                        CASE
                            WHEN ((sim.nota_porcentagem * 10)::double precision / ir.meta) > 10::double precision THEN 10::numeric
                            ELSE round(((sim.nota_porcentagem * 10)::double precision / ir.meta)::numeric, 1)
                        END AS indicador_nota,
                        CASE
                            WHEN ((sim.nota_porcentagem * 10)::double precision / ir.meta) >= 10::double precision THEN 0
                            ELSE 1
                        END AS indicador_prioridade,
                        CASE
                            WHEN sim.nota_porcentagem > 0 AND lag(sim.nota_porcentagem) OVER (ORDER BY lcm.id_sus, ir.nome, p.data_inicio) > 0 THEN (sim.nota_porcentagem - lag(sim.nota_porcentagem) OVER (ORDER BY lcm.id_sus, ir.nome, p.data_inicio))::numeric
                            ELSE 0::numeric
                        END AS delta
                   FROM impulso_previne.indicadores_premissas_score ips
                     JOIN previne_brasil.indicadores_regras ir ON ips.indicador_regras_id = ir.id
                     JOIN dados_publicos.sisab_indicadores_municipios_equipes_validas sim ON sim.indicadores_regras_id = ir.id
                     JOIN previne_brasil.indicadores_recomendacoes ir2 ON sim.indicadores_nome::text = ir2."indicador " AND ir2.versao = 2
                     JOIN listas_de_codigos.municipios lcm ON sim.municipio_id_sus::bpchar = lcm.id_sus
                     JOIN listas_de_codigos.periodos p ON sim.periodo_id = p.id) sub_1) sub