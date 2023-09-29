/******************************************************************************

          ROTINAS DE ATUALIZAÇÃO DAS CONSULTAS DE SAÚDE MENTAL

 ******************************************************************************/

CREATE OR REPLACE PROCEDURE
    saude_mental.atualizar_competencias_disponiveis()
LANGUAGE plpgsql
AS $$
    BEGIN
        REFRESH MATERIALIZED VIEW
            saude_mental._procedimentos_ultima_competencia_disponivel
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental._raas_primeira_competencia_disponivel
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental._raas_ultima_competencia_disponivel
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental._bpa_i_primeira_competencia_disponivel
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental._bpa_i_caps_ultima_competencia_disponivel
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental._aih_rd_ultima_competencia_disponivel
        ;
    END;
$$
;
COMMENT ON PROCEDURE
    saude_mental.atualizar_ultimas_competencias_disponiveis
IS '
Atualiza visualizações materializadas que guardam as últimas competências com
dados da AIH, RAAS, BPA-i e procedimentos ambulatoriais disponíveis.

Deve ser chamado sempre que qualquer uma das tabelas 
`dados_publicos.siasus_procedimentos_ambulatoriais`, 
`dados_publicos.siasus_raas_psicossocial_disseminacao`, 
`dados_publicos.siasus_bpa_i_disseminacao` ou 
`dados_publicos.sihsus_aih_reduzida_disseminacao` forem atualizadas.
';


CREATE OR REPLACE PROCEDURE saude_mental.atualizar_matriciamentos()
LANGUAGE plpgsql
AS $$
    BEGIN
        REFRESH MATERIALIZED VIEW
            saude_mental.matriciamentos_meta_por_caps_ultimo_ano
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.matriciamentos_meta_ultimo_ano
        ;
    END;
$$
;
COMMENT ON PROCEDURE
    saude_mental.atualizar_matriciamentos
IS '
Atualiza visualizações materializadas sobre matriciamentos entre equipes de CAPS
e da Atenção Primária à Saúde.

Deve ser chamada sempre que a tabela 
`dados_publicos.siasus_procedimentos_ambulatoriais` for atualizada.
';


CREATE OR REPLACE PROCEDURE saude_mental.atualizar_reducao_danos()
LANGUAGE plpgsql
AS $$
    BEGIN
        REFRESH MATERIALIZED VIEW
            saude_mental._reducao_danos_acoes_por_estabelecimento_por_mes
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.reducao_danos_acoes_por_estabelecimento_por_mes
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.reducao_danos_acoes_por_estabelecimento_ultimo_mes
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.reducao_danos_acoes_ultimos_12m
        ;
    END;
$$
;
COMMENT ON PROCEDURE
    saude_mental.atualizar_reducao_danos
IS '
Atualiza visualizações materializadas sobre ações de redução de danos.

Deve ser chamada sempre que a tabela 
`dados_publicos.siasus_procedimentos_ambulatoriais` for atualizada.
';



CREATE OR REPLACE PROCEDURE saude_mental.atualizar_consultorio_na_rua()
LANGUAGE plpgsql
AS $$
    BEGIN
        REFRESH MATERIALIZED VIEW
            saude_mental._consultorio_na_rua_atendimentos
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.consultorio_na_rua_atendimentos
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.consultorio_na_rua_atendimentos_ultimo_mes
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.consultorio_na_rua_atendimentos_ultimos_12meses
        ;
    END;
$$
;
COMMENT ON PROCEDURE
    saude_mental.atualizar_consultorio_na_rua
IS '
Atualiza visualizações materializadas dos atendimentos individuais das 
equipes do consultório na rua.

Deve ser chamada sempre que a tabela 
`dados_publicos.sisab_producao_municipios_por_tipo_equipe_por_tipo_producao`
for atualizada.
';



CREATE OR REPLACE PROCEDURE
    saude_mental.atualizar_atendimentos_individuais()
LANGUAGE plpgsql
AS $$
    BEGIN
        REFRESH MATERIALIZED VIEW
            saude_mental._usuarios_atendimentos_individuais
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.usuarios_atendimentos_individuais_perfil
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.atendimentos_individuais_perfil_resumo_ultimo_mes
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental._atendimentos_individuais_por_caps
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.atendimentos_individuais_por_caps
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.atendimentos_individuais_por_caps_ultimo_mes
        ;
    END;
$$
;
COMMENT ON PROCEDURE
    saude_mental.atualizar_atendimentos_individuais
IS '
Atualiza visualizações materializadas sobre usuários que realizam apenas 
atendimentos individuais em CAPS.

Deve ser chamada sempre que qualquer uma das tabelas 
`dados_publicos.siasus_bpa_i_disseminacao` ou
`dados_publicos.siasus_raas_psicossocial_disseminacao`
forem atualizadas.
';


 
CREATE OR REPLACE PROCEDURE
    saude_mental.atualizar_procedimentos_por_usuario()
LANGUAGE plpgsql
AS $$
    BEGIN
        REFRESH MATERIALIZED VIEW
            saude_mental.caps_procedimentos_total_e_acolhimentos
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental._usuarios_procedimentos_por_mes
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental._procedimentos_por_usuario_por_estabelecimento
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.procedimentos_por_usuario_por_caps
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.procedimentos_por_usuario_por_caps_ultimo_mes
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.procedimentos_por_usuario_por_tempo_servico
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.procedimentos_por_usuario_por_tempo_servico_resumo_ultimo_mes
        ;
    END;
$$
;
COMMENT ON PROCEDURE
    saude_mental.atualizar_procedimentos_por_usuario
IS '
Atualiza visualizações materializadas sobre a quantidade de procedimentos 
realizados por usuário em CAPS.

Deve ser chamada sempre que qualquer uma das tabelas 
`siasus_procedimentos_ambulatoriais`, `dados_publicos.siasus_bpa_i_disseminacao`
 ou `dados_publicos.siasus_raas_psicossocial_disseminacao` forem atualizadas.
';


CREATE OR REPLACE PROCEDURE
    saude_mental.atualizar_usuarios_ativos()
LANGUAGE plpgsql
AS $$
    BEGIN
        REFRESH MATERIALIZED VIEW
            saude_mental._usuarios_ativos
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.usuarios_ativos_perfil
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental._usuarios_ativos_por_estabelecimento
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental._usuarios_ativos_por_estabelecimento_resumo
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.usuarios_ativos_por_estabelecimento_resumo
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.usuarios_ativos_por_estabelecimento_resumo_ultimo_mes
        ;
    END;
$$
;
COMMENT ON PROCEDURE
    saude_mental.atualizar_usuarios_ativos
IS '
Atualiza visualizações materializadas sobre o perfil dos usuários ativos nos 
últimos três meses em CAPS.

Deve ser chamada sempre que qualquer uma das tabelas 
`dados_publicos.siasus_raas_psicossocial_disseminacao` ou (futuramente)
`dados_publicos.siasus_bpa_i_disseminacao` forem atualizadas.
';


CREATE OR REPLACE PROCEDURE
    saude_mental.atualizar_usuarios_novos()
LANGUAGE plpgsql
AS $$
    BEGIN
        REFRESH MATERIALIZED VIEW
            saude_mental._usuarios_novos
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.usuarios_novos
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.usuarios_novos_resumo_ultimo_mes
        ;
    END;
$$
;
COMMENT ON PROCEDURE
    saude_mental.atualizar_usuarios_novos
IS '
Atualiza visualizações materializadas sobre o perfil dos usuários 
recém-acolhidos em CAPS.

Deve ser chamada sempre que qualquer uma das tabelas 
`dados_publicos.siasus_raas_psicossocial_disseminacao` ou (futuramente) 
`dados_publicos.siasus_bpa_i_disseminacao` forem atualizadas.
';


CREATE OR REPLACE PROCEDURE
    saude_mental.atualizar_encaminhamentos_aps()
LANGUAGE plpgsql
AS $$
    BEGIN
        REFRESH MATERIALIZED VIEW
            saude_mental._aps_encaminhamentos_especializada
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.aps_encaminhamentos_especializada
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.aps_encaminhamentos_especializada_resumo_ultimo_mes
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.aps_encaminhamentos_especializada_resumo_ultimo_mes_vertical
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental._aps_encaminhamentos_caps
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.aps_encaminhamentos_caps
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.aps_encaminhamentos_caps_resumo_ultimo_mes
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.aps_encaminhamentos_caps_resumo_ultimo_mes_vertical
        ;
    END;
$$
;
COMMENT ON PROCEDURE
    saude_mental.atualizar_encaminhamentos_aps
IS '
Atualiza visualizações materializadas sobre os atendimentos em saúde mental da 
Atenção Primária à Saúde, com foco nos atendimentos que resultam em 
encaminhamentos para CAPS ou rede especializada.

Deve ser chamada sempre que a tabela 
`dados_publicos.sisab_producao_municipios_por_conduta_por_problema_condicao_ava`
 for atualizada.
';



CREATE OR REPLACE PROCEDURE
    saude_mental.atualizar_taxa_abandono()
LANGUAGE plpgsql
AS $$
    BEGIN
        REFRESH MATERIALIZED VIEW
            saude_mental._raas_primeiro_procedimento
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental._usuarios_recentes
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental._usuarios_recentes_perfil_ambulatorial
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental._usuarios_recentes_abandono
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental._usuarios_recentes_abandono_mensal
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.abandonos_perfil_usuarios
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.abandono_mensal
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental._usuarios_recentes_abandono_coortes
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.abandono_coortes_resumo
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.abandono_coortes_resumo_ultimo_mes
        ;
        END;
    $$
;
COMMENT ON PROCEDURE
    saude_mental.atualizar_taxa_abandono
IS '
Atualiza visualizações materializadas sobre o abandono de usuários recentes 
(até seis meses do primeiro procedimento realizado) em CAPS.

Deve ser chamada sempre que qualquer uma das tabelas 
`dados_publicos.siasus_raas_psicossocial_disseminacao` ou
(futuramente) `dados_publicos.siasus_bpa_i_disseminacao` forem atualizadas.
';


CREATE OR REPLACE PROCEDURE
    saude_mental.atualizar_referencias_sm()
LANGUAGE plpgsql
AS $$
    BEGIN
        REFRESH MATERIALIZED VIEW
            saude_mental._procedimentos_referencias_ambulatoriais
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.usuarios_referencias_por_faixa_etaria_ultimo_mes
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.procedimentos_referencias_resumo
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.procedimentos_referencias_resumo_ultimo_mes
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.referencias_perfil_usuarios
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.referencias_procedimentos_por_profissional_por_hora_ultimo_mes
        ;
    END;
$$
;
COMMENT ON PROCEDURE
    saude_mental.atualizar_referencias_sm
IS '
Atualiza visualizações materializadas sobre a quantidade de atendimentos das 
referências ambulatoriais em saúde mental (psicólogos e psiquiatras).

Deve ser chamada sempre que a da tabela 
`dados_publicos.siasus_bpa_i_disseminacao` for atualizada.
';


CREATE OR REPLACE PROCEDURE
    saude_mental.atualizar_internacoes()
LANGUAGE plpgsql
AS $$
    BEGIN
        REFRESH MATERIALIZED VIEW
            saude_mental._atendimentos_raps
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental._aih_saude_mental
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental._internacoes_relacao_raps
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.relacao_raps_reue_altas_resumo_12m
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.relacao_raps_reue_altas_resumo_12m_vertical
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.relacao_raps_reue_internacoes_resumo_12m
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.relacao_raps_reue_internacoes_resumo_12m_vertical
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.acolhimentos_noturnos_12m
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.internacoes_geolocalizadas
        ;
    END;
$$
;
COMMENT ON PROCEDURE
    saude_mental.atualizar_internacoes
IS '
Atualiza visualizações materializadas sobre a relação entre a Rede de Atenção 
Psicossocial e a Redes Hospitalar e de Urgência e Emergência.

Deve ser chamada sempre que qualquer uma das tabelas 
`saude_mental.condicoes_saude_mental`,
`dados_publicos.sihsus_aih_reduzida_disseminacao` ou
`dados_publicos.siasus_raas_psicossocial_disseminacao` forem atualizadas.
';


CREATE OR REPLACE PROCEDURE
    saude_mental.atualizar_producao_caps()
LANGUAGE plpgsql
AS $$
    BEGIN
        REFRESH MATERIALIZED VIEW
            saude_mental._procedimentos_por_ocupacao_por_mes
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental._procedimentos_por_hora_por_ocupacao_por_estabelecimento
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.procedimentos_por_hora_resumo
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.procedimentos_por_hora_resumo_ultimo_mes
        ;
        REFRESH MATERIALIZED VIEW
            saude_mental.procedimentos_realizados_por_tipo
        ;
    END;
$$
;
COMMENT ON PROCEDURE
    saude_mental.atualizar_producao_caps
IS '
Atualiza visualizações materializadas sobre a produção dos CAPS.

Deve ser chamada sempre que qualquer uma das tabelas 
`dados_publicos.siasus_procedimentos_ambulatoriais` ou
`dados_publicos.cnes_vinculos_disseminacao` forem atualizadas.
';



CREATE TABLE IF NOT EXISTS
    saude_mental.bd_log_erros (
        id uuid DEFAULT uuid_generate_v4() PRIMARY KEY,
        data_hora timestamptz NOT NULL DEFAULT now(),
        codigo text,
        mensagem text,
        contexto text
    )
;
COMMENT ON TABLE
    saude_mental.bd_log_erros
IS '
Registra o histórico de erros ocorridos ao atualizar visualizações de saúde 
mental.
'

CREATE OR REPLACE PROCEDURE
    saude_mental.atualizar_saude_mental()
LANGUAGE plpgsql
AS $$
    DECLARE
        erro_codigo text;
        erro_mensagem text;
        erro_contexto text;
    BEGIN
        CALL saude_mental.atualizar_competencias_disponiveis();
        CALL saude_mental.atualizar_matriciamentos();
        CALL saude_mental.atualizar_reducao_danos();
        CALL saude_mental.atualizar_consultorio_na_rua();
        CALL saude_mental.atualizar_atendimentos_individuais();
        CALL saude_mental.atualizar_procedimentos_por_usuario();
        CALL saude_mental.atualizar_usuarios_ativos();
        CALL saude_mental.atualizar_usuarios_novos();
        CALL saude_mental.atualizar_encaminhamentos_aps();
        CALL saude_mental.atualizar_taxa_abandono();
        CALL saude_mental.atualizar_referencias_sm();
        CALL saude_mental.atualizar_internacoes();
        CALL saude_mental.atualizar_producao_caps();
    EXCEPTION
        WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS
                erro_codigo = RETURNED_SQLSTATE,
                erro_mensagem = MESSAGE_TEXT,
                erro_contexto = PG_EXCEPTION_CONTEXT;
            INSERT INTO saude_mental.bd_log_erros (
                codigo,
                mensagem,
                contexto
            )
            VALUES (
                erro_codigo,
                erro_mensagem,
                erro_contexto
            );
            RAISE NOTICE '
                Algo de errado ocorreu enquanto tentava atualizar as 
                consultas de saúde mental. Consulta a tabela 
                `saude_mental.bd_log_erros` para mais informações sobre o 
                ocorrido.
            ';
    END;
$$
;
COMMENT ON PROCEDURE
    saude_mental.atualizar_saude_mental
IS '
Atualiza todas as visualizações materializadas do projeto de saúde mental.
';


CREATE OR REPLACE FUNCTION
    saude_mental.checar_fontes_alteradas(
        intervalo_alteracoes interval DEFAULT '1 day'::interval
    )
RETURNS bool
LANGUAGE plpgsql
AS $$
    BEGIN
        RETURN EXISTS (
            SELECT *
            FROM configuracoes.capturas_historico_consolidado historico
            LEFT JOIN configuracoes.capturas_operacoes operacao
            ON historico.operacao_id = operacao.id
            WHERE
                historico.atualizado_em >= now() - intervalo_alteracoes
            AND operacao.tabela_destino IN (
                    'dados_publicos.cnes_vinculos_disseminacao',
                    'dados_publicos.siasus_raas_psicossocial_disseminacao',
                    'dados_publicos.siasus_bpa_i_disseminacao',
                    'dados_publicos.siasus_procedimentos_ambulatoriais',
                    'dados_publicos.sihsus_aih_reduzida_disseminacao'
                )
            LIMIT 1
        );
    END;
$$
;


GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA cron TO current_user;
SELECT cron.schedule(
    'atualizar_sm_se_alterado',
    '42 2 * * *',
    $$
        IF saude_mental.checar_fontes_alteradas()
        THEN
            CALL saude_mental.atualizar_saude_mental();
            -- Rodar primeiro script `integracao_analitico_producao.sql` para
            -- garantir que a função a seguir foi definida
            CALL _saude_mental_producao.sincronizar_tabelas();
        END IF
    $$
);

