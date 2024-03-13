-- Checar duplicações
, duplicados AS (
    SELECT 
        'backup - Juquitiba SP' AS municipio_uf,
        COUNT(1) AS cont_linhas,
        COUNT(DISTINCT chave_cidadao) AS cont_chaves_nominais_distintas
    FROM aux1 -- Adicionar CTE com o resultado final da consulta da lista nominal (transmissao, lista unificada ou painel)
    GROUP BY 1
)
-- Checar denominador e numerador
, dem_num AS (
        SELECT
        'backup - Juquitiba SP' AS municipio_uf,
        -- concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
        count(DISTINCT chave_cidadao) AS cont_criancas,
        count(DISTINCT CASE WHEN codigo_vacina ='22' THEN chave_cidadao END) AS cont_criancas_vacinas_polio,
        count(DISTINCT CASE WHEN codigo_vacina ='42' THEN chave_cidadao END) AS cont_criancas_vacinas_penta,
        count(DISTINCT co_seq_fat_vacinacao_vacina) AS cont_vacinas,
        count(DISTINCT CASE WHEN codigo_vacina ='22'  THEN co_seq_fat_vacinacao_vacina END) AS cont_vacinas_polio,
        count(DISTINCT CASE WHEN codigo_vacina ='42' THEN co_seq_fat_vacinacao_vacina END) AS cont_vacinas_penta
    FROM aux1 l
    --LEFT JOIN listas_de_codigos.municipios m 
    --    ON m.id_sus = l.municipio_id_sus-- Adicionar CTE com o resultado final da consulta (transmissao, lista unificada ou painel)
    GROUP BY 1
)
SELECT 
    *
FROM dem_num

-- Validações código painel
-- Checar duplicações
, duplicados AS (
    SELECT 
        municipio_id_sus,
        COUNT(1) AS cont_linhas,
        COUNT(DISTINCT l.cidadao_nome||l.cidadao_cpf_dt_nascimento||l.municipio_id_sus) AS cont_chaves_nominais_distintas
    FROM aux1 l
    -- FROM impulso_previne_dados_nominais.painel_vacinacao_lista_nominal l
    WHERE municipio_id_sus != '100111'-- Adicionar CTE com o resultado final da consulta da lista nominal 
    GROUP BY 1
    ORDER BY 1
)
-- Checar denominador e numerador
, dem_num AS (
        SELECT
        municipio_uf,
        -- concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
        count(DISTINCT l.cidadao_nome||l.cidadao_cpf_dt_nascimento||l.municipio_id_sus) AS cont_criancas,
        count(DISTINCT CASE WHEN id_status_polio = 1  THEN l.cidadao_nome||l.cidadao_cpf_dt_nascimento||l.municipio_id_sus END) AS cont_criancas_vacinas_polio,
        count(DISTINCT CASE WHEN id_status_penta = 1 THEN l.cidadao_nome||l.cidadao_cpf_dt_nascimento||l.municipio_id_sus END) AS cont_criancas_vacinas_penta,
        count(DISTINCT CASE WHEN acs_nome = 'Não informado' OR acs_nome = ' ' OR acs_nome IS NULL OR acs_nome LIKE '%SEM PROFISSIONAL%' OR acs_nome LIKE '%SEM VISITA%'
                                THEN l.cidadao_nome||l.cidadao_cpf_dt_nascimento||municipio_id_sus
                        END) AS sem_profissional,
        count(DISTINCT CASE WHEN l.equipe_nome like '%SEM EQUIPE%' OR l.equipe_nome = ' ' OR l.equipe_nome IS NULL
                                THEN l.cidadao_nome||l.cidadao_cpf_dt_nascimento||municipio_id_sus
                        END) AS sem_equipe 
    FROM aux1 l
    -- FROM impulso_previne_dados_nominais.painel_vacinacao_lista_nominal l
    WHERE municipio_id_sus != '100111'
    --LEFT JOIN listas_de_codigos.municipios m 
    --    ON m.id_sus = l.municipio_id_sus-- Adicionar CTE com o resultado final da consulta (transmissao, lista unificada ou painel)
    GROUP BY 1
)
SELECT 
    *
FROM dem_num