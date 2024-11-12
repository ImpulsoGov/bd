WITH diabeticos AS (
         WITH base AS (
		    SELECT 
		        tbd.municipio_id_sus,
		        tbd.equipe_ine_cadastro AS equipe_ine,
		        'DIABETES'::text AS indicador,
		        COUNT(*) AS total_valor,
			    SUM(
			        CASE
			            WHEN tbd.status_usuario <> 'Consulta e solicitação de hemoglobina em dia' 
			            THEN 1
			            ELSE 0
			        END
			    ) AS fora_do_indicador_valor,
		        current_timestamp AS atualizacao_data
		    FROM impulso_previne_dados_nominais_replica.painel_enfermeiras_lista_nominal_diabeticos tbd
		    GROUP BY tbd.municipio_id_sus, tbd.equipe_ine_cadastro
	)
	SELECT 
	    municipio_id_sus,
	    equipe_ine,
	    indicador,
	    'FORA_DO_INDICADOR'::text AS parametro_descricao,
	    fora_do_indicador_valor AS parametro_valor,
	    atualizacao_data
	FROM base
	UNION ALL
	SELECT 
	    municipio_id_sus,
	    equipe_ine,
	    indicador,
	    'TOTAL'::text AS parametro_descricao,
	    total_valor AS parametro_valor,
	    atualizacao_data
	FROM base
)
, hipertensos AS (
		WITH base AS (
		    SELECT 
		        tbh.municipio_id_sus,
		        tbh.equipe_ine_cadastro AS equipe_ine,
		        'HIPERTENSOS'::text AS indicador,
		        COUNT(*) AS total_valor,
			    SUM(
			        CASE
			            WHEN tbh.status_usuario <> 'Consulta e aferição de PA em dia' 
			            THEN 1
			            ELSE 0
			        END
			    ) AS fora_do_indicador_valor,
		        current_timestamp AS atualizacao_data
		    FROM impulso_previne_dados_nominais_replica.painel_enfermeiras_lista_nominal_hipertensos tbh
		    GROUP BY tbh.municipio_id_sus, tbh.equipe_ine_cadastro
	)
	SELECT 
	    municipio_id_sus,
	    equipe_ine,
	    indicador,
	    'FORA_DO_INDICADOR'::text AS parametro_descricao,
	    fora_do_indicador_valor AS parametro_valor,
	    atualizacao_data
	FROM base
	UNION ALL
	SELECT 
	    municipio_id_sus,
	    equipe_ine,
	    indicador,
	    'TOTAL'::text AS parametro_descricao,
	    total_valor AS parametro_valor,
	    atualizacao_data
	FROM base
), 
citopatologico AS (
         WITH base AS (
		    SELECT 
		        tbc.municipio_id_sus,
		        tbc.equipe_ine AS equipe_ine,
		        'CITOPATOLOGICO'::text AS indicador,
		        COUNT(*) AS total_valor,
		        SUM(
		           CASE
		                WHEN  tbc.id_status_usuario <> 12 -- Quando status diferente de 12 (Em Dia)
		                THEN 1
		                ELSE 0
		            END
		        ) AS fora_do_indicador_valor,
		        current_timestamp AS atualizacao_data
		    FROM impulso_previne_dados_nominais_replica.painel_citopatologico_lista_nominal tbc
		    GROUP BY tbc.municipio_id_sus, tbc.equipe_ine
	)
	SELECT 
	    municipio_id_sus,
	    equipe_ine,
	    indicador,
	    'FORA_DO_INDICADOR'::text AS parametro_descricao,
	    fora_do_indicador_valor AS parametro_valor,
	    atualizacao_data
	FROM base
	UNION ALL
	SELECT 
	    municipio_id_sus,
	    equipe_ine,
	    indicador,
	    'TOTAL'::text AS parametro_descricao,
	    total_valor AS parametro_valor,
	    atualizacao_data
	FROM base
), 
vacinacao AS (
         WITH base AS (
		    SELECT 
		        tbv.municipio_id_sus,
		        tbv.equipe_ine AS equipe_ine,
		        'VACINACAO'::text AS indicador,
		        COUNT(DISTINCT tbv.cidadao_nome ||tbv.cidadao_cpf_dt_nascimento) AS total_valor,
		        COUNT(
		            DISTINCT 
		                CASE
		                    WHEN tbv.id_status_polio = 3 OR tbv.id_status_penta = 3 -- Quando status polio ou penta em atraso
		                THEN tbv.cidadao_nome||tbv.cidadao_cpf_dt_nascimento
		                ELSE NULL
		            END
		        ) AS fora_do_indicador_valor,
		        current_timestamp AS atualizacao_data
		    FROM impulso_previne_dados_nominais_replica.painel_vacinacao_lista_nominal tbv
		    WHERE tbv.id_status_quadrimestre = 1
		    GROUP BY tbv.municipio_id_sus, tbv.equipe_ine
	)
	SELECT 
	    municipio_id_sus,
	    equipe_ine,
	    indicador,
	    'FORA_DO_INDICADOR'::text AS parametro_descricao,
	    fora_do_indicador_valor AS parametro_valor,
	    atualizacao_data
	FROM base
	UNION ALL
	SELECT 
	    municipio_id_sus,
	    equipe_ine,
	    indicador,
	    'TOTAL'::text AS parametro_descricao,
	    total_valor AS parametro_valor,
	    atualizacao_data
	FROM base
), 
gestantes_6_consultas AS (
         WITH base AS (
		    SELECT 
		         tb6.municipio_id_sus,
		         tb6.equipe_ine,
		        'PRE_NATAL_6_CONSULTAS'::text AS indicador,
		        COUNT(DISTINCT tb6.chave_id_gestacao) AS total_valor,
		        COUNT(
		            DISTINCT 
		                CASE
		                    WHEN tb6.gestacao_idade_gestacional_primeiro_atendimento >= 0 
		                    AND tb6.gestacao_idade_gestacional_primeiro_atendimento <= 12 
		                    AND tb6.consultas_pre_natal_validas < 6  -- gestantes com menos de 6 consultas em 12 semanas
		                THEN tb6.chave_id_gestacao 
		                ELSE NULL
		            END
		        ) AS fora_do_indicador_valor,
		        current_timestamp AS atualizacao_data
		    FROM impulso_previne_dados_nominais_replica.painel_gestantes_lista_nominal tb6
		    WHERE tb6.id_status_usuario = 8 -- gestação ativa
		    GROUP BY tb6.municipio_id_sus, tb6.equipe_ine
	)
	SELECT 
	    municipio_id_sus,
	    equipe_ine,
	    indicador,
	    'FORA_DO_INDICADOR'::text AS parametro_descricao,
	    fora_do_indicador_valor AS parametro_valor,
	    atualizacao_data
	FROM base
	UNION ALL
	SELECT 
	    municipio_id_sus,
	    equipe_ine,
	    indicador,
	    'TOTAL'::text AS parametro_descricao,
	    total_valor AS parametro_valor,
	    atualizacao_data
	FROM base
), 
gestantes_odonto_indetificado AS (
         WITH base AS (
		    SELECT 
		         tbo.municipio_id_sus,
		         tbo.equipe_ine,
		        'PRE_NATAL_ODONTO'::text AS indicador,
		        COUNT(DISTINCT tbo.chave_id_gestacao) AS total_valor,
		        COUNT(
		            DISTINCT 
		                CASE
		                    WHEN tbo.id_atendimento_odontologico <> 1 -- Atend. odontológico NÃO identificado
		                THEN tbo.chave_id_gestacao 
		                ELSE NULL
		            END
		        ) AS fora_do_indicador_valor,
		        current_timestamp AS atualizacao_data
		    FROM impulso_previne_dados_nominais_replica.painel_gestantes_lista_nominal tbo
		    WHERE tbo.id_status_usuario = 8 -- gestação ativa
		    GROUP BY tbo.municipio_id_sus, tbo.equipe_ine
	)
	SELECT 
	    municipio_id_sus,
	    equipe_ine,
	    indicador,
	    'FORA_DO_INDICADOR'::text AS parametro_descricao,
	    fora_do_indicador_valor AS parametro_valor,
	    atualizacao_data
	FROM base
	UNION ALL
	SELECT 
	    municipio_id_sus,
	    equipe_ine,
	    indicador,
	    'TOTAL'::text AS parametro_descricao,
	    total_valor AS parametro_valor,
	    atualizacao_data
	FROM base
), 
gestantes_sifilis_hiv AS (
         WITH base AS (
		    SELECT 
		         tbe.municipio_id_sus,
		         tbe.equipe_ine,
		        'PRE_NATAL_SIFILIS_HIV'::text AS indicador,
		        COUNT(DISTINCT tbe.chave_id_gestacao) AS total_valor,
		        COUNT(
		            DISTINCT 
		                CASE
		                    WHEN tbe.id_exame_hiv_sifilis <> 4 -- Todos os demais status que não sejam os dois exames realizados
		                THEN tbe.chave_id_gestacao 
		                ELSE NULL
		            END
		        ) AS fora_do_indicador_valor,
		        current_timestamp AS atualizacao_data
		    FROM impulso_previne_dados_nominais_replica.painel_gestantes_lista_nominal tbe
		    WHERE tbe.id_status_usuario = 8 -- gestação ativa
		    GROUP BY tbe.municipio_id_sus, tbe.equipe_ine
	)
	SELECT 
	    municipio_id_sus,
	    equipe_ine,
	    indicador,
	    'FORA_DO_INDICADOR'::text AS parametro_descricao,
	    fora_do_indicador_valor AS parametro_valor,
	    atualizacao_data
	FROM base
	UNION ALL
	SELECT 
	    municipio_id_sus,
	    equipe_ine,
	    indicador,
	    'TOTAL'::text AS parametro_descricao,
	    total_valor AS parametro_valor,
	    atualizacao_data
	FROM base
)
 SELECT *
   FROM gestantes_6_consultas
UNION ALL
 SELECT *
   FROM gestantes_odonto_indetificado
UNION ALL
 SELECT *
   FROM gestantes_sifilis_hiv
UNION ALL
 SELECT *
   FROM diabeticos
UNION ALL
 SELECT *
   FROM hipertensos
UNION ALL
 SELECT *
   FROM citopatologico
UNION ALL
 SELECT *
   FROM vacinacao