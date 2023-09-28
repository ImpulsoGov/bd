-- impulso_previne.acoes_estrategicas_vigente_agrupada source

CREATE MATERIALIZED VIEW impulso_previne.acoes_estrategicas_vigente_agrupada
TABLESPACE pg_default
AS WITH visao AS (
         SELECT p.data_inicio,
            dados.acao_nome,
            concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
            dados.pagamento_total,
            dados.pagamento_implantacao,
            dados.nivel_repasse,
            dados.periodicidade,
            dados.requisitos
           FROM ( SELECT 'Programa Saúde na Hora'::text AS acao_nome,
                    egestor_financiamento_acoes_estrategicas_saude_hora.municipio_id_sus,
                    egestor_financiamento_acoes_estrategicas_saude_hora.periodo_id,
                    egestor_financiamento_acoes_estrategicas_saude_hora.pagamento_custeio AS pagamento_total,
                    egestor_financiamento_acoes_estrategicas_saude_hora.pagamento_implantacao::text AS pagamento_implantacao,
                    'Unidade'::text AS nivel_repasse,
                    'Mensal'::text AS periodicidade,
                    '• Cadastro no SCNES 

					• Ter todas as equipes de saúde respeitando a carga horária mínima exigida pelo programa para cada categoria profissional 
		
					• Ter o gerente de APS cadastrado no SCNES com carga horária mínima de 30 horas semanais 
		
					• Utilizar prontuário eletrônico, seja o e-SUS-APS/PEC ou outro sistema via Thrift 
					Observação: Esse critério tem prazo de 12 competências a partir da publicação da portaria de homologação de adesão para o formato de funcionamento 60h Simplificado 
		
					• Ter a identidade visual do programa Saúde na Hora 
		
					• Envio de dados dentro dos parâmetros estabelecidos'::text AS requisitos
                   FROM dados_publicos.egestor_financiamento_acoes_estrategicas_saude_hora
                UNION
                 SELECT 'Informatiza APS'::text AS acao_nome,
                    egestor_financiamento_acoes_estrategicas_informatiza_aps.municipio_id_sus,
                    egestor_financiamento_acoes_estrategicas_informatiza_aps.periodo_id,
                    egestor_financiamento_acoes_estrategicas_informatiza_aps.pagamento_total,
                    'NA'::text AS pagamento_implantacao,
                    'Por Equipe'::text AS nivel_repasse,
                    'Mensal'::text AS periodicidade,
                    '• CNES Atualizado 

			• Envio de produção através do SISAB (dentro dos parâmentos estabelecidos)'::text AS requisitos
                   FROM dados_publicos.egestor_financiamento_acoes_estrategicas_informatiza_aps
                UNION
                 SELECT 'Equipes de Saúde Bucal'::text AS acao_nome,
                    egestor_financiamento_acoes_estrategicas_saude_bucal.municipio_id_sus,
                    egestor_financiamento_acoes_estrategicas_saude_bucal.periodo_id,
                    egestor_financiamento_acoes_estrategicas_saude_bucal.pagamento_esb_custeio AS pagamento_total,
                    egestor_financiamento_acoes_estrategicas_saude_bucal.pagamento_esb_implantacao::text AS pagamento_implantacao,
                    'Municipal'::text AS nivel_repasse,
                    'Mensal'::text AS periodicidade,
                    '• CNES Atualizado 

			• Envio de produção através do SISAB'::text AS requisitos
                   FROM dados_publicos.egestor_financiamento_acoes_estrategicas_saude_bucal
                UNION
                 SELECT 'Programa Saúde na Escola (PSE)'::text AS acao_nome,
                    egestor_financiamento_acoes_estrategicas_outros.municipio_id_sus,
                    egestor_financiamento_acoes_estrategicas_outros.periodo_id,
                    egestor_financiamento_acoes_estrategicas_outros.pagamento_pse_municipal AS pagamento_total,
                    'NA'::text AS pagamento_implantacao,
                    'Municipal'::text AS nivel_repasse,
                    'Anual'::text AS periodicidade,
                    '• Pactuação bianual 

				• CNES Atualizado 

				• Envio de produção através do SISAB ( realização das ações de Promoção à saúde estabelecidas)'::text AS requisitos
                   FROM dados_publicos.egestor_financiamento_acoes_estrategicas_outros
                UNION
                 SELECT 'Equipe de Consultório na Rua (eCR)'::text AS acao_nome,
                    egestor_financiamento_acoes_estrategicas_consultorio_rua.municipio_id_sus,
                    egestor_financiamento_acoes_estrategicas_consultorio_rua.periodo_id,
                    egestor_financiamento_acoes_estrategicas_consultorio_rua.pagamento_total,
                    'NA'::text AS pagamento_implantacao,
                    'Municipal'::text AS nivel_repasse,
                    'Mensal'::text AS periodicidade,
                    '• CNES Atualizado 

			• Envio de produção através do SISAB'::text AS requisitos
                   FROM dados_publicos.egestor_financiamento_acoes_estrategicas_consultorio_rua
                UNION
                 SELECT 'Equipe de Atenção Básica Prisional (eABP)'::text AS acao_nome,
                    egestor_financiamento_acoes_estrategicas_outros.municipio_id_sus,
                    egestor_financiamento_acoes_estrategicas_outros.periodo_id,
                    egestor_financiamento_acoes_estrategicas_outros.pagamento_eabp_municipal AS pagamento_total,
                    'NA'::text AS pagamento_implantacao,
                    'Municipal'::text AS nivel_repasse,
                    'Mensal'::text AS periodicidade,
                    '• CNES Atualizado 

			• Envio de produção através do SISAB'::text AS requisitos
                   FROM dados_publicos.egestor_financiamento_acoes_estrategicas_outros
                UNION
                 SELECT 'Estratégia de Agentes Comunitários de Saúde (ACS)'::text AS acao_nome,
                    egestor_financiamento_acoes_estrategicas_acs.municipio_id_sus,
                    egestor_financiamento_acoes_estrategicas_acs.periodo_id,
                    egestor_financiamento_acoes_estrategicas_acs.pagamento_total_acs AS pagamento_total,
                    'NA'::text AS pagamento_implantacao,
                    'Por profissional'::text AS nivel_repasse,
                    'Anual'::text AS periodicidade,
                    '• CNES Atualizado 

			• Envio de produção através do SISAB'::text AS requisitos
                   FROM dados_publicos.egestor_financiamento_acoes_estrategicas_acs
                UNION
                 SELECT 'Microscopista'::text AS acao_nome,
                    egestor_financiamento_acoes_estrategicas_outros.municipio_id_sus,
                    egestor_financiamento_acoes_estrategicas_outros.periodo_id,
                    egestor_financiamento_acoes_estrategicas_outros.pagamento_microscopista_regular AS pagamento_total,
                    'NA'::text AS pagamento_implantacao,
                    'Por profissional'::text AS nivel_repasse,
                    'Mensal'::text AS periodicidade,
                    '• CNES Atualizado 

			• Envio de produção através do SISAB'::text AS requisitos
                   FROM dados_publicos.egestor_financiamento_acoes_estrategicas_outros
                UNION
                 SELECT 'Residência Profissional'::text AS acao_nome,
                    egestor_financiamento_acoes_estrategicas_residencia_profissiona.municipio_id_sus,
                    egestor_financiamento_acoes_estrategicas_residencia_profissiona.periodo_id,
                    egestor_financiamento_acoes_estrategicas_residencia_profissiona.pagamento_total,
                    'NA'::text AS pagamento_implantacao,
                    'Por Equipe'::text AS nivel_repasse,
                    'Mensal'::text AS periodicidade,
                    '• CNES Atualizado 

			• Envio de produção através do SISAB

			• Programa de Residência  em situação Regular'::text AS requisitos
                   FROM dados_publicos.egestor_financiamento_acoes_estrategicas_residencia_profissiona
                UNION
                 SELECT 'Unidade Odontológica Móvel (UOM)'::text AS acao_nome,
                    egestor_financiamento_acoes_estrategicas_saude_bucal.municipio_id_sus,
                    egestor_financiamento_acoes_estrategicas_saude_bucal.periodo_id,
                    egestor_financiamento_acoes_estrategicas_saude_bucal.pagamento_custeio_uom AS pagamento_total,
                    'NA'::text AS pagamento_implantacao,
                    'Municipal'::text AS nivel_repasse,
                    'Mensal'::text AS periodicidade,
                    '• CNES Atualizado 

					• Todos os profissionais da UOM devem estar cadastrados também na eSB com a qual compartilham carga horária;

					• Envio de produção através do SISAB'::text AS requisitos
                   FROM dados_publicos.egestor_financiamento_acoes_estrategicas_saude_bucal
                UNION
                 SELECT 'Centro de Especialidades Odontológicas (CEO)'::text AS acao_nome,
                    egestor_financiamento_acoes_estrategicas_saude_bucal.municipio_id_sus,
                    egestor_financiamento_acoes_estrategicas_saude_bucal.periodo_id,
                    egestor_financiamento_acoes_estrategicas_saude_bucal.pagamento_ceo_municipal AS pagamento_total,
                    'NA'::text AS pagamento_implantacao,
                    'Municipal'::text AS nivel_repasse,
                    'Mensal'::text AS periodicidade,
                    '• CNES Atualizado

						• Ter realizado produção mínima mensal de acordo com a Portaria de Consolidação nº 6/2017, e informar no SIA/SUS as produções.

						• Não ultrapassar o período de 2 (dois) meses alternados e 3 (três) consecutivos sem realizar o lançamento da produção.'::text AS requisitos
                   FROM dados_publicos.egestor_financiamento_acoes_estrategicas_saude_bucal
                UNION
                 SELECT 'Laboratório Regional de Prótese Dentária (LRPD)'::text AS acao_nome,
                    egestor_financiamento_acoes_estrategicas_saude_bucal.municipio_id_sus,
                    egestor_financiamento_acoes_estrategicas_saude_bucal.periodo_id,
                    egestor_financiamento_acoes_estrategicas_saude_bucal.pagamento_lrpd_municipal AS pagamento_total,
                    'NA'::text AS pagamento_implantacao,
                    'Municipal'::text AS nivel_repasse,
                    'Mensal'::text AS periodicidade,
                    '• Cadastramento no CNES  ( Possuir no mínimo, um profissional com o CBO: 3224-10 

						• Protético Dentário e/ou CBO: 2232 

						• Cirurgião-Dentista (qualquer CBO dentro desta família), ambos com carga horária ambulatorial SUS e realizar, ao menos, um dos procedimentos definidos)
						
						• Envio de produção mensal através do (SIA/SUS)
						A produção executada deve ser enviada mensalmente, por meio do Sistema de Informações Ambulatoriais do SUS (SIA/SUS), utilizando os instrumentos de registro direcionados a cada especialidade. É necessário informar a produção no SIA/SUS. Após 3 (três)
						meses sem informar a produção, o estabelecimento será
						suspenso.

						• Realizar produção compatível com a faixa de produção de credenciamento (I - entre 20 a 50 próteses por mês; II - entre 51 a 80 próteses por mês; III- entre 81 e 120 próteses por mês; IV - acima de 120 próteses por mês)'::text AS requisitos
                   FROM dados_publicos.egestor_financiamento_acoes_estrategicas_saude_bucal
                UNION
                 SELECT 'Programa Academia da Saúde'::text AS acao_nome,
                    egestor_financiamento_acoes_estrategicas_academia_saude.municipio_id_sus,
                    egestor_financiamento_acoes_estrategicas_academia_saude.periodo_id,
                    egestor_financiamento_acoes_estrategicas_academia_saude.pagamento_total,
                    'NA'::text AS pagamento_implantacao,
                    'Municipal'::text AS nivel_repasse,
                    'Mensal'::text AS periodicidade,
                    '• CNES Atualizado (sendo pelo menos 1 (um) profissional com carga horária de 40 (quarenta) horas semanais ou, no mínimo, 2 (dois) profissionais com carga horária de 20 (vinte) horas semanais cada; 

						• Acessar no mesmo sistema do Ministério da Saúde onde a proposta de construção foi cadastrada e inclua o (s) SCNES do polo, para fins de comprovação do funcionamento da unidade de saúde; 

						• Envio de produção através do SISAB'::text AS requisitos
                   FROM dados_publicos.egestor_financiamento_acoes_estrategicas_academia_saude
                UNION
                 SELECT 'Unidade Básica de Saúde Fluvial (UBSF)'::text AS acao_nome,
                    egestor_financiamento_acoes_estrategicas_ubs_fluvial.municipio_id_sus,
                    egestor_financiamento_acoes_estrategicas_ubs_fluvial.periodo_id,
                    egestor_financiamento_acoes_estrategicas_ubs_fluvial.pagamento_ubsf_custeio AS pagamento_total,
                    'NA'::text AS pagamento_implantacao,
                    'Municipal'::text AS nivel_repasse,
                    'Mensal'::text AS periodicidade,
                    '• CNES Atualizado

						• Envio de produção através do SISAB'::text AS requisitos
                   FROM dados_publicos.egestor_financiamento_acoes_estrategicas_ubs_fluvial
                UNION
                 SELECT 'Equipe de Saúde da Família Ribeirinha (eSFR)'::text AS acao_nome,
                    egestor_financiamento_acoes_estrategicas_ribeirinha.municipio_id_sus,
                    egestor_financiamento_acoes_estrategicas_ribeirinha.periodo_id,
                    egestor_financiamento_acoes_estrategicas_ribeirinha.pagamento_esfrb AS pagamento_total,
                    'NA'::text AS pagamento_implantacao,
                    'Por Equipe'::text AS nivel_repasse,
                    'Mensal'::text AS periodicidade,
                    '• CNES Atualizado

						• Envio de produção através do SISAB '::text AS requisitos
                   FROM dados_publicos.egestor_financiamento_acoes_estrategicas_ribeirinha
                UNION
                 SELECT 'Atenção Integral à Saúde dos Adolescentes em Situação de Privação de Liberdade'::text AS acao_nome,
                    egestor_financiamento_acoes_estrategicas_outros.municipio_id_sus,
                    egestor_financiamento_acoes_estrategicas_outros.periodo_id,
                    egestor_financiamento_acoes_estrategicas_outros.pagamento_equipes_adolescentes_socioeducacao AS pagamento_total,
                    'NA'::text AS pagamento_implantacao,
                    'Municipal'::text AS nivel_repasse,
                    'Mensal'::text AS periodicidade,
                    '• CNES Atualizado

						• Envio de produção através do SISAB'::text AS requisitos
                   FROM dados_publicos.egestor_financiamento_acoes_estrategicas_outros) dados
             JOIN listas_de_codigos.periodos p ON p.id = dados.periodo_id
             JOIN listas_de_codigos.municipios m ON m.id_sus = dados.municipio_id_sus
          WHERE p.data_inicio > (CURRENT_DATE - '1 year'::interval) AND dados.pagamento_total > 0::numeric
          ORDER BY (concat(m.uf_sigla, ' - ', m.nome)), dados.acao_nome, p.data_inicio DESC
        )
 SELECT v.municipio_uf,
    v.acao_nome,
    sum(v.pagamento_total) AS acumulado_12meses,
    v.nivel_repasse,
    v.periodicidade,
    max(v.data_inicio) AS ultimo_pagamento,
    v.requisitos,
    CURRENT_TIMESTAMP AS atualizacao_data
   FROM visao v
  GROUP BY v.acao_nome, v.municipio_uf, v.nivel_repasse, v.periodicidade, v.requisitos
WITH DATA;

-- View indexes:
CREATE INDEX acoes_estrategicas_vigente_agrupada_municipio_uf_idx ON impulso_previne.acoes_estrategicas_vigente_agrupada USING btree (municipio_uf);