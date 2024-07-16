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
 
			Observação: Esse critério tem prazo de 12 competências a partir da publicação da portaria de homologação de 

			adesão para o formato de funcionamento 60h Simplificado 

			• Ter a identidade visual do programa Saúde na Hora 

			• Envio de dados dentro dos parâmetros estabelecidos'::text AS requisitos
           FROM dados_publicos.egestor_financiamento_acoes_estrategicas_saude_hora
        UNION
         SELECT 'Informatiza APS'::text AS acao_nome,
            egestor_financiamento_acoes_estrategicas_informatiza_aps.municipio_id_sus,
            egestor_financiamento_acoes_estrategicas_informatiza_aps.periodo_id,
            egestor_financiamento_acoes_estrategicas_informatiza_aps.pagamento_total,
            'NA'::text AS pagamento_implantacao,
            'Equipe'::text AS nivel_repasse,
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
            'Anual'::text AS periodicidade,
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
            'Anual'::text AS periodicidade,
            '• CNES Atualizado 

			• Envio de produção através do SISAB'::text AS requisitos
           FROM dados_publicos.egestor_financiamento_acoes_estrategicas_outros
        UNION
         SELECT 'Estratégia de Agentes Comunitários de Saúde (ACS)'::text AS acao_nome,
            egestor_financiamento_acoes_estrategicas_acs.municipio_id_sus,
            egestor_financiamento_acoes_estrategicas_acs.periodo_id,
            egestor_financiamento_acoes_estrategicas_acs.pagamento_total_acs AS pagamento_total,
            'NA'::text AS pagamento_implantacao,
            'Municipal'::text AS nivel_repasse,
            'Anual'::text AS periodicidade,
            '• CNES Atualizado 

			• Envio de produção através do SISAB'::text AS requisitos
           FROM dados_publicos.egestor_financiamento_acoes_estrategicas_acs) dados
     JOIN listas_de_codigos.periodos p ON p.id = dados.periodo_id
     JOIN listas_de_codigos.municipios m ON m.id_sus = dados.municipio_id_sus
  WHERE p.data_inicio > (CURRENT_DATE - '1 year'::interval) AND dados.pagamento_total > 0::numeric
  ORDER BY (concat(m.uf_sigla, ' - ', m.nome)), dados.acao_nome, p.data_inicio DESC