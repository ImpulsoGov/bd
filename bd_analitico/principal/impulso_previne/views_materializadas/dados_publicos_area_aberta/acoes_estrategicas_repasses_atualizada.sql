SELECT concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
    p.codigo,
    p.data_inicio,
    dados.acao_nome,
    dados.pagamento_total,
    CURRENT_TIMESTAMP AS atualizacao_data
   FROM ( SELECT 'Programa Academia da Saúde'::text AS acao_nome,
            egestor_financiamento_acoes_estrategicas_academia_saude.municipio_id_sus,
            egestor_financiamento_acoes_estrategicas_academia_saude.periodo_id,
            egestor_financiamento_acoes_estrategicas_academia_saude.pagamento_total
           FROM dados_publicos.egestor_financiamento_acoes_estrategicas_academia_saude
        UNION
         SELECT 'Equipe de Consultório na Rua (eCR)'::text AS acao_nome,
            egestor_financiamento_acoes_estrategicas_consultorio_rua.municipio_id_sus,
            egestor_financiamento_acoes_estrategicas_consultorio_rua.periodo_id,
            egestor_financiamento_acoes_estrategicas_consultorio_rua.pagamento_total
           FROM dados_publicos.egestor_financiamento_acoes_estrategicas_consultorio_rua
        UNION
         SELECT 'Programa Saúde na Escola (PSE)'::text AS acao_nome,
            egestor_financiamento_acoes_estrategicas_outros.municipio_id_sus,
            egestor_financiamento_acoes_estrategicas_outros.periodo_id,
            egestor_financiamento_acoes_estrategicas_outros.pagamento_pse_municipal
           FROM dados_publicos.egestor_financiamento_acoes_estrategicas_outros
        UNION
         SELECT 'Equipe de Atenção Básica Prisional (eABP)'::text AS acao_nome,
            egestor_financiamento_acoes_estrategicas_outros.municipio_id_sus,
            egestor_financiamento_acoes_estrategicas_outros.periodo_id,
            egestor_financiamento_acoes_estrategicas_outros.pagamento_eabp_municipal
           FROM dados_publicos.egestor_financiamento_acoes_estrategicas_outros
        UNION
         SELECT 'Equipe de Saúde da Família Ribeirinha (eSFR)'::text AS acao_nome,
            egestor_financiamento_acoes_estrategicas_ribeirinha.municipio_id_sus,
            egestor_financiamento_acoes_estrategicas_ribeirinha.periodo_id,
            egestor_financiamento_acoes_estrategicas_ribeirinha.pagamento_esfrb
           FROM dados_publicos.egestor_financiamento_acoes_estrategicas_ribeirinha
        UNION
         SELECT 'Equipes de Saúde Bucal'::text AS acao_nome,
            egestor_financiamento_acoes_estrategicas_saude_bucal.municipio_id_sus,
            egestor_financiamento_acoes_estrategicas_saude_bucal.periodo_id,
            egestor_financiamento_acoes_estrategicas_saude_bucal.pagamento_esb_custeio
           FROM dados_publicos.egestor_financiamento_acoes_estrategicas_saude_bucal
        UNION
         SELECT 'Programa Saúde na Hora'::text AS acao_nome,
            egestor_financiamento_acoes_estrategicas_saude_hora.municipio_id_sus,
            egestor_financiamento_acoes_estrategicas_saude_hora.periodo_id,
            egestor_financiamento_acoes_estrategicas_saude_hora.pagamento_custeio
           FROM dados_publicos.egestor_financiamento_acoes_estrategicas_saude_hora
        UNION
         SELECT 'Unidade Básica de Saúde Fluvial (UBSF)'::text AS acao_nome,
            egestor_financiamento_acoes_estrategicas_ubs_fluvial.municipio_id_sus,
            egestor_financiamento_acoes_estrategicas_ubs_fluvial.periodo_id,
            egestor_financiamento_acoes_estrategicas_ubs_fluvial.pagamento_ubsf_custeio
           FROM dados_publicos.egestor_financiamento_acoes_estrategicas_ubs_fluvial
        UNION
         SELECT 'Estratégia de Agentes Comunitários de Saúde (ACS)'::text AS acao_nome,
            egestor_financiamento_acoes_estrategicas_acs.municipio_id_sus,
            egestor_financiamento_acoes_estrategicas_acs.periodo_id,
            egestor_financiamento_acoes_estrategicas_acs.pagamento_total_acs AS pagamento_total
           FROM dados_publicos.egestor_financiamento_acoes_estrategicas_acs
        UNION
         SELECT 'Informatiza APS'::text AS acao_nome,
            egestor_financiamento_acoes_estrategicas_informatiza_aps.municipio_id_sus,
            egestor_financiamento_acoes_estrategicas_informatiza_aps.periodo_id,
            egestor_financiamento_acoes_estrategicas_informatiza_aps.pagamento_total
           FROM dados_publicos.egestor_financiamento_acoes_estrategicas_informatiza_aps
        UNION
         SELECT 'Unidade odontológica Móvel (UOM)'::text AS acao_nome,
            esb.municipio_id_sus,
            esb.periodo_id,
            esb.pagamento_custeio_uom
           FROM dados_publicos.egestor_financiamento_acoes_estrategicas_saude_bucal esb
        UNION
         SELECT 'Laboratório Regional de Prótese Dentária (LRPD)'::text AS acao_nome,
            esb.municipio_id_sus,
            esb.periodo_id,
            esb.pagamento_lrpd_municipal
           FROM dados_publicos.egestor_financiamento_acoes_estrategicas_saude_bucal esb
        UNION
         SELECT 'Centro de Especialidades Odontológicas (CEO)'::text AS acao_nome,
            esb.municipio_id_sus,
            esb.periodo_id,
            esb.pagamento_ceo_municipal
           FROM dados_publicos.egestor_financiamento_acoes_estrategicas_saude_bucal esb
        UNION
         SELECT 'Atenção Integral à Saúde dos Adolescentes em Situação de Privação de Liberdade'::text AS acao_nome,
            eao.municipio_id_sus,
            eao.periodo_id,
            eao.pagamento_equipes_adolescentes_socioeducacao
           FROM dados_publicos.egestor_financiamento_acoes_estrategicas_outros eao
        UNION
         SELECT 'Microscopista'::text AS acao_nome,
            eao.municipio_id_sus,
            eao.periodo_id,
            eao.pagamento_microscopista_regular
           FROM dados_publicos.egestor_financiamento_acoes_estrategicas_outros eao
        UNION
         SELECT 'Residência Profissional'::text AS acao_nome,
            erp.municipio_id_sus,
            erp.periodo_id,
            erp.pagamento_total
           FROM dados_publicos.egestor_financiamento_acoes_estrategicas_residencia_profissiona erp) dados
     JOIN listas_de_codigos.periodos p ON p.id = dados.periodo_id
     JOIN listas_de_codigos.municipios m ON m.id_sus = dados.municipio_id_sus