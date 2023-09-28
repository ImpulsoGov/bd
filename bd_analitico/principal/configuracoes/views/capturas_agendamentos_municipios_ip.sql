-- configuracoes.capturas_agendamentos_municipios_ip source

CREATE OR REPLACE VIEW configuracoes.capturas_agendamentos_municipios_ip
AS SELECT capturas_agendamentos.operacao_id,
    capturas_agendamentos.periodo_id,
    capturas_agendamentos.unidade_geografica_tipo,
    capturas_agendamentos.unidade_geografica_id,
    capturas_agendamentos.unidade_geografica_id_ibge,
    capturas_agendamentos.unidade_geografica_id_sus,
    capturas_agendamentos.tabela_destino,
    capturas_agendamentos.capturar_apos,
    capturas_agendamentos.parametros,
    capturas_agendamentos.periodo_data_inicio,
    capturas_agendamentos.uf_sigla,
    capturas_agendamentos.periodo_codigo,
    capturas_agendamentos.atualizacao_retroativa,
    capturas_agendamentos.periodo_data_fim
   FROM configuracoes.capturas_agendamentos
  WHERE (capturas_agendamentos.operacao_id = ANY (ARRAY['063c6b40-ab9a-7459-b59c-6ebaa34f1bfd'::uuid, '063b5cf8-34d1-744d-8f96-353d4f199171'::uuid, '0642f1cd-083b-783d-b855-c837cfa7439b'::uuid, '0643da4f-8562-7520-ba7a-606062b1e1e1'::uuid, '063d29a0-a77c-7f0b-b4d2-1274ffe59619'::uuid])) AND (capturas_agendamentos.unidade_geografica_id_sus::text ~~ ANY (ARRAY['211280'::text, '150030'::text, '231325'::text, '140015'::text, '313190'::text, '350190'::text, '220375'::text, '521975'::text, '310230'::text, '312737'::text, '410720'::text, '220930'::text, '220310'::text, '160040'::text, '431846'::text, '313170'::text, '291790'::text, '240145'::text, '210280'::text, '411190'::text, '250215'::text, '291750'::text, '130240'::text, '160050'::text, '311440'::text, '210215'::text, '171865'::text, '317130'::text, '315210'::text, '520920'::text, '316935'::text, '315570'::text, '521308'::text, '261485'::text, '260060'::text, '210735'::text, '355060'::text, '120070'::text, '352620'::text, '150630'::text, '210635'::text, '220760'::text, '221062'::text, '230170'::text, '230625'::text, '250933'::text, '260180'::text, '291110'::text, '291370'::text, '291850'::text, '292467'::text, '292490'::text, '311480'::text, '330513'::text, '351230'::text, '351340'::text, '411120'::text, '411300'::text, '420190'::text, '421280'::text, '430410'::text, '521220'::text, '120025'::text, '120080'::text, '150510'::text, '171840'::text, '210590'::text, '211080'::text, '211300'::text, '220130'::text, '220323'::text, '231340'::text, '241070'::text, '250200'::text, '290580'::text, '291845'::text, '312510'::text, '317040'::text, '320430'::text, '330025'::text, '353930'::text, '420360'::text, '430020'::text, '430045'::text, '430790'::text, '431370'::text]));