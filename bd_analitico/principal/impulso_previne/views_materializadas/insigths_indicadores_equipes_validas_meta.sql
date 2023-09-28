-- impulso_previne.insigths_indicadores_equipes_validas_meta source

CREATE MATERIALIZED VIEW impulso_previne.insigths_indicadores_equipes_validas_meta
TABLESPACE pg_default
AS SELECT p.data_inicio,
    p.data_fim,
    tb1.periodo_codigo AS quadrimestre,
    tb1.municipio_id_sus,
    concat(m.nome, ' - ', m.uf_sigla) AS municipio_uf,
    m.macrorregiao_nome AS regiao,
    tb1.indicadores_nome,
    tb1.nota_porcentagem,
    ir.meta,
        CASE
            WHEN tb1.nota_porcentagem::double precision < ir.meta THEN true
            ELSE false
        END AS indicador_abaixo_meta
   FROM dados_publicos.sisab_indicadores_municipios_equipes_validas tb1
     LEFT JOIN listas_de_codigos.municipios m ON tb1.municipio_id_sus::bpchar = m.id_sus
     LEFT JOIN listas_de_codigos.periodos p ON tb1.periodo_id = p.id
     LEFT JOIN previne_brasil.indicadores_regras ir ON tb1.indicadores_regras_id = ir.id
WITH DATA;

-- View indexes:
CREATE INDEX insigths_indicadores_equipes_validas_meta_quadrimestre_idx ON impulso_previne.insigths_indicadores_equipes_validas_meta USING btree (quadrimestre);
CREATE INDEX insigths_indicadores_equipes_validas_meta_regiao_idx ON impulso_previne.insigths_indicadores_equipes_validas_meta USING btree (regiao, quadrimestre);