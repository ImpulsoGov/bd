/**************************************************************************

                 CONSULTA DE CÓDIGOS DE ENDEREÇAMENTO POSTAL 
                     SEM INFORMAÇÃO DE ENDEREÇO ASSOCIADA

 **************************************************************************/


CREATE OR REPLACE VIEW configuracoes.ceps_pendentes AS
SELECT DISTINCT aih.usuario_residencia_cep AS id_cep
FROM dados_publicos.sihsus_aih_reduzida_disseminacao aih
LEFT JOIN listas_de_codigos.ceps cep
ON aih.usuario_residencia_cep = cep.id_cep
WHERE cep.id_cep IS NULL
;
