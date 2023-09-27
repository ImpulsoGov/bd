CREATE MATERIALIZED VIEW busca_ativa_gestantes AS
SELECT v3.estabelecimento_cnes,
    upper(v3.estabelecimento_nome) AS estabelecimento_nome,
    v3.equipe_ine,
    v3.equipe_nome,
    upper(v3.acs_nome) AS acs_nome,
    v3.acs_data_ultima_visita,
    v3.gestante_documento_cpf,
    v3.gestante_documento_cns,
    v3.gestante_nome,
    v3.gestante_data_de_nascimento,
    v3.gestante_telefone,
    v3.gestante_endereco,
    v3.gestante_dum,
    v3.gestante_idade_gestacional as gestante_idade_gestacional_atual,
    /* 
      Retorna a primeira idade gestacional:
      - Caso data de DUM preenchida corretamente (diferente de 3000), retorna (data de atendimento - data da dum)/7
      - Pega a primeira gestacional, sendo o campo nu_idade_gestacional_semanas da Ficha de atendimento individual (tb_fat_atendimento_individual), ordenado pelo atendimento 
      - Caso ambos estejam vazios, retorna nulo
    */
    CASE
        WHEN extract(year from v3.gestante_dum) <> 3000 THEN ((v3.atendimento_primeiro_data - v3.gestante_dum) / 7)
        WHEN v3.gestante_idade_gestacional_primeiro_atendimento IS NOT NULL THEN gestante_idade_gestacional_primeiro_atendimento
        ELSE NULL::integer
    END AS gestante_idade_gestacional_primeiro_atendimento,
    v3.gestante_primeira_dpp AS gestante_dpp,
    /* Considera DPP como data limite para registro de consultas de pré-natal */
    v3.gestante_primeira_dpp AS gestante_consulta_prenatal_data_limite,
    /* Retorna diferença de dias da data da atual (do script rodado) para a da dpp */
    v3.gestante_primeira_dpp::date - CURRENT_DATE AS gestante_dpp_dias_para,
    v3.gestante_consulta_prenatal_total,
    v3.gestante_consulta_prenatal_ultima_data,
    v3.gestante_consulta_prenatal_ultima_dias_desde,
    v3.atendimento_odontologico_realizado,
    v3.sorologia_hiv_solicitada,
    v3.sorologia_hiv_avaliada,
    v3.teste_rapido_hiv_realizado,
    v3.exame_hiv_realizado,
    v3.sorologia_sifilis_solicitada,
    v3.sorologia_sifilis_avaliada,
    v3.teste_rapido_sifilis_realizado,
    v3.exame_sifilis_realizado,
    /* Campo verdadeiro caso campo de exame_sifilis_realizado e exame_sifilis_hiv_realizado verdadeiros */
    v3.exame_hiv_realizado AND v3.exame_sifilis_realizado AS exame_sifilis_hiv_realizado,
        CASE
            WHEN v3.possui_registro_aborto IS TRUE THEN 'Sim'
            ELSE 'Não'
        END AS possui_registro_aborto,
        CASE
            WHEN v3.possui_registro_parto IS TRUE THEN 'Sim'
            ELSE 'Não'
        END AS possui_registro_parto,
   CURRENT_TIMESTAMP as criacao_data 
   FROM ( SELECT row_number() OVER (PARTITION BY v2.gestante_nome, v2.gestante_data_de_nascimento ORDER BY v2.atendimento_data DESC) AS r,
            v2.atendimento_data,
            v2.atendimento_primeiro_data,
            v2.estabelecimento_cnes,
            v2.estabelecimento_nome,
            v2.equipe_ine,
            v2.equipe_nome,
            v2.acs_nome,
            v2.acs_data_ultima_visita,
            v2.co_fat_cidadao_pec,
            v2.gestante_documento_cpf,
            v2.gestante_documento_cns,
            v2.gestante_nome,
            v2.gestante_data_de_nascimento,
            v2.gestante_data_nascimento_ts,
            v2.gestante_telefone,
            v2.gestante_endereco,
            v2.gestante_idade_gestacional,
            v2.gestante_dum_primeiro_atendimento as gestante_dum,
            v2.gestante_idade_gestacional_primeiro_atendimento,
            v2.gestante_consulta_prenatal_total,
            v2.gestante_consulta_prenatal_ultima_data,
            v2.gestante_consulta_prenatal_ultima_dias_desde,
            v2.gestante_primeira_dpp,
            /* 
              Avalia se foi realizado atendimento odontológico através das seguintes condições:
              Busca gestantes da Ficha de Atendimento Odontológico Individual (tb_fat_atendimento_odonto):
              - Cujo Nome da gestante na ficha tb_fat_cidadao_pec igual tb_fat_cidadao_pec (tfcp.no_cidadao unida pela ficha de atendimento individual) E Data de nascimento na ficha tb_fat_cidadao_pec igual a tb_dim_tempo (tempocidadaopec.dt_registro unida pela ficha cidadao pec pelo tempo de nascimento)
              - Data de registro maior ou igual data da DUM 
              - Data de registro menor ou igual data do DPP
            */
            (
                SELECT count(*) > 0 AS bool
                FROM tb_fat_atendimento_odonto otfodont
                JOIN tb_dim_cbo otdcbo ON otdcbo.co_seq_dim_cbo = otfodont.co_dim_cbo_1
                JOIN tb_dim_tempo otdtempo ON otdtempo.co_seq_dim_tempo = otfodont.co_dim_tempo
                WHERE (otfodont.co_fat_cidadao_pec IN ( SELECT tfcodonto.co_seq_fat_cidadao_pec
                FROM tb_fat_cidadao_pec tfcodonto
                WHERE tfcodonto.no_cidadao= v2.gestante_nome AND tfcodonto.co_dim_tempo_nascimento = v2.gestante_data_nascimento_ts)) AND otdcbo.nu_cbo ~~ '2232%' AND otdtempo.dt_registro >= v2.gestante_dum_primeiro_atendimento AND otdtempo.dt_registro <= v2.gestante_primeira_dpp
            ) AS atendimento_odontologico_realizado,
            /* 
              Avalia se exame de HIV a partir das seguintes condições:
              União de Grupo 1 - Busca Ficha de Procedimento Individual (tb_fat_proced_atend_proced):
              - Cujo Nome da gestante na ficha tb_fat_cidadao_pec igual tb_fat_cidadao_pec (tfcp.no_cidadao unida pela ficha de atendimento individual) E Data de nascimento na ficha tb_fat_cidadao_pec igual a tb_dim_tempo (tempocidadaopec.dt_registro unida pela ficha cidadao pec pelo tempo de nascimento)
              - Número de CBO nessa [lista](https://impulsogov.notion.site/Lista-HIV-tdcbo-nu_cbo-541cedbfff83485e83d56c98d3ed5d1e)
              - Número de Procedimento nessa [lista](https://impulsogov.notion.site/Lista-HIV-tdp-co_proced-18146819f62645e1a337034ca9dac3ee)
              - Data de registro maior ou igual data da DUM 
              - Data de registro menor ou igual data do DPP
              Com Grupo 2 - Pega da Ficha de Atendimento Individual (tb_fat_atd_ind_procedimentos):
              - Cujo Nome da gestante na ficha tb_fat_cidadao_pec igual tb_fat_cidadao_pec (tfcp.no_cidadao unida pela ficha de atendimento individual) E Data de nascimento na ficha tb_fat_cidadao_pec igual a tb_dim_tempo (tempocidadaopec.dt_registro unida pela ficha cidadao pec pelo tempo de nascimento)
              - Número de CBO nessa [lista](https://impulsogov.notion.site/Lista-HIV-tdcbo-nu_cbo-541cedbfff83485e83d56c98d3ed5d1e)
              - Número de Procedimento nessa [lista](https://impulsogov.notion.site/Lista-HIV-tdp-co_proced-18146819f62645e1a337034ca9dac3ee)
              - Data de registro maior ou igual data da DUM 
              - Data de registro menor ou igual data do DPP
            */
            ( SELECT count(*) > 0
                           FROM tb_fat_atd_ind_procedimentos tfaip
                             JOIN tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfaip.co_dim_procedimento_solicitado
                             JOIN tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfaip.co_dim_cbo_1
                             JOIN tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfaip.co_dim_tempo
                          WHERE (tfaip.co_fat_cidadao_pec IN ( SELECT tfcprocedhiv.co_seq_fat_cidadao_pec
                                   FROM tb_fat_cidadao_pec tfcprocedhiv
                                  WHERE tfcprocedhiv.no_cidadao::text = v2.gestante_nome::text AND tfcprocedhiv.co_dim_tempo_nascimento = v2.gestante_data_nascimento_ts)) AND (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND tdtempo.dt_registro >= v2.gestante_dum_primeiro_atendimento AND tdtempo.dt_registro <= v2.gestante_primeira_dpp AND (tdp.co_proced::text = ANY (ARRAY['0202030300'::text, 'ABEX018'::text]))
              ) AS sorologia_hiv_solicitada,
              ( SELECT count(*) > 0
                           FROM tb_fat_atd_ind_procedimentos tfaip
                             JOIN tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfaip.co_dim_procedimento_avaliado
                             JOIN tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfaip.co_dim_cbo_1
                             JOIN tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfaip.co_dim_tempo
                          WHERE (tfaip.co_fat_cidadao_pec IN ( SELECT tfcprocedhiv.co_seq_fat_cidadao_pec
                                   FROM tb_fat_cidadao_pec tfcprocedhiv
                                  WHERE tfcprocedhiv.no_cidadao::text = v2.gestante_nome::text AND tfcprocedhiv.co_dim_tempo_nascimento = v2.gestante_data_nascimento_ts)) AND (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND tdtempo.dt_registro >= v2.gestante_dum_primeiro_atendimento AND tdtempo.dt_registro <= v2.gestante_primeira_dpp AND (tdp.co_proced::text = ANY (ARRAY['0202030300'::text, 'ABEX018'::text]))
              ) AS sorologia_hiv_avaliada,
              ( SELECT count(*) > 0
                           FROM tb_fat_proced_atend_proced tfpap
                             JOIN tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfpap.co_dim_procedimento
                             JOIN tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfpap.co_dim_cbo
                             JOIN tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfpap.co_dim_tempo
                          WHERE (tfpap.co_fat_cidadao_pec IN ( SELECT tfcprocedhiv.co_seq_fat_cidadao_pec
                                   FROM tb_fat_cidadao_pec tfcprocedhiv
                                  WHERE tfcprocedhiv.no_cidadao::text = v2.gestante_nome::text AND tfcprocedhiv.co_dim_tempo_nascimento = v2.gestante_data_nascimento_ts)) AND (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND tdtempo.dt_registro >= v2.gestante_dum_primeiro_atendimento AND tdtempo.dt_registro <= v2.gestante_primeira_dpp AND (tdp.co_proced::text = ANY (ARRAY['0214010058'::text, '0214010040'::text, 'ABPG024'::text]))
            ) AS teste_rapido_hiv_realizado,
            ( SELECT count(*) > 0
                   FROM ( SELECT tdp.co_proced
                           FROM tb_fat_proced_atend_proced tfpap
                             JOIN tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfpap.co_dim_procedimento
                             JOIN tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfpap.co_dim_cbo
                             JOIN tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfpap.co_dim_tempo
                          WHERE (tfpap.co_fat_cidadao_pec IN ( SELECT tfcprocedhiv.co_seq_fat_cidadao_pec
                                   FROM tb_fat_cidadao_pec tfcprocedhiv
                                  WHERE tfcprocedhiv.no_cidadao::text = v2.gestante_nome::text AND tfcprocedhiv.co_dim_tempo_nascimento = v2.gestante_data_nascimento_ts)) AND (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND tdtempo.dt_registro >= v2.gestante_dum_primeiro_atendimento AND tdtempo.dt_registro <= v2.gestante_primeira_dpp AND (tdp.co_proced::text = ANY (ARRAY['0214010058'::text, '0214010040'::text, 'ABPG024'::text]))
                        UNION ALL
                         SELECT tdp.co_proced
                           FROM tb_fat_atd_ind_procedimentos tfaip
                             JOIN tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfaip.co_dim_procedimento_avaliado
                             JOIN tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfaip.co_dim_cbo_1
                             JOIN tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfaip.co_dim_tempo
                          WHERE (tfaip.co_fat_cidadao_pec IN ( SELECT tfcprocedhiv.co_seq_fat_cidadao_pec
                                   FROM tb_fat_cidadao_pec tfcprocedhiv
                                  WHERE tfcprocedhiv.no_cidadao::text = v2.gestante_nome::text AND tfcprocedhiv.co_dim_tempo_nascimento = v2.gestante_data_nascimento_ts)) AND (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND tdtempo.dt_registro >= v2.gestante_dum_primeiro_atendimento AND tdtempo.dt_registro <= v2.gestante_primeira_dpp AND (tdp.co_proced::text = ANY (ARRAY['0202030300'::text, 'ABEX018'::text]))) e1
              ) AS exame_hiv_realizado,
            /*  
              Avalia se exame de Sifilis a partir das seguintes condições:
              União de Grupo 1 - Busca Ficha de Procedimento Individual (tb_fat_proced_atend_proced):
              - Cujo Nome da gestante na ficha tb_fat_cidadao_pec igual tb_fat_cidadao_pec (tfcp.no_cidadao unida pela ficha de atendimento individual) E Data de nascimento na ficha tb_fat_cidadao_pec igual a tb_dim_tempo (tempocidadaopec.dt_registro unida pela ficha cidadao pec pelo tempo de nascimento)
              - Número de CBO nessa [lista](https://impulsogov.notion.site/Lista-tdcbo-nu_cbo-541cedbfff83485e83d56c98d3ed5d1e)
              - Número de Procedimento nessa [lista](https://impulsogov.notion.site/Lista-Sifilis-tdp-co_proced-1fda8bee5ecc49c3a66c7f737d9a91a5)
              - Data de registro maior ou igual data da DUM 
              - Data de registro menor ou igual data do DPP
              Com Grupo 2 - Pega da Ficha de Atendimento Individual (tb_fat_atd_ind_procedimentos):
              - Cujo Nome da gestante na ficha tb_fat_cidadao_pec igual tb_fat_cidadao_pec (tfcp.no_cidadao unida pela ficha de atendimento individual) E Data de nascimento na ficha tb_fat_cidadao_pec igual a tb_dim_tempo (tempocidadaopec.dt_registro unida pela ficha cidadao pec pelo tempo de nascimento)
              - Número de CBO nessa [lista](https://impulsogov.notion.site/Lista-tdcbo-nu_cbo-541cedbfff83485e83d56c98d3ed5d1e)
              - Número de Procedimento nessa [lista](https://impulsogov.notion.site/Lista-Sifilis-tdp-co_proced-1fda8bee5ecc49c3a66c7f737d9a91a5)
              - Data de registro maior ou igual data da DUM 
              - Data de registro menor ou igual data do DPP
              */
              ( SELECT count(*) > 0
                           FROM tb_fat_atd_ind_procedimentos tfaip
                             JOIN tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfaip.co_dim_procedimento_solicitado
                             JOIN tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfaip.co_dim_cbo_1
                             JOIN tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfaip.co_dim_tempo
                          WHERE (tfaip.co_fat_cidadao_pec IN ( SELECT tfcprocedsilfilis.co_seq_fat_cidadao_pec
                                   FROM tb_fat_cidadao_pec tfcprocedsilfilis
                                  WHERE tfcprocedsilfilis.no_cidadao::text = v2.gestante_nome::text AND tfcprocedsilfilis.co_dim_tempo_nascimento = v2.gestante_data_nascimento_ts)) AND (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND tdtempo.dt_registro >= v2.gestante_dum_primeiro_atendimento AND tdtempo.dt_registro <= v2.gestante_primeira_dpp AND (tdp.co_proced::text = ANY (ARRAY['0202031110'::text, '0202031179'::text, 'ABEX019'::text]))
            ) AS sorologia_sifilis_solicitada,
            ( SELECT count(*) > 0
                           FROM tb_fat_atd_ind_procedimentos tfaip
                             JOIN tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfaip.co_dim_procedimento_avaliado
                             JOIN tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfaip.co_dim_cbo_1
                             JOIN tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfaip.co_dim_tempo
                          WHERE (tfaip.co_fat_cidadao_pec IN ( SELECT tfcprocedsilfilis.co_seq_fat_cidadao_pec
                                   FROM tb_fat_cidadao_pec tfcprocedsilfilis
                                  WHERE tfcprocedsilfilis.no_cidadao::text = v2.gestante_nome::text AND tfcprocedsilfilis.co_dim_tempo_nascimento = v2.gestante_data_nascimento_ts)) AND (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND tdtempo.dt_registro >= v2.gestante_dum_primeiro_atendimento AND tdtempo.dt_registro <= v2.gestante_primeira_dpp AND (tdp.co_proced::text = ANY (ARRAY['0202031110'::text, '0202031179'::text, 'ABEX019'::text]))
            ) AS sorologia_sifilis_avaliada,
            ( SELECT count(*) > 0
                           FROM tb_fat_proced_atend_proced tfpap
                             JOIN tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfpap.co_dim_procedimento
                             JOIN tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfpap.co_dim_cbo
                             JOIN tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfpap.co_dim_tempo
                          WHERE (tfpap.co_fat_cidadao_pec IN ( SELECT tfcprocedsilfilis.co_seq_fat_cidadao_pec
                                   FROM tb_fat_cidadao_pec tfcprocedsilfilis
                                  WHERE tfcprocedsilfilis.no_cidadao::text = v2.gestante_nome::text AND tfcprocedsilfilis.co_dim_tempo_nascimento = v2.gestante_data_nascimento_ts)) AND (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND tdtempo.dt_registro >= v2.gestante_dum_primeiro_atendimento AND tdtempo.dt_registro <= v2.gestante_primeira_dpp AND (tdp.co_proced::text = ANY (ARRAY['0214010074'::text, '0214010082'::text, 'ABPG026'::text]))
            ) AS teste_rapido_sifilis_realizado,
            ( SELECT count(*) > 0
                   FROM ( SELECT tdp.co_proced
                           FROM tb_fat_proced_atend_proced tfpap
                             JOIN tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfpap.co_dim_procedimento
                             JOIN tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfpap.co_dim_cbo
                             JOIN tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfpap.co_dim_tempo
                          WHERE (tfpap.co_fat_cidadao_pec IN ( SELECT tfcprocedsilfilis.co_seq_fat_cidadao_pec
                                   FROM tb_fat_cidadao_pec tfcprocedsilfilis
                                  WHERE tfcprocedsilfilis.no_cidadao::text = v2.gestante_nome::text AND tfcprocedsilfilis.co_dim_tempo_nascimento = v2.gestante_data_nascimento_ts)) AND (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND tdtempo.dt_registro >= v2.gestante_dum_primeiro_atendimento AND tdtempo.dt_registro <= v2.gestante_primeira_dpp AND (tdp.co_proced::text = ANY (ARRAY['0214010074'::text, '0214010082'::text, 'ABPG026'::text]))
                        UNION ALL
                         SELECT tdp.co_proced
                           FROM tb_fat_atd_ind_procedimentos tfaip
                             JOIN tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfaip.co_dim_procedimento_avaliado
                             JOIN tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfaip.co_dim_cbo_1
                             JOIN tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfaip.co_dim_tempo
                          WHERE (tfaip.co_fat_cidadao_pec IN ( SELECT tfcprocedsilfilis.co_seq_fat_cidadao_pec
                                   FROM tb_fat_cidadao_pec tfcprocedsilfilis
                                  WHERE tfcprocedsilfilis.no_cidadao::text = v2.gestante_nome::text AND tfcprocedsilfilis.co_dim_tempo_nascimento = v2.gestante_data_nascimento_ts)) AND (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND tdtempo.dt_registro >= v2.gestante_dum_primeiro_atendimento AND tdtempo.dt_registro <= v2.gestante_primeira_dpp AND (tdp.co_proced::text = ANY (ARRAY['0202031110'::text, '0202031179'::text, 'ABEX019'::text]))) e2
            ) AS exame_sifilis_realizado,
     
            /* 
              Avalia se houve aborto da gestação a partir das seguintes condições:
              - Nome da gestante na ficha tb_fat_cidadao_pec igual tb_fat_cidadao_pec (tfcp.no_cidadao unida pela ficha de atendimento individual) E Data de nascimento na ficha tb_fat_cidadao_pec igual a tb_dim_tempo (tempocidadaopec.dt_registro unida pela ficha cidadao pec pelo tempo de nascimento)
              - Número de CIAPS nessa [lista](https://impulsogov.notion.site/Lista-de-tdciapaborto-nu_ciap-3553024b7829430aacdec7c337cc9436) OU Número de CID nessa [lista](https://impulsogov.notion.site/Lista-de-tdcidaborto-nu_cid-6584ff5c22764c5894c1db828f4fdba3) 
              - Data de registro maior ou igual data da DUM E Data de registro menor ou igual data do DPP.
              Depois retorna 'Sim' ou 'Não' conforme regras acima. 
            */
            ( 
                SELECT count(*) > 0
                FROM tb_fat_atendimento_individual tfaiaborto
                JOIN tb_fat_atd_ind_problemas tfaipaborto ON tfaiaborto.co_seq_fat_atd_ind = tfaipaborto.co_fat_atd_ind
                JOIN tb_dim_tempo tdtempoaborto ON tdtempoaborto.co_seq_dim_tempo = tfaiaborto.co_dim_tempo
                LEFT JOIN tb_dim_cid tdcidaborto ON tdcidaborto.co_seq_dim_cid = tfaipaborto.co_dim_cid
                LEFT JOIN tb_dim_ciap tdciapaborto ON tdciapaborto.co_seq_dim_ciap = tfaipaborto.co_dim_ciap
                WHERE (tfaiaborto.co_fat_cidadao_pec IN ( 
                                                            SELECT tfparto.co_seq_fat_cidadao_pec
                                                            FROM tb_fat_cidadao_pec tfparto
                                                            WHERE tfparto.no_cidadao = v2.gestante_nome 
                                                                AND tfparto.co_dim_tempo_nascimento = v2.gestante_data_nascimento_ts
                                                        )) 
                        AND ((tdciapaborto.nu_ciap = ANY (ARRAY['W82', 'W83'])) OR (tdcidaborto.nu_cid = ANY (ARRAY['O02', 'O03', 'O05', 'O06', 'O04', 'Z303']))) 
                            AND tdtempoaborto.dt_registro >= v2.gestante_dum_primeiro_atendimento
                                AND tdtempoaborto.dt_registro <= v2.gestante_primeira_dpp
            ) AS possui_registro_aborto,
            /* 
              Avalia se houve parto da gestante a partir das seguintes condições:
              - Nome da gestante na ficha tb_fat_cidadao_pec igual tb_fat_cidadao_pec (tfcp.no_cidadao unida pela ficha de atendimento individual) E Data de nascimento na ficha tb_fat_cidadao_pec igual a tb_dim_tempo (tempocidadaopec.dt_registro unida pela ficha cidadao pec pelo tempo de nascimento)
              - Número de CIAPS nessa [lista](https://impulsogov.notion.site/Lista-de-tdciapparto-nu_ciap-13968ef64119490aaf36ace6ecae9a06) OU Número de CID nessa [lista](https://impulsogov.notion.site/Lista-de-tdcidparto-nu_cid-cde4796ac50a4858a8077546190fa03f) 
              - Data de registro maior ou igual data da DUM E Data de registro menor ou igual data do DPP.
              Depois retorna 'Sim' ou 'Não' conforme regras acima. 
            */
            ( 
                SELECT count(*) > 0
                FROM tb_fat_atendimento_individual tfaiparto
                JOIN tb_fat_atd_ind_problemas tfaipparto ON tfaiparto.co_seq_fat_atd_ind = tfaipparto.co_fat_atd_ind
                JOIN tb_dim_tempo tdtempoparto ON tdtempoparto.co_seq_dim_tempo = tfaiparto.co_dim_tempo
                LEFT JOIN tb_dim_cid tdcidparto ON tdcidparto.co_seq_dim_cid = tfaipparto.co_dim_cid
                LEFT JOIN tb_dim_ciap tdciapparto ON tdciapparto.co_seq_dim_ciap = tfaipparto.co_dim_ciap
                WHERE (tfaiparto.co_fat_cidadao_pec IN ( 
                                                            SELECT tfcparto.co_seq_fat_cidadao_pec
                                                            FROM tb_fat_cidadao_pec tfcparto
                                                            WHERE tfcparto.no_cidadao = v2.gestante_nome AND tfcparto.co_dim_tempo_nascimento = v2.gestante_data_nascimento_ts
                                                        )
                        ) AND ((tdciapparto.nu_ciap = ANY (ARRAY['W90', 'W91', 'W92', 'W93'])) OR (tdcidparto.nu_cid = ANY (ARRAY['O80', 'Z370', 'Z379', 'Z38', 'Z39', 'Z371', 'Z379', 'O42', 'O45', 'O60', 'O61', 'O62', 'O63', 'O64', 'O65', 'O66', 'O67', 'O68', 'O69', 'O70', 'O71', 'O73', 'O750', 'O751', 'O754', 'O755', 'O756', 'O757', 'O758', 'O759', 'O81', 'O82', 'O83', 'O84', 'Z372', 'Z375', 'Z379', 'Z38', 'Z39']))) 
                            AND tdtempoparto.dt_registro >= v2.gestante_dum_primeiro_atendimento
                                AND tdtempoparto.dt_registro <= v2.gestante_primeira_dpp
            ) AS possui_registro_parto
            
           FROM ( SELECT v1.atendimento_data,
                    v1.estabelecimento_cnes,
                    v1.estabelecimento_nome,
                    v1.equipe_ine,
                    v1.equipe_nome,
                    v1.acs_nome,
                    v1.acs_data_ultima_visita,
                    v1.co_fat_cidadao_pec,
                    v1.gestante_documento_cpf,
                    v1.gestante_documento_cns,
                    v1.gestante_nome,
                    v1.gestante_data_de_nascimento,
                    v1.gestante_data_nascimento_ts,
                    v1.gestante_telefone,
                    v1.gestante_endereco,
                    /* 
                      Retorna quantas consultas teve a partir da contagem de linhas totais registradas para a gestante (nome e data de nascimento)
                      São contabilizadas todas as fichas que possuam:
                      - Número de CBO nessa [lista](https://impulsogov.notion.site/Lista-de-tdcbo-nu_cbo-0b7036debf9d48a5a0bf17c1a5c20c89)
                      - Número de CIAP nessa [lista](https://impulsogov.notion.site/Lista-de-tdciap-nu_ciap-f0e9d7ad510e4d09b8cc927fba2681b4) OU - Número de CID nessa [lista](https://impulsogov.notion.site/Lista-de-tdcid-nu_cid-48e1093c19e4473983812d7bd283b34a).
                      - Data de registro em tb_dim_tempo é maior que últimos 294 dias (280 dias de gestação + 14 dias de margem)
                    */
                    count(*) OVER (PARTITION BY v1.gestante_nome, v1.gestante_data_de_nascimento) AS gestante_consulta_prenatal_total,
                    /* 
                      Retorna a data máxima (mais recente) de atendimento a partir do campo registro de atendimento ficha de atendimento individual.
                      São contabilizadas todas as fichas que possuam:
                      - Número de CBO nessa [lista](https://impulsogov.notion.site/Lista-de-tdcbo-nu_cbo-0b7036debf9d48a5a0bf17c1a5c20c89)
                      - Número de CIAP nessa [lista](https://impulsogov.notion.site/Lista-de-tdciap-nu_ciap-f0e9d7ad510e4d09b8cc927fba2681b4) OU - Número de CID nessa [lista](https://impulsogov.notion.site/Lista-de-tdcid-nu_cid-48e1093c19e4473983812d7bd283b34a).
                      - Data de registro em tb_dim_tempo é maior que últimos 294 dias (280 dias de gestação + 14 dias de margem)
                    */
                    max(v1.atendimento_data) OVER (PARTITION BY v1.gestante_nome, v1.gestante_data_de_nascimento) AS gestante_consulta_prenatal_ultima_data,
                    /* 
                      Retorna diferença entre data atual e data máxima (mais recente) do atendimento, calculada a partir do campo registro de atendimento ficha de atendimento individual.
                      São contabilizadas todas as fichas que possuam:
                      - Número de CBO nessa [lista](https://impulsogov.notion.site/Lista-de-tdcbo-nu_cbo-0b7036debf9d48a5a0bf17c1a5c20c89)
                      - Número de CIAP nessa [lista](https://impulsogov.notion.site/Lista-de-tdciap-nu_ciap-f0e9d7ad510e4d09b8cc927fba2681b4) OU - Número de CID nessa [lista](https://impulsogov.notion.site/Lista-de-tdcid-nu_cid-48e1093c19e4473983812d7bd283b34a).
                      - Data de registro em tb_dim_tempo é maior que últimos 294 dias (280 dias de gestação + 14 dias de margem)
                    */
                    CURRENT_DATE - max(v1.atendimento_data) OVER (PARTITION BY v1.gestante_nome, v1.gestante_data_de_nascimento) AS gestante_consulta_prenatal_ultima_dias_desde,
                    /* Pega a primeira DPP não nula (ordenada pela data de atendimento) a partir do campo co_dim_tempo_dum da ficha de atendimento individual (tb_fat_atendimento_individual), repartido a partir de nome da gestante e data de nascimento */
                    (array_agg(v1.gestante_dpp) FILTER (WHERE v1.gestante_dpp IS NOT NULL) OVER (PARTITION BY v1.gestante_nome, v1.gestante_data_de_nascimento ORDER BY v1.atendimento_data ASC))[1] AS gestante_primeira_dpp,
                    /* Pega a primeira IG não nula (ordenada pela data de atendimento) a partir do campo co_dim_tempo_dum da ficha de atendimento individual (tb_fat_atendimento_individual), repartido a partir de nome da gestante e data de nascimento */
                    (array_agg(v1.gestante_idade_gestacional) FILTER (WHERE v1.gestante_idade_gestacional IS NOT NULL) OVER (PARTITION BY v1.gestante_nome, v1.gestante_data_de_nascimento ORDER BY v1.atendimento_data ASC))[1] AS gestante_idade_gestacional,
                    /* Pega a idade gestacional do primeiro atendimento onde a mesma não está nula (ordenada pela data de atendimento) a partir do campo co_dim_tempo_dum da ficha de atendimento individual (tb_fat_atendimento_individual), repartido a partir de nome da gestante e data de nascimento */
                    (array_agg(v1.gestante_idade_gestacional_atendimento) FILTER (WHERE v1.gestante_idade_gestacional_atendimento IS NOT NULL) OVER (PARTITION BY v1.gestante_nome, v1.gestante_data_de_nascimento ORDER BY v1.atendimento_data ASC))[1] AS gestante_idade_gestacional_primeiro_atendimento,
                     /* Pega a primeira DUM não nula (ordenada pela data de atendimento) a partir do campo co_dim_tempo_dum da ficha de atendimento individual (tb_fat_atendimento_individual), repartido a partir de nome da gestante e data de nascimento */
                    (array_agg(v1.gestante_dum) FILTER (WHERE v1.gestante_dum <> '3000-12-31') OVER (PARTITION BY v1.gestante_nome, v1.gestante_data_de_nascimento ORDER BY v1.atendimento_data ASC))[1] AS gestante_dum_primeiro_atendimento,
                    /* Pega a primeira data de atendimento não nula (ordenada pela data de atendimento) a partir do campo co_dim_tempo_dum da ficha de atendimento individual (tb_fat_atendimento_individual), repartido a partir de nome da gestante e data de nascimento */
                    min(v1.atendimento_data) FILTER (WHERE v1.gestante_dpp IS NOT NULL) OVER (PARTITION BY v1.gestante_nome, v1.gestante_data_de_nascimento) AS atendimento_primeiro_data
                    FROM ( 
                            SELECT
                            /* Faz distinct pela PK da tabela, para evitar registros duplicados caso o atendimento possua mais de 1 ou CID e/ou CIAP registrados */ 
                            distinct(tfai.co_seq_fat_atd_ind),
                            tdt.dt_registro AS atendimento_data,
                            /* Retorna código da unidade na Ficha de cadastro individual recente (tb_fat_cad_individual), caso nulo retorna código da unidade na Ficha de atendimento individual recente (tb_fat_atendimento_individual) */
                            COALESCE(NULLIF(unidadecadastrorecente.nu_cnes, '-'), unidadeatendimentorecente.nu_cnes) AS estabelecimento_cnes,
                            /* Retorna nome da unidade na Ficha de cadastro individual recente (tb_fat_cad_individual), caso nulo retorna nome da unidade na Ficha de atendimento individual recente (tb_fat_atendimento_individual) */
                            COALESCE(NULLIF(unidadecadastrorecente.no_unidade_saude, 'Não informado'), unidadeatendimentorecente.no_unidade_saude) AS estabelecimento_nome,
                            /* Retorna código da equipe na Ficha de cadastro individual recente (tb_fat_cad_individual), caso nulo retorna código da equipe na Ficha de atendimento individual recente (tb_fat_atendimento_individual) */
                            COALESCE(NULLIF(equipeacadastrorecente.nu_ine, '-'), equipeatendimentorecente.nu_ine) AS equipe_ine,
                            /* Retorna nome da equipe na Ficha de cadastro individual recente (tb_fat_cad_individual), caso nulo retorna nome da equipe na Ficha de atendimento individual recente (tb_fat_atendimento_individual) */
                            COALESCE(NULLIF(equipeacadastrorecente.no_equipe, 'SEM EQUIPE'), equipeatendimentorecente.no_equipe) AS equipe_nome,
                            /* Retorna nome do ACS na Ficha de cadastro individual recente (tb_fat_cad_individual), caso nulo retorna nome do ACS da Ficha de Visita Domiciliar (tb_fat_visita_domiciliar) */
                            COALESCE(acsvisitarecente.no_profissional, acscadastrorecente.no_profissional) AS acs_nome,
                            /* Retorna a data de última visita do ACS, a partir da Ficha de Visita Domiciliar (tb_fat_visita_domiciliar)  */
                            acstempovisitarecente.dt_registro AS acs_data_ultima_visita,
                            tfai.co_fat_cidadao_pec,
                            /* Retorna o CPF do cidadão (tb_fat_cidadao_pec) */
                            tfcp.nu_cpf_cidadao AS gestante_documento_cpf,
                            /* Retorna o CNS do cidadão (tb_fat_cidadao_pec) */
                            tfcp.nu_cns AS gestante_documento_cns,
                            /* Retorna o nome do cidadão (tb_fat_cidadao_pec) */
                            tfcp.no_cidadao AS gestante_nome,
                            /* Retorna a data de nascimento do cidadão em formato date */
                            tempocidadaopec.dt_registro AS gestante_data_de_nascimento,
                             /* Retorna a data de nascimento do cidadão em formato timestamp */
                            tfcp.co_dim_tempo_nascimento AS gestante_data_nascimento_ts,
                            /* Retorna o número de telefone do cidadão (tb_fat_cidadao_pec) */
                            tfcp.nu_telefone_celular AS gestante_telefone,
                            /* Retorna o endereço do cidadão a partir da Ficha de Cadastro Domiciliar (tb_fat_cad_domiciliar)  */
                            NULLIF(concat(tfcd.no_logradouro, ', ', tfcd.nu_num_logradouro), ', ') AS gestante_endereco,
                            /* 
                              Calcula a DPP por:
                              - Caso a DUM da Ficha de Atendimento Individual (tb_fat_atendimento_individual) preenchida corretamente (diferente de 3000), retorna data de registro + 294 dias
                              - Caso idade gestacional (nu_idade_gestacional_semanas) da Ficha de Atendimento Individual (tb_fat_atendimento_individual) preenchida, retorna data de registro - 7 dias * Idade gestacional em semanas + 294 dias
                              - Caso ambos estejam vazias, retorna nulo
                            */
                            CASE
                                WHEN tdtdum.nu_ano <> 3000 THEN tdtdum.dt_registro + '294 days'::interval
                                WHEN tfai.nu_idade_gestacional_semanas IS NOT NULL THEN tdt.dt_registro - '7 days'::interval * tfai.nu_idade_gestacional_semanas::double precision + '294 days'::interval
                                ELSE NULL
                            END AS gestante_dpp,
                            /* 
                              Calcula a Idade Gestacional por:
                              - Caso a DUM da Ficha de Atendimento Individual (tb_fat_atendimento_individual) preenchida corretamente (diferente de 3000), retorna (data atual - data de registro)/7
                              - Caso idade gestacional (nu_idade_gestacional_semanas) da Ficha de Atendimento Individual (tb_fat_atendimento_individual) preenchida, retorna ((data atual - (data de registro - 7 dias * Idade gestacional em semanas )) / 7)
                              - Caso ambos esteja vazios, retorna nulo
                            */
                            CASE
                                WHEN tdtdum.nu_ano <> 3000 THEN (CURRENT_DATE - tdtdum.dt_registro) / 7
                                WHEN tfai.nu_idade_gestacional_semanas IS NOT NULL THEN (CURRENT_DATE - (tdt.dt_registro - '7 days'::interval * tfai.nu_idade_gestacional_semanas::double precision)::date) / 7
                                ELSE NULL
                            END AS gestante_idade_gestacional,
                            tfai.nu_idade_gestacional_semanas as gestante_idade_gestacional_atendimento,
                             /* 
                              Calcula a DUM por:
                              - Caso a DUM da Ficha de Atendimento Individual (tb_fat_atendimento_individual) preenchida corretamente (diferente de 3000), retorna a DUM preenchida
                              - Caso a DUM não esteja preenchida, utiliza a data de atendimento substraindo-se a idade gestacional da mesma, chegando a uma DUM estimada
                              - Caso ambos esteja vazios, retorna nulo
                            */
                            CASE
                                WHEN tdtdum.nu_ano <> 3000 THEN tdtdum.dt_registro
                                WHEN tfai.nu_idade_gestacional_semanas IS NOT NULL THEN (tdt.dt_registro - '7 days'::interval * tfai.nu_idade_gestacional_semanas::double precision)::date
                                ELSE NULL
                            END AS gestante_dum
                           /* Inicio do FROM */
                           FROM tb_fat_atendimento_individual tfai
                             JOIN tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfai.co_dim_cbo_1
                             JOIN tb_dim_tempo tdt ON tfai.co_dim_tempo = tdt.co_seq_dim_tempo
                             JOIN tb_dim_tempo tdtdum ON tfai.co_dim_tempo_dum = tdtdum.co_seq_dim_tempo
                             JOIN tb_fat_atd_ind_problemas tfaip ON tfai.co_seq_fat_atd_ind = tfaip.co_fat_atd_ind
                             JOIN tb_fat_cidadao_pec tfcp ON tfcp.co_seq_fat_cidadao_pec = tfai.co_fat_cidadao_pec
                             JOIN tb_dim_tempo tempocidadaopec ON tempocidadaopec.co_seq_dim_tempo = tfcp.co_dim_tempo_nascimento
                             LEFT JOIN tb_dim_cid tdcid ON tdcid.co_seq_dim_cid = tfaip.co_dim_cid
                             LEFT JOIN tb_dim_ciap tdciap ON tdciap.co_seq_dim_ciap = tfaip.co_dim_ciap
                             LEFT JOIN tb_fat_cad_domiciliar tfcd ON tfcd.co_seq_fat_cad_domiciliar = (( SELECT cadomiciliar.co_seq_fat_cad_domiciliar
                                   FROM tb_fat_cad_dom_familia caddomiciliarfamilia
                                     JOIN tb_fat_cad_domiciliar cadomiciliar ON cadomiciliar.co_seq_fat_cad_domiciliar = caddomiciliarfamilia.co_fat_cad_domiciliar
                                  WHERE (caddomiciliarfamilia.co_fat_cidadao_pec IN ( 
                                      SELECT tfccaddomiciliarfamilia.co_seq_fat_cidadao_pec
                                      FROM tb_fat_cidadao_pec tfccaddomiciliarfamilia
                                      WHERE tfccaddomiciliarfamilia.no_cidadao = tfcp.no_cidadao AND tfccaddomiciliarfamilia.co_dim_tempo_nascimento = tfcp.co_dim_tempo_nascimento
                                    ))
                                  ORDER BY cadomiciliar.co_dim_tempo DESC LIMIT 1))
                             LEFT JOIN tb_fat_visita_domiciliar tfvdrecente ON tfvdrecente.co_seq_fat_visita_domiciliar = (( SELECT visitadomiciliar.co_seq_fat_visita_domiciliar
                                   FROM tb_fat_visita_domiciliar visitadomiciliar
                                  WHERE (visitadomiciliar.co_fat_cidadao_pec IN ( 
                                    SELECT tfcvisita.co_seq_fat_cidadao_pec
                                    FROM tb_fat_cidadao_pec tfcvisita
                                    WHERE tfcvisita.no_cidadao = tfcp.no_cidadao AND tfcvisita.co_dim_tempo_nascimento = tfcp.co_dim_tempo_nascimento
                                  ))
                                  ORDER BY visitadomiciliar.co_dim_tempo DESC LIMIT 1))
                             LEFT JOIN tb_fat_cad_individual tfcirecente ON tfcirecente.co_seq_fat_cad_individual = (( SELECT cadastroindividual.co_seq_fat_cad_individual
                                   FROM tb_fat_cad_individual cadastroindividual
                                  WHERE (cadastroindividual.co_fat_cidadao_pec IN ( 
                                      SELECT tfccadastro.co_seq_fat_cidadao_pec
                                      FROM tb_fat_cidadao_pec tfccadastro
                                      WHERE tfccadastro.no_cidadao = tfcp.no_cidadao AND tfccadastro.co_dim_tempo_nascimento = tfcp.co_dim_tempo_nascimento
                                  ))
                                  ORDER BY cadastroindividual.co_dim_tempo DESC LIMIT 1))
                             LEFT JOIN tb_fat_atendimento_individual tfairecente ON tfairecente.co_seq_fat_atd_ind = (( SELECT atendimentoindividual.co_seq_fat_atd_ind
                                   FROM tb_fat_atendimento_individual atendimentoindividual
                                  WHERE (atendimentoindividual.co_fat_cidadao_pec IN ( 
                                        SELECT tfcatendimento.co_seq_fat_cidadao_pec
                                        FROM tb_fat_cidadao_pec tfcatendimento
                                        WHERE tfcatendimento.no_cidadao = tfcp.no_cidadao AND tfcatendimento.co_dim_tempo_nascimento = tfcp.co_dim_tempo_nascimento
                                  ))
                                  ORDER BY atendimentoindividual.co_dim_tempo DESC LIMIT 1))
                             LEFT JOIN tb_dim_equipe equipeatendimentorecente ON equipeatendimentorecente.co_seq_dim_equipe = tfairecente.co_dim_equipe_1
                             LEFT JOIN tb_dim_profissional profissinalatendimentorecente ON profissinalatendimentorecente.co_seq_dim_profissional = tfairecente.co_dim_profissional_1
                             LEFT JOIN tb_dim_unidade_saude unidadeatendimentorecente ON unidadeatendimentorecente.co_seq_dim_unidade_saude = tfairecente.co_dim_unidade_saude_1
                             LEFT JOIN tb_dim_equipe equipeacadastrorecente ON equipeacadastrorecente.co_seq_dim_equipe = tfcirecente.co_dim_equipe
                             LEFT JOIN tb_dim_profissional profissinalcadastrorecente ON profissinalcadastrorecente.co_seq_dim_profissional = tfcirecente.co_dim_profissional
                             LEFT JOIN tb_dim_unidade_saude unidadecadastrorecente ON unidadecadastrorecente.co_seq_dim_unidade_saude = tfcirecente.co_dim_unidade_saude
                             LEFT JOIN tb_dim_profissional acsvisitarecente ON acsvisitarecente.co_seq_dim_profissional = tfvdrecente.co_dim_profissional
                             LEFT JOIN tb_dim_profissional acscadastrorecente ON acscadastrorecente.co_seq_dim_profissional = tfcirecente.co_dim_profissional
                             LEFT JOIN tb_dim_tempo acstempovisitarecente ON tfvdrecente.co_dim_tempo = acstempovisitarecente.co_seq_dim_tempo
                             /* 
                              Filtra os registros que tenham as seguintes condições: 
                              - Número de CBO em tb_dim_cbo esta nessa [lista](https://impulsogov.notion.site/Lista-de-tdcbo-nu_cbo-0b7036debf9d48a5a0bf17c1a5c20c89) 
                              - Número de CIAP em tb_dim_ciap esta nessa [lista](https://impulsogov.notion.site/Lista-de-tdciap-nu_ciap-f0e9d7ad510e4d09b8cc927fba2681b4) OU Número de CID nessa [lista](https://impulsogov.notion.site/Lista-de-tdcid-nu_cid-48e1093c19e4473983812d7bd283b34a).
                              - Data de registro em tb_dim_tempo é maior que últimos 294 dias. 
                             */
                          WHERE (tdcbo.nu_cbo ~~ ANY (ARRAY['2231%', '2235%', '2251%', '2252%', '2253%'])) AND ((tdciap.nu_ciap = ANY (ARRAY['ABP001', 'W03', 'W05', 'W29', 'W71', 'W78', 'W79', 'W80', 'W81', 'W84', 'W85'])) OR (tdcid.nu_cid = ANY (ARRAY['O11', 'O120', 'O121', 'O122', 'O13', 'O140', 'O141', 'O149', 'O150', 'O151', 'O159', 'O16', 'O200', 'O208', 'O209', 'O210', 'O211', 'O212', 'O218', 'O219', 'O220', 'O221', 'O222', 'O223', 'O224', 'O225', 'O228', 'O229', 'O230', 'O231', 'O232', 'O233', 'O234', 'O235', 'O239', 'O299', 'O300', 'O301', 'O302', 'O308', 'O309', 'O311', 'O312', 'O318', 'O320', 'O321', 'O322', 'O323', 'O324', 'O325', 'O326', 'O328', 'O329', 'O330', 'O331', 'O332', 'O333', 'O334', 'O335', 'O336', 'O337', 'O338', 'O752', 'O753', 'O990', 'O991', 'O992', 'O993', 'O994', 'O240', 'O241', 'O242', 'O243', 'O244', 'O249', 'O25', 'O260', 'O261', 'O263', 'O264', 'O265', 'O268', 'O269', 'O280', 'O281', 'O282', 'O283', 'O284', 'O285', 'O288', 'O289', 'O290', 'O291', 'O292', 'O293', 'O294', 'O295', 'O296', 'O298', 'O009', 'O339', 'O340', 'O341', 'O342', 'O343', 'O344', 'O345', 'O346', 'O347', 'O348', 'O349', 'O350', 'O351', 'O352', 'O353', 'O354', 'O355', 'O356', 'O357', 'O358', 'O359', 'O360', 'O361', 'O362', 'O363', 'O365', 'O366', 'O367', 'O368', 'O369', 'O40', 'O410', 'O411', 'O418', 'O419', 'O430', 'O431', 'O438', 'O439', 'O440', 'O441', 'O460', 'O468', 'O469', 'O470', 'O471', 'O479', 'O48', 'O995', 'O996', 'O997', 'Z640', 'O00', 'O10', 'O12', 'O14', 'O15', 'O20', 'O21', 'O22', 'O23', 'O24', 'O26', 'O28', 'O29', 'O30', 'O31', 'O32', 'O33', 'O34', 'O35', 'O36', 'O41', 'O43', 'O44', 'O46', 'O47', 'O98', 'Z34', 'Z35', 'Z36', 'Z321', 'Z33', 'Z340', 'Z348', 'Z349', 'Z350', 'Z351', 'Z352', 'Z353', 'Z354', 'Z357', 'Z358', 'Z359']))) 
                          AND tdt.dt_registro >= (CURRENT_DATE - '294 days'::interval)) v1) v2) v3
    /* 
      Filtra:
      - Primeira linha para cada gestante (registro mais recente)
      - Cuja data provável de parto maior ou igual que a data atual
    */
  WHERE v3.r = 1;
