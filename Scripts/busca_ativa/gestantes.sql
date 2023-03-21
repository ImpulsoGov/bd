SELECT v3.estabelecimento_cnes,
    upper(v3.estabelecimento_nome) AS estabelecimento_nome,
    v3.equipe_ine,
    v3.equipe_nome,
    upper(v3.acs_nome::text) AS acs_nome,
    v3.acs_data_ultima_visita,
    v3.gestante_documento_cpf,
    v3.gestante_documento_cns,
    v3.gestante_nome,
    v3.gestante_data_de_nascimento,
    v3.gestante_telefone,
    v3.gestante_endereco,
    v3.gestante_dum,
    v3.gestante_idade_gestacional AS gestante_idade_gestacional_atual,v3.atendimento_primeiro_data,
        CASE
            WHEN date_part('year'::text, v3.gestante_dum) <> 3000::double precision THEN (v3.atendimento_primeiro_data - v3.gestante_dum) / 7
            WHEN v3.gestante_idade_gestacional_primeiro_atendimento IS NOT NULL THEN v3.gestante_idade_gestacional_primeiro_atendimento
            ELSE NULL::integer
        END AS gestante_idade_gestacional_primeiro_atendimento,
    v3.gestante_primeira_dpp AS gestante_dpp,
    v3.gestante_primeira_dpp AS gestante_consulta_prenatal_data_limite,
    v3.gestante_primeira_dpp::date - CURRENT_DATE AS gestante_dpp_dias_para,
    v3.gestante_consulta_prenatal_total,
    v3.gestante_consulta_prenatal_ultima_data,
    v3.gestante_consulta_prenatal_ultima_dias_desde,
    ( SELECT count(*) > 0 AS bool
                   FROM tb_fat_atendimento_odonto otfodont
                     JOIN tb_dim_cbo otdcbo ON otdcbo.co_seq_dim_cbo = otfodont.co_dim_cbo_1
                     JOIN tb_dim_tempo otdtempo ON otdtempo.co_seq_dim_tempo = otfodont.co_dim_tempo
                  WHERE (otfodont.co_fat_cidadao_pec IN ( SELECT tfcodonto.co_seq_fat_cidadao_pec
                           FROM tb_fat_cidadao_pec tfcodonto
                          WHERE tfcodonto.no_cidadao::text = v3.gestante_nome::text AND tfcodonto.co_dim_tempo_nascimento = v3.gestante_data_nascimento_ts)) AND otdcbo.nu_cbo::text ~~ '2232%'::text AND otdtempo.dt_registro >= v3.gestante_dum_primeiro_atendimento AND otdtempo.dt_registro <= v3.gestante_primeira_dpp) AS atendimento_odontologico_realizado,        
     ( SELECT count(*) > 0
                   FROM ( SELECT tdp.co_proced
                           FROM tb_fat_proced_atend_proced tfpap
                             JOIN tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfpap.co_dim_procedimento
                             JOIN tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfpap.co_dim_cbo
                             JOIN tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfpap.co_dim_tempo
                          WHERE (tfpap.co_fat_cidadao_pec IN ( SELECT tfcprocedhiv.co_seq_fat_cidadao_pec
                                   FROM tb_fat_cidadao_pec tfcprocedhiv
                                  WHERE tfcprocedhiv.no_cidadao::text = v3.gestante_nome::text AND tfcprocedhiv.co_dim_tempo_nascimento = v3.gestante_data_nascimento_ts)) AND (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND tdtempo.dt_registro >= v3.gestante_dum_primeiro_atendimento AND tdtempo.dt_registro <= v3.gestante_primeira_dpp AND (tdp.co_proced::text = ANY (ARRAY['0214010058'::text, '0214010040'::text, 'ABPG024'::text]))
                        UNION ALL
                         SELECT tdp.co_proced
                           FROM tb_fat_atd_ind_procedimentos tfaip
                             JOIN tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfaip.co_dim_procedimento_avaliado
                             JOIN tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfaip.co_dim_cbo_1
                             JOIN tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfaip.co_dim_tempo
                          WHERE (tfaip.co_fat_cidadao_pec IN ( SELECT tfcprocedhiv.co_seq_fat_cidadao_pec
                                   FROM tb_fat_cidadao_pec tfcprocedhiv
                                  WHERE tfcprocedhiv.no_cidadao::text = v3.gestante_nome::text AND tfcprocedhiv.co_dim_tempo_nascimento = v3.gestante_data_nascimento_ts)) AND (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND tdtempo.dt_registro >= v3.gestante_dum_primeiro_atendimento AND tdtempo.dt_registro <= v3.gestante_primeira_dpp AND (tdp.co_proced::text = ANY (ARRAY['0202030300'::text, 'ABEX018'::text]))) e1) AS exame_hiv_realizado,
   ( SELECT count(*) > 0
                   FROM ( SELECT tdp.co_proced
                           FROM tb_fat_proced_atend_proced tfpap
                             JOIN tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfpap.co_dim_procedimento
                             JOIN tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfpap.co_dim_cbo
                             JOIN tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfpap.co_dim_tempo
                          WHERE (tfpap.co_fat_cidadao_pec IN ( SELECT tfcprocedsilfilis.co_seq_fat_cidadao_pec
                                   FROM tb_fat_cidadao_pec tfcprocedsilfilis
                                  WHERE tfcprocedsilfilis.no_cidadao::text = v3.gestante_nome::text AND tfcprocedsilfilis.co_dim_tempo_nascimento = v3.gestante_data_nascimento_ts)) AND (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND tdtempo.dt_registro >= v3.gestante_dum_primeiro_atendimento AND tdtempo.dt_registro <= v3.gestante_primeira_dpp AND (tdp.co_proced::text = ANY (ARRAY['0214010074'::text, '0214010082'::text, 'ABPG026'::text]))
                        UNION ALL
                         SELECT tdp.co_proced
                           FROM tb_fat_atd_ind_procedimentos tfaip
                             JOIN tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfaip.co_dim_procedimento_avaliado
                             JOIN tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfaip.co_dim_cbo_1
                             JOIN tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfaip.co_dim_tempo
                          WHERE (tfaip.co_fat_cidadao_pec IN ( SELECT tfcprocedsilfilis.co_seq_fat_cidadao_pec
                                   FROM tb_fat_cidadao_pec tfcprocedsilfilis
                                  WHERE tfcprocedsilfilis.no_cidadao::text = v3.gestante_nome::text AND tfcprocedsilfilis.co_dim_tempo_nascimento = v3.gestante_data_nascimento_ts)) AND (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND tdtempo.dt_registro >= v3.gestante_dum_primeiro_atendimento AND tdtempo.dt_registro <= v3.gestante_primeira_dpp AND (tdp.co_proced::text = ANY (ARRAY['0202031110'::text, '0202031179'::text, 'ABEX019'::text]))) e2) AS exame_sifilis_realizado,
    ( SELECT count(*) > 0
                   FROM ( SELECT tdp.co_proced
                           FROM tb_fat_proced_atend_proced tfpap
                             JOIN tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfpap.co_dim_procedimento
                             JOIN tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfpap.co_dim_cbo
                             JOIN tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfpap.co_dim_tempo
                          WHERE (tfpap.co_fat_cidadao_pec IN ( SELECT tfcprocedhiv.co_seq_fat_cidadao_pec
                                   FROM tb_fat_cidadao_pec tfcprocedhiv
                                  WHERE tfcprocedhiv.no_cidadao::text = v3.gestante_nome::text AND tfcprocedhiv.co_dim_tempo_nascimento = v3.gestante_data_nascimento_ts)) AND (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND tdtempo.dt_registro >= v3.gestante_dum_primeiro_atendimento AND tdtempo.dt_registro <= v3.gestante_primeira_dpp AND (tdp.co_proced::text = ANY (ARRAY['0214010058'::text, '0214010040'::text, 'ABPG024'::text]))
                        UNION ALL
                         SELECT tdp.co_proced
                           FROM tb_fat_atd_ind_procedimentos tfaip
                             JOIN tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfaip.co_dim_procedimento_avaliado
                             JOIN tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfaip.co_dim_cbo_1
                             JOIN tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfaip.co_dim_tempo
                          WHERE (tfaip.co_fat_cidadao_pec IN ( SELECT tfcprocedhiv.co_seq_fat_cidadao_pec
                                   FROM tb_fat_cidadao_pec tfcprocedhiv
                                  WHERE tfcprocedhiv.no_cidadao::text = v3.gestante_nome::text AND tfcprocedhiv.co_dim_tempo_nascimento = v3.gestante_data_nascimento_ts)) AND (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND tdtempo.dt_registro >= v3.gestante_dum_primeiro_atendimento AND tdtempo.dt_registro <= v3.gestante_primeira_dpp AND (tdp.co_proced::text = ANY (ARRAY['0202030300'::text, 'ABEX018'::text]))) e1) 
     and  ( SELECT count(*) > 0
                   FROM ( SELECT tdp.co_proced
                           FROM tb_fat_proced_atend_proced tfpap
                             JOIN tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfpap.co_dim_procedimento
                             JOIN tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfpap.co_dim_cbo
                             JOIN tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfpap.co_dim_tempo
                          WHERE (tfpap.co_fat_cidadao_pec IN ( SELECT tfcprocedsilfilis.co_seq_fat_cidadao_pec
                                   FROM tb_fat_cidadao_pec tfcprocedsilfilis
                                  WHERE tfcprocedsilfilis.no_cidadao::text = v3.gestante_nome::text AND tfcprocedsilfilis.co_dim_tempo_nascimento = v3.gestante_data_nascimento_ts)) AND (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND tdtempo.dt_registro >= v3.gestante_dum_primeiro_atendimento AND tdtempo.dt_registro <= v3.gestante_primeira_dpp AND (tdp.co_proced::text = ANY (ARRAY['0214010074'::text, '0214010082'::text, 'ABPG026'::text]))
                        UNION ALL
                         SELECT tdp.co_proced
                           FROM tb_fat_atd_ind_procedimentos tfaip
                             JOIN tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfaip.co_dim_procedimento_avaliado
                             JOIN tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfaip.co_dim_cbo_1
                             JOIN tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfaip.co_dim_tempo
                          WHERE (tfaip.co_fat_cidadao_pec IN ( SELECT tfcprocedsilfilis.co_seq_fat_cidadao_pec
                                   FROM tb_fat_cidadao_pec tfcprocedsilfilis
                                  WHERE tfcprocedsilfilis.no_cidadao::text = v3.gestante_nome::text AND tfcprocedsilfilis.co_dim_tempo_nascimento = v3.gestante_data_nascimento_ts)) AND (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND tdtempo.dt_registro >= v3.gestante_dum_primeiro_atendimento AND tdtempo.dt_registro <= v3.gestante_primeira_dpp AND (tdp.co_proced::text = ANY (ARRAY['0202031110'::text, '0202031179'::text, 'ABEX019'::text]))) e2) AS exame_sifilis_hiv_realizado,
        CASE
            WHEN v3.possui_registro_aborto IS TRUE THEN 'Sim'::text
            ELSE 'Não'::text
        END AS possui_registro_aborto,
        CASE
            WHEN v3.possui_registro_parto IS TRUE THEN 'Sim'::text
            ELSE 'Não'::text
        END AS possui_registro_parto,
    now() AS atualizacao_data,
    now() AS criacao_data
   FROM ( SELECT
                CASE
                    WHEN v2.gestante_dum_primeiro_atendimento IS NULL THEN NULL::bigint
                    ELSE row_number() OVER (PARTITION BY v2.gestante_nome, v2.gestante_data_de_nascimento, v2.gestante_dum_primeiro_atendimento ORDER BY v2.atendimento_data DESC)
                END AS r,
            v2.atendimento_data,
        /* Caso haja atendimento com data igual ou superior a qualquer registro de parto, aborto ou DPP então retorna o primeiro atendimento após o registro de parto, aborto ou DP. Caso
         * não haja retorna a primeira data atendimento identificada */
            CASE
                WHEN v2.atendimento_data >= ANY (ARRAY[v2.dt_registro_parto::timestamp without time zone, v2.gestante_dpp + '14 days'::interval, v2.dt_registro_aborto::timestamp without time zone]) THEN (array_agg(v2.atendimento_data) FILTER (WHERE v2.gestante_dum <> '3000-12-31'::date AND (v2.atendimento_data >= ANY (ARRAY[v2.dt_registro_parto::timestamp without time zone, v2.gestante_dpp + '14 days'::interval, v2.dt_registro_aborto::timestamp without time zone]))) OVER (PARTITION BY v2.gestante_nome, v2.gestante_data_de_nascimento,v2.gestante_dum_primeiro_atendimento ORDER BY v2.atendimento_data))[1]
                ELSE (array_agg(v2.atendimento_data) FILTER (WHERE v2.gestante_dum <> '3000-12-31'::date) OVER (PARTITION BY v2.gestante_nome, v2.gestante_data_de_nascimento,v2.gestante_dum_primeiro_atendimento ORDER BY v2.atendimento_data))[1]
            END AS atendimento_primeiro_data,
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
            v2.gestante_dum_primeiro_atendimento AS gestante_dum,
            count(v2.atendimento_data) OVER (PARTITION BY v2.gestante_nome, v2.gestante_data_de_nascimento, v2.gestante_dum_primeiro_atendimento) AS gestante_consulta_prenatal_total,
            max(v2.atendimento_data) OVER (PARTITION BY v2.gestante_nome, v2.gestante_data_de_nascimento, v2.gestante_dum_primeiro_atendimento) AS gestante_consulta_prenatal_ultima_data,
            CURRENT_DATE - max(v2.atendimento_data) OVER (PARTITION BY v2.gestante_nome, v2.gestante_data_de_nascimento, v2.gestante_dum_primeiro_atendimento) AS gestante_consulta_prenatal_ultima_dias_desde,
            (array_agg(v2.gestante_dpp) FILTER (WHERE v2.gestante_dpp IS NOT NULL) OVER (PARTITION BY v2.gestante_nome, v2.gestante_data_de_nascimento, v2.gestante_dum_primeiro_atendimento ORDER BY v2.atendimento_data))[1] AS gestante_primeira_dpp,
            (array_agg(v2.gestante_idade_gestacional) FILTER (WHERE v2.gestante_idade_gestacional IS NOT NULL) OVER (PARTITION BY v2.gestante_nome, v2.gestante_data_de_nascimento, v2.gestante_dum_primeiro_atendimento ORDER BY v2.atendimento_data))[1] AS gestante_idade_gestacional,
            (array_agg(v2.gestante_idade_gestacional_atendimento) FILTER (WHERE v2.gestante_idade_gestacional_atendimento IS NOT NULL) OVER (PARTITION BY v2.gestante_nome, v2.gestante_data_de_nascimento, v2.gestante_dum_primeiro_atendimento ORDER BY v2.atendimento_data))[1] AS gestante_idade_gestacional_primeiro_atendimento,
            v2.gestante_dum_primeiro_atendimento,
            count(v2.atendimento_data) FILTER (WHERE v2.dt_registro_parto_mais_recente IS NOT null and v2.dt_registro_parto_mais_recente > v2.atendimento_data) OVER (PARTITION BY v2.gestante_nome, v2.gestante_data_de_nascimento, v2.gestante_dum_primeiro_atendimento) > 0 AS possui_registro_parto,
            count(v2.atendimento_data) FILTER (WHERE v2.dt_registro_aborto_mais_recente IS NOT null and v2.dt_registro_parto_mais_recente > v2.atendimento_data) OVER (PARTITION BY v2.gestante_nome, v2.gestante_data_de_nascimento, v2.gestante_dum_primeiro_atendimento) > 0 AS possui_registro_aborto,
            ( SELECT count(*) > 0 AS bool
                   FROM tb_fat_atendimento_odonto otfodont
                     JOIN tb_dim_cbo otdcbo ON otdcbo.co_seq_dim_cbo = otfodont.co_dim_cbo_1
                     JOIN tb_dim_tempo otdtempo ON otdtempo.co_seq_dim_tempo = otfodont.co_dim_tempo
                  WHERE (otfodont.co_fat_cidadao_pec IN ( SELECT tfcodonto.co_seq_fat_cidadao_pec
                           FROM tb_fat_cidadao_pec tfcodonto
                          WHERE tfcodonto.no_cidadao::text = v2.gestante_nome::text AND tfcodonto.co_dim_tempo_nascimento = v2.gestante_data_nascimento_ts)) AND otdcbo.nu_cbo::text ~~ '2232%'::text AND otdtempo.dt_registro >= v2.gestante_dum_primeiro_atendimento AND otdtempo.dt_registro <= v2.gestante_primeira_dpp) AS atendimento_odontologico_realizado,
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
                                  WHERE tfcprocedhiv.no_cidadao::text = v2.gestante_nome::text AND tfcprocedhiv.co_dim_tempo_nascimento = v2.gestante_data_nascimento_ts)) AND (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND tdtempo.dt_registro >= v2.gestante_dum_primeiro_atendimento AND tdtempo.dt_registro <= v2.gestante_primeira_dpp AND (tdp.co_proced::text = ANY (ARRAY['0202030300'::text, 'ABEX018'::text]))) e1) AS exame_hiv_realizado,
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
                                  WHERE tfcprocedsilfilis.no_cidadao::text = v2.gestante_nome::text AND tfcprocedsilfilis.co_dim_tempo_nascimento = v2.gestante_data_nascimento_ts)) AND (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND tdtempo.dt_registro >= v2.gestante_dum_primeiro_atendimento AND tdtempo.dt_registro <= v2.gestante_primeira_dpp AND (tdp.co_proced::text = ANY (ARRAY['0202031110'::text, '0202031179'::text, 'ABEX019'::text]))) e2) AS exame_sifilis_realizado
           FROM ( 
           		SELECT v1.atendimento_data,
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
                    v1.dt_registro_parto,
                    v1.dt_registro_aborto,
                    v1.gestante_dpp,
                    v1.gestante_idade_gestacional,
                    v1.gestante_idade_gestacional_atendimento,
                    (array_agg(v1.gestante_dpp) FILTER (WHERE v1.gestante_dpp IS NOT NULL) OVER (PARTITION BY v1.gestante_nome, v1.gestante_data_de_nascimento, v1.gestante_dum ORDER BY v1.atendimento_data))[1] AS gestante_primeira_dpp,
                        CASE
                            WHEN v1.atendimento_data >= ANY (ARRAY[min(v1.gestante_dpp+ '14 days'::interval) FILTER (WHERE v1.gestante_dpp IS NOT NULL) OVER (PARTITION BY v1.gestante_nome, v1.gestante_data_de_nascimento ORDER BY v1.atendimento_data),v1.dt_registro_parto::timestamp without time zone, v1.dt_registro_aborto::timestamp without time zone]) THEN (array_agg(v1.gestante_dum) FILTER (WHERE v1.gestante_dum <> '3000-12-31'::date) OVER (PARTITION BY v1.gestante_nome, v1.gestante_data_de_nascimento,v1.gestante_dpp ORDER BY v1.atendimento_data))[1]
                            ELSE (array_agg(v1.gestante_dum) FILTER (WHERE v1.gestante_dum <> '3000-12-31'::date) OVER (PARTITION BY v1.gestante_nome, v1.gestante_data_de_nascimento ORDER BY v1.atendimento_data))[1]
                        END AS gestante_dum_primeiro_atendimento,
                    v1.gestante_dum,
                        /* Caso haja registro de DUM inválido e e data de atendimento igual ou inferior a uma data de registro de parto então retorna a primeira data de parto onde a DUM não é inválida e data de atendimento é menor ou igual a data de registro de parto.
                         * Senão retorna a data de parto mais recente particionado por nome da gestante, data de nascimento e DUM */
                        CASE
                            WHEN v1.gestante_dum = '3000-12-31'::date AND v1.atendimento_data <= v1.dt_registro_parto THEN (array_agg(v1.dt_registro_parto) FILTER (WHERE v1.gestante_dum <> '3000-12-31'::date AND v1.atendimento_data <= v1.dt_registro_parto) OVER (PARTITION BY v1.gestante_nome, v1.gestante_data_de_nascimento ORDER BY v1.atendimento_data))[1]
                            WHEN v1.dt_registro_parto <= v1.gestante_dum OR v1.gestante_dum = '3000-12-31'::date AND v1.atendimento_data >= v1.dt_registro_parto THEN NULL::date
                            ELSE max(v1.dt_registro_parto) FILTER (WHERE v1.gestante_dpp IS NOT NULL) OVER (PARTITION BY v1.gestante_nome, v1.gestante_data_de_nascimento, v1.gestante_dum)
                        END AS dt_registro_parto_mais_recente,
                        /* Caso haja registro de DUM inválido e e data de atendimento igual ou inferior a uma data de registro de aborto então retorna a primeira data de aborto onde a DUM não é inválida e data de atendimento é menor ou igual a data de registro de aborto.
                         * Senão retorna a data de aborto mais recente particionado por nome da gestante, data de nascimento e DUM */
                        CASE
                            WHEN v1.gestante_dum = '3000-12-31'::date THEN (array_agg(v1.dt_registro_aborto) FILTER (WHERE v1.gestante_dum <> '3000-12-31'::date AND v1.atendimento_data <= v1.dt_registro_aborto) OVER (PARTITION BY v1.gestante_nome, v1.gestante_data_de_nascimento ORDER BY v1.atendimento_data))[1]
                            WHEN v1.dt_registro_aborto <= v1.gestante_dum THEN NULL::date
                            ELSE max(v1.dt_registro_aborto) FILTER (WHERE v1.gestante_dpp IS NOT NULL) OVER (PARTITION BY v1.gestante_nome, v1.gestante_data_de_nascimento, v1.gestante_dum)
                        END AS dt_registro_aborto_mais_recente
                   FROM ( 
                   SELECT DISTINCT tfai.co_seq_fat_atd_ind,
                            tdt.dt_registro AS atendimento_data,
                            COALESCE(NULLIF(unidadecadastrorecente.nu_cnes::text, '-'::text), unidadeatendimentorecente.nu_cnes::text) AS estabelecimento_cnes,
                            COALESCE(NULLIF(unidadecadastrorecente.no_unidade_saude::text, 'Não informado'::text), unidadeatendimentorecente.no_unidade_saude::text) AS estabelecimento_nome,
                            COALESCE(NULLIF(equipeacadastrorecente.nu_ine::text, '-'::text), equipeatendimentorecente.nu_ine::text) AS equipe_ine,
                            COALESCE(NULLIF(equipeacadastrorecente.no_equipe::text, 'SEM EQUIPE'::text), equipeatendimentorecente.no_equipe::text) AS equipe_nome,
                            COALESCE(acsvisitarecente.no_profissional, acscadastrorecente.no_profissional) AS acs_nome,
                            acstempovisitarecente.dt_registro AS acs_data_ultima_visita,
                            tfai.co_fat_cidadao_pec,
                            tfcp.nu_cpf_cidadao AS gestante_documento_cpf,
                            tfcp.nu_cns AS gestante_documento_cns,
                            tfcp.no_cidadao AS gestante_nome,
                            tempocidadaopec.dt_registro AS gestante_data_de_nascimento,
                            tfcp.co_dim_tempo_nascimento AS gestante_data_nascimento_ts,
                            tfcp.nu_telefone_celular AS gestante_telefone,
                            NULLIF(concat(tfcd.no_logradouro, ', ', tfcd.nu_num_logradouro), ', '::text) AS gestante_endereco,
                                CASE
                                    WHEN tdtdum.nu_ano IS NOT NULL THEN tdtdum.dt_registro
                                    WHEN tfai.nu_idade_gestacional_semanas IS NOT NULL THEN (tdt.dt_registro - '7 days'::interval * tfai.nu_idade_gestacional_semanas::double precision)::date
                                    ELSE NULL::date
                                END AS gestante_dum,
                                CASE
                                    WHEN tdtdum.nu_ano <> 3000 THEN tdtdum.dt_registro + '294 days'::interval
                                    WHEN tfai.nu_idade_gestacional_semanas IS NOT NULL THEN tdt.dt_registro - '7 days'::interval * tfai.nu_idade_gestacional_semanas::double precision + '294 days'::interval
                                    ELSE NULL::timestamp without time zone
                                END AS gestante_dpp,
                                CASE
                                    WHEN tdtdum.nu_ano <> 3000 THEN (CURRENT_DATE - tdtdum.dt_registro) / 7
                                    WHEN tfai.nu_idade_gestacional_semanas IS NOT NULL THEN (CURRENT_DATE - (tdt.dt_registro - '7 days'::interval * tfai.nu_idade_gestacional_semanas::double precision)::date) / 7
                                    ELSE NULL::integer
                                END AS gestante_idade_gestacional,
                            tfai.nu_idade_gestacional_semanas AS gestante_idade_gestacional_atendimento,
                            ( SELECT max(tdtempoparto.dt_registro) AS max
                                   FROM tb_fat_atendimento_individual tfaiparto
                                     JOIN tb_fat_atd_ind_problemas tfaipparto ON tfaiparto.co_seq_fat_atd_ind = tfaipparto.co_fat_atd_ind
                                     JOIN tb_dim_tempo tdtempoparto ON tdtempoparto.co_seq_dim_tempo = tfaiparto.co_dim_tempo
                                     LEFT JOIN tb_dim_cid tdcidparto ON tdcidparto.co_seq_dim_cid = tfaipparto.co_dim_cid
                                     LEFT JOIN tb_dim_ciap tdciapparto ON tdciapparto.co_seq_dim_ciap = tfaipparto.co_dim_ciap
                                  WHERE (tfaiparto.co_fat_cidadao_pec IN ( SELECT tfcparto.co_seq_fat_cidadao_pec
   FROM tb_fat_cidadao_pec tfcparto
  WHERE tfcparto.no_cidadao::text = tfcp.no_cidadao::text AND tfcparto.co_dim_tempo_nascimento::text = tfcp.co_dim_tempo_nascimento::text)) AND ((tdciapparto.nu_ciap::text = ANY (ARRAY['W90'::text, 'W91'::text, 'W92'::text, 'W93'::text])) OR (tdcidparto.nu_cid::text = ANY (ARRAY['O80'::text, 'Z370'::text, 'Z379'::text, 'Z38'::text, 'Z39'::text, 'Z371'::text, 'Z379'::text, 'O42'::text, 'O45'::text, 'O60'::text, 'O61'::text, 'O62'::text, 'O63'::text, 'O64'::text, 'O65'::text, 'O66'::text, 'O67'::text, 'O68'::text, 'O69'::text, 'O70'::text, 'O71'::text, 'O73'::text, 'O750'::text, 'O751'::text, 'O754'::text, 'O755'::text, 'O756'::text, 'O757'::text, 'O758'::text, 'O759'::text, 'O81'::text, 'O82'::text, 'O83'::text, 'O84'::text, 'Z372'::text, 'Z375'::text, 'Z379'::text, 'Z38'::text, 'Z39'::text])))) AS dt_registro_parto,
                            ( SELECT max(tdtempoaborto.dt_registro) AS max
                                   FROM tb_fat_atendimento_individual tfaiaborto
                                     JOIN tb_fat_atd_ind_problemas tfaipaborto ON tfaiaborto.co_seq_fat_atd_ind = tfaipaborto.co_fat_atd_ind
                                     JOIN tb_dim_tempo tdtempoaborto ON tdtempoaborto.co_seq_dim_tempo = tfaiaborto.co_dim_tempo
                                     LEFT JOIN tb_dim_cid tdcidaborto ON tdcidaborto.co_seq_dim_cid = tfaipaborto.co_dim_cid
                                     LEFT JOIN tb_dim_ciap tdciapaborto ON tdciapaborto.co_seq_dim_ciap = tfaipaborto.co_dim_ciap
                                  WHERE (tfaiaborto.co_fat_cidadao_pec IN ( SELECT tfparto.co_seq_fat_cidadao_pec
   FROM tb_fat_cidadao_pec tfparto
  WHERE tfparto.no_cidadao::text = tfcp.no_cidadao::text AND tfparto.co_dim_tempo_nascimento::text = tfcp.co_dim_tempo_nascimento::text)) AND ((tdciapaborto.nu_ciap::text = ANY (ARRAY['W82'::text, 'W83'::text])) OR (tdcidaborto.nu_cid::text = ANY (ARRAY['O02'::text, 'O03'::text, 'O05'::text, 'O06'::text, 'O04'::text, 'Z303'::text])))) AS dt_registro_aborto
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
                                  WHERE (caddomiciliarfamilia.co_fat_cidadao_pec IN ( SELECT tfccaddomiciliarfamilia.co_seq_fat_cidadao_pec
   FROM tb_fat_cidadao_pec tfccaddomiciliarfamilia
  WHERE tfccaddomiciliarfamilia.no_cidadao::text = tfcp.no_cidadao::text AND tfccaddomiciliarfamilia.co_dim_tempo_nascimento = tfcp.co_dim_tempo_nascimento))
                                  ORDER BY cadomiciliar.co_dim_tempo DESC
                                 LIMIT 1))
                             LEFT JOIN tb_fat_visita_domiciliar tfvdrecente ON tfvdrecente.co_seq_fat_visita_domiciliar = (( SELECT visitadomiciliar.co_seq_fat_visita_domiciliar
                                   FROM tb_fat_visita_domiciliar visitadomiciliar
                                  WHERE (visitadomiciliar.co_fat_cidadao_pec IN ( SELECT tfcvisita.co_seq_fat_cidadao_pec
   FROM tb_fat_cidadao_pec tfcvisita
  WHERE tfcvisita.no_cidadao::text = tfcp.no_cidadao::text AND tfcvisita.co_dim_tempo_nascimento = tfcp.co_dim_tempo_nascimento))
                                  ORDER BY visitadomiciliar.co_dim_tempo DESC
                                 LIMIT 1))
                             LEFT JOIN tb_fat_cad_individual tfcirecente ON tfcirecente.co_seq_fat_cad_individual = (( SELECT cadastroindividual.co_seq_fat_cad_individual
                                   FROM tb_fat_cad_individual cadastroindividual
                                  WHERE (cadastroindividual.co_fat_cidadao_pec IN ( SELECT tfccadastro.co_seq_fat_cidadao_pec
   FROM tb_fat_cidadao_pec tfccadastro
  WHERE tfccadastro.no_cidadao::text = tfcp.no_cidadao::text AND tfccadastro.co_dim_tempo_nascimento = tfcp.co_dim_tempo_nascimento))
                                  ORDER BY cadastroindividual.co_dim_tempo DESC
                                 LIMIT 1))
                             LEFT JOIN tb_fat_atendimento_individual tfairecente ON tfairecente.co_seq_fat_atd_ind = (( SELECT atendimentoindividual.co_seq_fat_atd_ind
                                   FROM tb_fat_atendimento_individual atendimentoindividual
                                  WHERE (atendimentoindividual.co_fat_cidadao_pec IN ( SELECT tfcatendimento.co_seq_fat_cidadao_pec
   FROM tb_fat_cidadao_pec tfcatendimento
  WHERE tfcatendimento.no_cidadao::text = tfcp.no_cidadao::text AND tfcatendimento.co_dim_tempo_nascimento = tfcp.co_dim_tempo_nascimento))
                                  ORDER BY atendimentoindividual.co_dim_tempo DESC
                                 LIMIT 1))
                             LEFT JOIN tb_dim_equipe equipeatendimentorecente ON equipeatendimentorecente.co_seq_dim_equipe = tfairecente.co_dim_equipe_1
                             LEFT JOIN tb_dim_profissional profissinalatendimentorecente ON profissinalatendimentorecente.co_seq_dim_profissional = tfairecente.co_dim_profissional_1
                             LEFT JOIN tb_dim_unidade_saude unidadeatendimentorecente ON unidadeatendimentorecente.co_seq_dim_unidade_saude = tfairecente.co_dim_unidade_saude_1
                             LEFT JOIN tb_dim_equipe equipeacadastrorecente ON equipeacadastrorecente.co_seq_dim_equipe = tfcirecente.co_dim_equipe
                             LEFT JOIN tb_dim_profissional profissinalcadastrorecente ON profissinalcadastrorecente.co_seq_dim_profissional = tfcirecente.co_dim_profissional
                             LEFT JOIN tb_dim_unidade_saude unidadecadastrorecente ON unidadecadastrorecente.co_seq_dim_unidade_saude = tfcirecente.co_dim_unidade_saude
                             LEFT JOIN tb_dim_profissional acsvisitarecente ON acsvisitarecente.co_seq_dim_profissional = tfvdrecente.co_dim_profissional
                             LEFT JOIN tb_dim_profissional acscadastrorecente ON acscadastrorecente.co_seq_dim_profissional = tfcirecente.co_dim_profissional
                             LEFT JOIN tb_dim_tempo acstempovisitarecente ON tfvdrecente.co_dim_tempo = acstempovisitarecente.co_seq_dim_tempo
                          where (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2231%'::text, '2235%'::text, '2251%'::text, '2252%'::text, '2253%'::text])) AND ((tdciap.nu_ciap::text = ANY (ARRAY['ABP001'::text, 'W03'::text, 'W05'::text, 'W29'::text, 'W71'::text, 'W78'::text, 'W79'::text, 'W80'::text, 'W81'::text, 'W84'::text, 'W85'::text])) OR (tdcid.nu_cid::text = ANY (ARRAY['O11'::text, 'O120'::text, 'O121'::text, 'O122'::text, 'O13'::text, 'O140'::text, 'O141'::text, 'O149'::text, 'O150'::text, 'O151'::text, 'O159'::text, 'O16'::text, 'O200'::text, 'O208'::text, 'O209'::text, 'O210'::text, 'O211'::text, 'O212'::text, 'O218'::text, 'O219'::text, 'O220'::text, 'O221'::text, 'O222'::text, 'O223'::text, 'O224'::text, 'O225'::text, 'O228'::text, 'O229'::text, 'O230'::text, 'O231'::text, 'O232'::text, 'O233'::text, 'O234'::text, 'O235'::text, 'O239'::text, 'O299'::text, 'O300'::text, 'O301'::text, 'O302'::text, 'O308'::text, 'O309'::text, 'O311'::text, 'O312'::text, 'O318'::text, 'O320'::text, 'O321'::text, 'O322'::text, 'O323'::text, 'O324'::text, 'O325'::text, 'O326'::text, 'O328'::text, 'O329'::text, 'O330'::text, 'O331'::text, 'O332'::text, 'O333'::text, 'O334'::text, 'O335'::text, 'O336'::text, 'O337'::text, 'O338'::text, 'O752'::text, 'O753'::text, 'O990'::text, 'O991'::text, 'O992'::text, 'O993'::text, 'O994'::text, 'O240'::text, 'O241'::text, 'O242'::text, 'O243'::text, 'O244'::text, 'O249'::text, 'O25'::text, 'O260'::text, 'O261'::text, 'O263'::text, 'O264'::text, 'O265'::text, 'O268'::text, 'O269'::text, 'O280'::text, 'O281'::text, 'O282'::text, 'O283'::text, 'O284'::text, 'O285'::text, 'O288'::text, 'O289'::text, 'O290'::text, 'O291'::text, 'O292'::text, 'O293'::text, 'O294'::text, 'O295'::text, 'O296'::text, 'O298'::text, 'O009'::text, 'O339'::text, 'O340'::text, 'O341'::text, 'O342'::text, 'O343'::text, 'O344'::text, 'O345'::text, 'O346'::text, 'O347'::text, 'O348'::text, 'O349'::text, 'O350'::text, 'O351'::text, 'O352'::text, 'O353'::text, 'O354'::text, 'O355'::text, 'O356'::text, 'O357'::text, 'O358'::text, 'O359'::text, 'O360'::text, 'O361'::text, 'O362'::text, 'O363'::text, 'O365'::text, 'O366'::text, 'O367'::text, 'O368'::text, 'O369'::text, 'O40'::text, 'O410'::text, 'O411'::text, 'O418'::text, 'O419'::text, 'O430'::text, 'O431'::text, 'O438'::text, 'O439'::text, 'O440'::text, 'O441'::text, 'O460'::text, 'O468'::text, 'O469'::text, 'O470'::text, 'O471'::text, 'O479'::text, 'O48'::text, 'O995'::text, 'O996'::text, 'O997'::text, 'Z640'::text, 'O00'::text, 'O10'::text, 'O12'::text, 'O14'::text, 'O15'::text, 'O20'::text, 'O21'::text, 'O22'::text, 'O23'::text, 'O24'::text, 'O26'::text, 'O28'::text, 'O29'::text, 'O30'::text, 'O31'::text, 'O32'::text, 'O33'::text, 'O34'::text, 'O35'::text, 'O36'::text, 'O41'::text, 'O43'::text, 'O44'::text, 'O46'::text, 'O47'::text, 'O98'::text, 'Z34'::text, 'Z35'::text, 'Z36'::text, 'Z321'::text, 'Z33'::text, 'Z340'::text, 'Z348'::text, 'Z349'::text, 'Z350'::text, 'Z351'::text, 'Z352'::text, 'Z353'::text, 'Z354'::text, 'Z357'::text, 'Z358'::text, 'Z359'::text]))) AND tdt.dt_registro >= (( SELECT
CASE
 WHEN date_part('month'::text, CURRENT_DATE) >= 1::double precision AND date_part('month'::text, CURRENT_DATE) <= 4::double precision THEN concat(date_part('year'::text, (CURRENT_DATE - '365 days'::interval)::date), '-09-01')
 WHEN date_part('month'::text, CURRENT_DATE) >= 5::double precision AND date_part('month'::text, CURRENT_DATE) <= 8::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-01-01')
 WHEN date_part('month'::text, CURRENT_DATE) >= 9::double precision AND date_part('month'::text, CURRENT_DATE) <= 12::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-05-01')
 ELSE NULL::text
END::date - '294 days'::interval))
                          ORDER BY tfcp.no_cidadao
                          ) v1
                          ) v2 ) v3
  WHERE v3.r = 1