/* 
    Essa consulta retorna:
        1. Todos atendimentos de pré-natal, parto e aborto de cada gestante
        2. Informações de cadastro da gestante registrado no Cidadão PEC
        3. Checa a realização de consultas odontológicas, exames de hiv e sífilis a cada atendimento
        4. Vinculações de equipe e unidade no cadastro individual mais recente a cada atendimento
        5. Vinculações de ACS no cadastro domiciliar mais recente a cada atendimento

*/
WITH consultas_por_gestantes AS (
         SELECT v1.co_seq_fat_atd_ind,
            v1.co_dim_tempo,
            tdt.dt_registro AS atendimento_data,
            lag(tdt.dt_registro) OVER (PARTITION BY v1.gestante_nome, tdtnascimento.dt_registro ORDER BY tdt.dt_registro, v1.co_seq_fat_atd_ind) AS atendimento_anterior_data,
            v1.atendimento_nome,
            v1.co_dim_tempo_nascimento,
            v1.co_seq_fat_cidadao_pec,
            v1.gestante_nome,
            v1.gestante_documento_cns,
            v1.gestante_telefone,
            tdtnascimento.dt_registro AS gestante_data_de_nascimento,
                CASE
                    WHEN tdtdum.nu_ano IS NOT NULL THEN tdtdum.dt_registro
                    WHEN v1.nu_idade_gestacional_semanas IS NOT NULL THEN (tdt.dt_registro - '7 days'::interval * v1.nu_idade_gestacional_semanas::double precision)::date
                    ELSE NULL::date
                END AS gestante_dum,
            v1.nu_idade_gestacional_semanas AS idade_gestacional_semanas,
            unidadeatendimento.nu_cnes AS atendimento_unidade_cnes,
            unidadeatendimento.no_unidade_saude AS atendimento_unidade_nome,
            equipeatendimento.nu_ine AS atendimento_equipe_ine,
            equipeatendimento.no_equipe AS atendimento_equipes_nome,
            profissinalatendimento.no_profissional AS atendimento_profissional_nome,
            profissinalatendimento.nu_cns AS atendimento_profissional_cns
           FROM ( SELECT DISTINCT tfai.co_seq_fat_atd_ind,
                    tfai.co_dim_tempo,
                    'Pré-natal'::text AS atendimento_nome,
                    tfcp.co_seq_fat_cidadao_pec,
                    tfcp.no_cidadao AS gestante_nome,
                    tfcp.nu_cns AS gestante_documento_cns,
                    tfcp.nu_telefone_celular AS gestante_telefone,
                    tfcp.co_dim_tempo_nascimento,
                    tfai.co_dim_tempo_dum,
                    tfai.nu_idade_gestacional_semanas,
                    tfai.co_dim_unidade_saude_1,
                    tfai.co_dim_equipe_1,
                    tfai.co_dim_profissional_1
                   FROM esus_3169356_tresmarias_mg_20230314.tb_fat_atendimento_individual tfai
                     JOIN esus_3169356_tresmarias_mg_20230314.tb_fat_cidadao_pec tfcp ON tfcp.co_seq_fat_cidadao_pec = tfai.co_fat_cidadao_pec
                     JOIN esus_3169356_tresmarias_mg_20230314.tb_fat_atd_ind_problemas tfaip ON tfai.co_seq_fat_atd_ind = tfaip.co_fat_atd_ind
                     JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfai.co_dim_cbo_1
                     LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_cid tdcid ON tdcid.co_seq_dim_cid = tfaip.co_dim_cid
                     LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_ciap tdciap ON tdciap.co_seq_dim_ciap = tfaip.co_dim_ciap
                  WHERE (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2231%'::text, '2235%'::text, '2251%'::text, '2252%'::text, '2253%'::text])) AND ((tdciap.nu_ciap::text = ANY (ARRAY['ABP001'::text, 'W03'::text, 'W05'::text, 'W29'::text, 'W71'::text, 'W78'::text, 'W79'::text, 'W80'::text, 'W81'::text, 'W84'::text, 'W85'::text])) OR (tdcid.nu_cid::text = ANY (ARRAY['O11'::text, 'O120'::text, 'O121'::text, 'O122'::text, 'O13'::text, 'O140'::text, 'O141'::text, 'O149'::text, 'O150'::text, 'O151'::text, 'O159'::text, 'O16'::text, 'O200'::text, 'O208'::text, 'O209'::text, 'O210'::text, 'O211'::text, 'O212'::text, 'O218'::text, 'O219'::text, 'O220'::text, 'O221'::text, 'O222'::text, 'O223'::text, 'O224'::text, 'O225'::text, 'O228'::text, 'O229'::text, 'O230'::text, 'O231'::text, 'O232'::text, 'O233'::text, 'O234'::text, 'O235'::text, 'O239'::text, 'O299'::text, 'O300'::text, 'O301'::text, 'O302'::text, 'O308'::text, 'O309'::text, 'O311'::text, 'O312'::text, 'O318'::text, 'O320'::text, 'O321'::text, 'O322'::text, 'O323'::text, 'O324'::text, 'O325'::text, 'O326'::text, 'O328'::text, 'O329'::text, 'O330'::text, 'O331'::text, 'O332'::text, 'O333'::text, 'O334'::text, 'O335'::text, 'O336'::text, 'O337'::text, 'O338'::text, 'O752'::text, 'O753'::text, 'O990'::text, 'O991'::text, 'O992'::text, 'O993'::text, 'O994'::text, 'O240'::text, 'O241'::text, 'O242'::text, 'O243'::text, 'O244'::text, 'O249'::text, 'O25'::text, 'O260'::text, 'O261'::text, 'O263'::text, 'O264'::text, 'O265'::text, 'O268'::text, 'O269'::text, 'O280'::text, 'O281'::text, 'O282'::text, 'O283'::text, 'O284'::text, 'O285'::text, 'O288'::text, 'O289'::text, 'O290'::text, 'O291'::text, 'O292'::text, 'O293'::text, 'O294'::text, 'O295'::text, 'O296'::text, 'O298'::text, 'O009'::text, 'O339'::text, 'O340'::text, 'O341'::text, 'O342'::text, 'O343'::text, 'O344'::text, 'O345'::text, 'O346'::text, 'O347'::text, 'O348'::text, 'O349'::text, 'O350'::text, 'O351'::text, 'O352'::text, 'O353'::text, 'O354'::text, 'O355'::text, 'O356'::text, 'O357'::text, 'O358'::text, 'O359'::text, 'O360'::text, 'O361'::text, 'O362'::text, 'O363'::text, 'O365'::text, 'O366'::text, 'O367'::text, 'O368'::text, 'O369'::text, 'O40'::text, 'O410'::text, 'O411'::text, 'O418'::text, 'O419'::text, 'O430'::text, 'O431'::text, 'O438'::text, 'O439'::text, 'O440'::text, 'O441'::text, 'O460'::text, 'O468'::text, 'O469'::text, 'O470'::text, 'O471'::text, 'O479'::text, 'O48'::text, 'O995'::text, 'O996'::text, 'O997'::text, 'Z640'::text, 'O00'::text, 'O10'::text, 'O12'::text, 'O14'::text, 'O15'::text, 'O20'::text, 'O21'::text, 'O22'::text, 'O23'::text, 'O24'::text, 'O26'::text, 'O28'::text, 'O29'::text, 'O30'::text, 'O31'::text, 'O32'::text, 'O33'::text, 'O34'::text, 'O35'::text, 'O36'::text, 'O41'::text, 'O43'::text, 'O44'::text, 'O46'::text, 'O47'::text, 'O98'::text, 'Z34'::text, 'Z35'::text, 'Z36'::text, 'Z321'::text, 'Z33'::text, 'Z340'::text, 'Z348'::text, 'Z349'::text, 'Z350'::text, 'Z351'::text, 'Z352'::text, 'Z353'::text, 'Z354'::text, 'Z357'::text, 'Z358'::text, 'Z359'::text])))
                UNION ALL
                 SELECT DISTINCT tfaiparto.co_seq_fat_atd_ind,
                    tfaiparto.co_dim_tempo,
                    'Parto'::text AS atendimento_nome,
                    tfcp.co_seq_fat_cidadao_pec,
                    tfcp.no_cidadao AS gestante_nome,
                    tfcp.nu_cns AS gestante_documento_cns,
                    tfcp.nu_telefone_celular AS gestante_telefone,
                    tfcp.co_dim_tempo_nascimento,
                    tfaiparto.co_dim_tempo_dum,
                    tfaiparto.nu_idade_gestacional_semanas,
                    tfaiparto.co_dim_unidade_saude_1,
                    tfaiparto.co_dim_equipe_1,
                    tfaiparto.co_dim_profissional_1
                   FROM esus_3169356_tresmarias_mg_20230314.tb_fat_atendimento_individual tfaiparto
                     JOIN esus_3169356_tresmarias_mg_20230314.tb_fat_cidadao_pec tfcp ON tfcp.co_seq_fat_cidadao_pec = tfaiparto.co_fat_cidadao_pec
                     JOIN esus_3169356_tresmarias_mg_20230314.tb_fat_atd_ind_problemas tfaipparto ON tfaiparto.co_seq_fat_atd_ind = tfaipparto.co_fat_atd_ind
                     JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfaiparto.co_dim_cbo_1
                     LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_cid tdcidparto ON tdcidparto.co_seq_dim_cid = tfaipparto.co_dim_cid
                     LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_ciap tdciapparto ON tdciapparto.co_seq_dim_ciap = tfaipparto.co_dim_ciap
                  WHERE (tdciapparto.nu_ciap::text = ANY (ARRAY['W90'::text, 'W91'::text, 'W92'::text, 'W93'::text])) OR (tdcidparto.nu_cid::text = ANY (ARRAY['O80'::text, 'Z370'::text, 'Z379'::text, 'Z38'::text, 'Z39'::text, 'Z371'::text, 'Z379'::text, 'O42'::text, 'O45'::text, 'O60'::text, 'O61'::text, 'O62'::text, 'O63'::text, 'O64'::text, 'O65'::text, 'O66'::text, 'O67'::text, 'O68'::text, 'O69'::text, 'O70'::text, 'O71'::text, 'O73'::text, 'O750'::text, 'O751'::text, 'O754'::text, 'O755'::text, 'O756'::text, 'O757'::text, 'O758'::text, 'O759'::text, 'O81'::text, 'O82'::text, 'O83'::text, 'O84'::text, 'Z372'::text, 'Z375'::text, 'Z379'::text, 'Z38'::text, 'Z39'::text]))
                UNION ALL
                 SELECT DISTINCT tfaiaborto.co_seq_fat_atd_ind,
                    tfaiaborto.co_dim_tempo,
                    'Aborto'::text AS atendimento_nome,
                    tfcp.co_seq_fat_cidadao_pec,
                    tfcp.no_cidadao AS gestante_nome,
                    tfcp.nu_cns AS gestante_documento_cns,
                    tfcp.nu_telefone_celular AS gestante_telefone,
                    tfcp.co_dim_tempo_nascimento,
                    tfaiaborto.co_dim_tempo_dum,
                    tfaiaborto.nu_idade_gestacional_semanas,
                    tfaiaborto.co_dim_unidade_saude_1,
                    tfaiaborto.co_dim_equipe_1,
                    tfaiaborto.co_dim_profissional_1
                   FROM esus_3169356_tresmarias_mg_20230314.tb_fat_atendimento_individual tfaiaborto
                     JOIN esus_3169356_tresmarias_mg_20230314.tb_fat_cidadao_pec tfcp ON tfcp.co_seq_fat_cidadao_pec = tfaiaborto.co_fat_cidadao_pec
                     JOIN esus_3169356_tresmarias_mg_20230314.tb_fat_atd_ind_problemas tfaipaborto ON tfaiaborto.co_seq_fat_atd_ind = tfaipaborto.co_fat_atd_ind
                     JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfaiaborto.co_dim_cbo_1
                     LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_cid tdcidaborto ON tdcidaborto.co_seq_dim_cid = tfaipaborto.co_dim_cid
                     LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_ciap tdciapaborto ON tdciapaborto.co_seq_dim_ciap = tfaipaborto.co_dim_ciap
                  WHERE (tdciapaborto.nu_ciap::text = ANY (ARRAY['W82'::text, 'W83'::text])) OR (tdcidaborto.nu_cid::text = ANY (ARRAY['O02'::text, 'O03'::text, 'O05'::text, 'O06'::text, 'O04'::text, 'Z303'::text]))) v1
             LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_unidade_saude unidadeatendimento ON unidadeatendimento.co_seq_dim_unidade_saude = v1.co_dim_unidade_saude_1
             LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_equipe equipeatendimento ON equipeatendimento.co_seq_dim_equipe = v1.co_dim_equipe_1
             LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_profissional profissinalatendimento ON profissinalatendimento.co_seq_dim_profissional = v1.co_dim_profissional_1
             JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_tempo tdt ON v1.co_dim_tempo = tdt.co_seq_dim_tempo
             JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_tempo tdtdum ON v1.co_dim_tempo_dum = tdtdum.co_seq_dim_tempo
             JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_tempo tdtnascimento ON v1.co_dim_tempo_nascimento = tdtnascimento.co_seq_dim_tempo
        ), identifica_atendimento_odontologico AS (
         SELECT DISTINCT otdtempo.dt_registro,
            otfodont.co_fat_cidadao_pec
           FROM esus_3169356_tresmarias_mg_20230314.tb_fat_atendimento_odonto otfodont
             JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_cbo otdcbo ON otdcbo.co_seq_dim_cbo = otfodont.co_dim_cbo_1
             JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_tempo otdtempo ON otdtempo.co_seq_dim_tempo = otfodont.co_dim_tempo
          WHERE otdcbo.nu_cbo::text ~~ '2232%'::text
        ), identifica_exame_sifilis AS (
         SELECT DISTINCT v1.dt_registro,
            v1.co_fat_cidadao_pec
           FROM ( SELECT tdtempo.dt_registro,
                    tfpap.co_fat_cidadao_pec
                   FROM esus_3169356_tresmarias_mg_20230314.tb_fat_proced_atend_proced tfpap
                     JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfpap.co_dim_procedimento
                     JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfpap.co_dim_cbo
                     JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfpap.co_dim_tempo
                  WHERE (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND (tdp.co_proced::text = ANY (ARRAY['0214010058'::text, '0214010040'::text, 'ABPG024'::text]))
                UNION ALL
                 SELECT tdtempo.dt_registro,
                    tfaip.co_fat_cidadao_pec
                   FROM esus_3169356_tresmarias_mg_20230314.tb_fat_atd_ind_procedimentos tfaip
                     JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfaip.co_dim_procedimento_avaliado
                     JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfaip.co_dim_cbo_1
                     JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfaip.co_dim_tempo
                  WHERE (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND (tdp.co_proced::text = ANY (ARRAY['0202030300'::text, 'ABEX018'::text]))) v1
        ), identifica_exame_hiv AS (
         SELECT DISTINCT v1.dt_registro,
            v1.co_fat_cidadao_pec
           FROM ( SELECT tdtempo.dt_registro,
                    tfpap.co_fat_cidadao_pec
                   FROM esus_3169356_tresmarias_mg_20230314.tb_fat_proced_atend_proced tfpap
                     JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfpap.co_dim_procedimento
                     JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfpap.co_dim_cbo
                     JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfpap.co_dim_tempo
                  WHERE (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND (tdp.co_proced::text = ANY (ARRAY['0214010074'::text, '0214010082'::text, 'ABPG026'::text]))
                UNION ALL
                 SELECT tdtempo.dt_registro,
                    tfaip.co_fat_cidadao_pec
                   FROM esus_3169356_tresmarias_mg_20230314.tb_fat_atd_ind_procedimentos tfaip
                     JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfaip.co_dim_procedimento_avaliado
                     JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfaip.co_dim_cbo_1
                     JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfaip.co_dim_tempo
                  WHERE (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND (tdp.co_proced::text = ANY (ARRAY['0202031110'::text, '0202031179'::text, 'ABEX019'::text]))) v1
        ), adiciona_exames_no_prenatal AS (
         SELECT prenatal.co_seq_fat_atd_ind,
            prenatal.co_dim_tempo,
            prenatal.atendimento_data,
            prenatal.atendimento_anterior_data,
            prenatal.atendimento_nome,
            prenatal.co_seq_fat_cidadao_pec,
            prenatal.gestante_nome,
            prenatal.gestante_documento_cns,
            prenatal.gestante_telefone,
            prenatal.gestante_data_de_nascimento,
            prenatal.co_dim_tempo_nascimento,
            prenatal.gestante_dum,
            prenatal.idade_gestacional_semanas,
            prenatal.atendimento_unidade_cnes,
            prenatal.atendimento_unidade_nome,
            prenatal.atendimento_equipe_ine,
            prenatal.atendimento_equipes_nome,
            prenatal.atendimento_profissional_nome,
            prenatal.atendimento_profissional_cns,
                CASE
                    WHEN odonto.dt_registro IS NOT NULL THEN true
                    ELSE false
                END AS atendimento_odontologico_realizado,
                CASE
                    WHEN examesifilis.dt_registro IS NOT NULL THEN true
                    ELSE false
                END AS exame_sifilis_realizado,
                CASE
                    WHEN examehiv.dt_registro IS NOT NULL THEN true
                    ELSE false
                END AS exame_hiv_realizado
           FROM consultas_por_gestantes prenatal
             LEFT JOIN identifica_atendimento_odontologico odonto ON prenatal.co_seq_fat_cidadao_pec = odonto.co_fat_cidadao_pec AND odonto.dt_registro > prenatal.atendimento_anterior_data AND odonto.dt_registro <= prenatal.atendimento_data
             LEFT JOIN identifica_exame_sifilis examesifilis ON prenatal.co_seq_fat_cidadao_pec = examesifilis.co_fat_cidadao_pec AND examesifilis.dt_registro > prenatal.atendimento_anterior_data AND examesifilis.dt_registro <= prenatal.atendimento_data
             LEFT JOIN identifica_exame_hiv examehiv ON prenatal.co_seq_fat_cidadao_pec = examehiv.co_fat_cidadao_pec AND examehiv.dt_registro > prenatal.atendimento_anterior_data AND examehiv.dt_registro <= prenatal.atendimento_data
        ), vincula_cadastro_individual_e_territorial AS (
         SELECT prenatal.co_seq_fat_atd_ind,
            prenatal.co_dim_tempo,
            prenatal.atendimento_data,
            prenatal.atendimento_anterior_data,
            prenatal.atendimento_nome,
            prenatal.atendimento_profissional_nome,
            prenatal.atendimento_profissional_cns,
            prenatal.co_seq_fat_cidadao_pec,
            prenatal.gestante_nome,
            prenatal.gestante_documento_cns,
            prenatal.gestante_telefone,
            prenatal.gestante_data_de_nascimento,
            prenatal.gestante_dum,
            prenatal.idade_gestacional_semanas,
            NULLIF(concat(tfcd.no_logradouro, ', ', tfcd.nu_num_logradouro), ', '::text) AS gestante_endereco,
            prenatal.atendimento_unidade_cnes,
            prenatal.atendimento_unidade_nome,
            unidadecadastro.nu_cnes AS cadastro_recente_unidade_cnes,
            unidadecadastro.no_unidade_saude AS cadastro_recente_unidade_nome,
            prenatal.atendimento_equipe_ine,
            prenatal.atendimento_equipes_nome,
            equipeacadastrorecente.nu_ine AS cadastro_recente_equipe_ine,
            equipeacadastrorecente.no_equipe AS cadastro_recente_equipe_nome,
            acscadastrorecente.no_profissional AS cadastro_individual_recente_acs_nome,
            acscadastrodomiciliarrecente.no_profissional AS cadastro_domiciliar_recente_acs_nome,
            acsvisitarecente.no_profissional AS visita_recente_acs_nome,
            acstempovisitarecente.dt_registro AS visita_recente_acs_data,
            prenatal.atendimento_odontologico_realizado,
            prenatal.exame_sifilis_realizado,
            prenatal.exame_hiv_realizado
           FROM adiciona_exames_no_prenatal prenatal
             LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_fat_cad_individual tfcirecente ON tfcirecente.co_seq_fat_cad_individual = (( SELECT cadastroindividual.co_seq_fat_cad_individual
                   FROM esus_3169356_tresmarias_mg_20230314.tb_fat_cad_individual cadastroindividual
                  WHERE cadastroindividual.co_fat_cidadao_pec = prenatal.co_seq_fat_cidadao_pec AND cadastroindividual.co_dim_tempo <= prenatal.co_dim_tempo
                  ORDER BY cadastroindividual.co_dim_tempo DESC
                 LIMIT 1))
             LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_unidade_saude unidadecadastro ON unidadecadastro.co_seq_dim_unidade_saude = tfcirecente.co_dim_unidade_saude
             LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_equipe equipeacadastrorecente ON equipeacadastrorecente.co_seq_dim_equipe = tfcirecente.co_dim_equipe
             LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_fat_cad_domiciliar tfcd ON tfcd.co_seq_fat_cad_domiciliar = (( SELECT cadomiciliar.co_seq_fat_cad_domiciliar
                   FROM esus_3169356_tresmarias_mg_20230314.tb_fat_cad_dom_familia caddomiciliarfamilia
                     JOIN esus_3169356_tresmarias_mg_20230314.tb_fat_cad_domiciliar cadomiciliar ON cadomiciliar.co_seq_fat_cad_domiciliar = caddomiciliarfamilia.co_fat_cad_domiciliar
                  WHERE caddomiciliarfamilia.co_fat_cidadao_pec = prenatal.co_seq_fat_cidadao_pec AND caddomiciliarfamilia.co_dim_tempo <= prenatal.co_dim_tempo
                  ORDER BY cadomiciliar.co_dim_tempo DESC
                 LIMIT 1))
             LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_profissional acscadastrorecente ON acscadastrorecente.co_seq_dim_profissional = tfcirecente.co_dim_profissional
             LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_profissional acscadastrodomiciliarrecente ON acscadastrodomiciliarrecente.co_seq_dim_profissional = tfcd.co_dim_profissional
             LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_fat_visita_domiciliar tfvdrecente ON tfvdrecente.co_seq_fat_visita_domiciliar = (( SELECT visitadomiciliar.co_seq_fat_visita_domiciliar
                   FROM esus_3169356_tresmarias_mg_20230314.tb_fat_visita_domiciliar visitadomiciliar
                  WHERE visitadomiciliar.co_fat_cidadao_pec = prenatal.co_seq_fat_cidadao_pec AND visitadomiciliar.co_dim_tempo <= prenatal.co_dim_tempo
                  ORDER BY visitadomiciliar.co_dim_tempo DESC
                 LIMIT 1))
             LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_profissional acsvisitarecente ON acsvisitarecente.co_seq_dim_profissional = tfvdrecente.co_dim_profissional
             LEFT JOIN esus_3169356_tresmarias_mg_20230314.tb_dim_tempo acstempovisitarecente ON tfvdrecente.co_dim_tempo = acstempovisitarecente.co_seq_dim_tempo
        )
 SELECT tb1.co_seq_fat_atd_ind,
    tb1.atendimento_data,
    tb1.atendimento_nome,
    tb1.atendimento_profissional_nome,
    tb1.atendimento_profissional_cns,
    tb1.atendimento_unidade_cnes,
    tb1.atendimento_unidade_nome,
    tb1.cadastro_recente_unidade_cnes,
    tb1.cadastro_recente_unidade_nome,
    tb1.atendimento_equipe_ine,
    tb1.atendimento_equipes_nome,
    tb1.cadastro_recente_equipe_ine,
    tb1.cadastro_recente_equipe_nome,
    tb1.co_seq_fat_cidadao_pec,
    tb1.gestante_nome,
    tb1.gestante_documento_cns,
    tb1.gestante_telefone,
    tb1.gestante_data_de_nascimento,
    tb1.gestante_dum,
    tb1.idade_gestacional_semanas,
    tb1.gestante_endereco,
    tb1.cadastro_individual_recente_acs_nome,
    tb1.cadastro_domiciliar_recente_acs_nome,
    tb1.visita_recente_acs_nome,
    tb1.visita_recente_acs_data,
    tb1.atendimento_odontologico_realizado,
    tb1.exame_sifilis_realizado,
    tb1.exame_hiv_realizado
   FROM vincula_cadastro_individual_e_territorial tb1
  WHERE tb1.atendimento_data >= (
        CASE
            WHEN date_part('month'::text, CURRENT_DATE) >= 1::double precision AND date_part('month'::text, CURRENT_DATE) <= 4::double precision THEN concat(date_part('year'::text, (CURRENT_DATE - '365 days'::interval)::date), '-09-01')
            WHEN date_part('month'::text, CURRENT_DATE) >= 5::double precision AND date_part('month'::text, CURRENT_DATE) <= 8::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-01-01')
            WHEN date_part('month'::text, CURRENT_DATE) >= 9::double precision AND date_part('month'::text, CURRENT_DATE) <= 12::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-05-01')
            ELSE NULL::text
        END::date - '294 days'::interval)
  ORDER BY tb1.gestante_nome, tb1.atendimento_data DESC