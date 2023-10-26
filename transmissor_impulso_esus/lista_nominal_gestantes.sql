-- Lista Pré natal 
/* 
    Base com o histórico de registros referentes aos indicadores de pré-natal
    
    Granularidade - cada linha é um registro único das seguintes ocorrências:
      * consultas de pré-natal
      * registros de parto ou aborto
      * atendimento odontológico
      * exames de sífilis e HIV
    
    Janela de observação: nove meses anteriores antes do início do último quadrimestre
    Objetivo: trazer histórico completo de atendimento de gestações encerradas no quadrimeste anterior, atual e futuros
*/
WITH atendimentos_pre_natal AS (
         SELECT DISTINCT tfai.co_seq_fat_atd_ind::text AS id_registro,
            'consulta_pre_natal'::text AS tipo_registro,
            tdt.dt_registro AS data_registro,
            tfcp.no_cidadao::text || tfcp.co_dim_tempo_nascimento AS chave_gestante,
            tfcp.no_cidadao AS gestante_nome,
            tempocidadaopec.dt_registro AS gestante_data_de_nascimento,
            tfcp.nu_cpf_cidadao AS gestante_documento_cpf,
            tfcp.nu_cns AS gestante_documento_cns,
            tfcp.nu_telefone_celular AS gestante_telefone,
            tdtdum.dt_registro AS data_dum,
            tfai.nu_idade_gestacional_semanas AS idade_gestacional_atendimento,
            tdprof.nu_cns AS profissional_cns_atendimento,
            tdprof.no_profissional AS profissional_nome_atendimento,
            uns.nu_cnes AS estabelecimento_cnes_atendimento,
            uns.no_unidade_saude AS estabelecimento_nome_atendimento,
            eq.nu_ine AS equipe_ine_atendimento,
            eq.no_equipe AS equipe_nome_atendimento
           FROM public.tb_fat_atendimento_individual tfai
             JOIN public.tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfai.co_dim_cbo_1
             JOIN public.tb_dim_tempo tdt ON tfai.co_dim_tempo = tdt.co_seq_dim_tempo
             JOIN public.tb_dim_tempo tdtdum ON tfai.co_dim_tempo_dum = tdtdum.co_seq_dim_tempo
             JOIN public.tb_fat_atd_ind_problemas tfaip ON tfai.co_seq_fat_atd_ind = tfaip.co_fat_atd_ind
             LEFT JOIN public.tb_dim_profissional tdprof ON tdprof.co_seq_dim_profissional = tfai.co_dim_profissional_1
             LEFT JOIN public.tb_dim_equipe eq ON eq.co_seq_dim_equipe = tfai.co_dim_equipe_1
             LEFT JOIN public.tb_dim_unidade_saude uns ON uns.co_seq_dim_unidade_saude = tfai.co_dim_unidade_saude_1
             JOIN public.tb_fat_cidadao_pec tfcp ON tfcp.co_seq_fat_cidadao_pec = tfai.co_fat_cidadao_pec
             JOIN public.tb_dim_tempo tempocidadaopec ON tempocidadaopec.co_seq_dim_tempo = tfcp.co_dim_tempo_nascimento
             LEFT JOIN public.tb_dim_cid tdcid ON tdcid.co_seq_dim_cid = tfaip.co_dim_cid
             LEFT JOIN public.tb_dim_ciap tdciap ON tdciap.co_seq_dim_ciap = tfaip.co_dim_ciap
          WHERE (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2231%'::text, '2235%'::text, '2251%'::text, '2252%'::text, '2253%'::text])) AND ((tdciap.nu_ciap::text = ANY (ARRAY['ABP001'::text, 'W03'::text, 'W05'::text, 'W29'::text, 'W71'::text, 'W78'::text, 'W79'::text, 'W80'::text, 'W81'::text, 'W84'::text, 'W85'::text])) OR (tdcid.nu_cid::text = ANY (ARRAY['O11'::text, 'O120'::text, 'O121'::text, 'O122'::text, 'O13'::text, 'O140'::text, 'O141'::text, 'O149'::text, 'O150'::text, 'O151'::text, 'O159'::text, 'O16'::text, 'O200'::text, 'O208'::text, 'O209'::text, 'O210'::text, 'O211'::text, 'O212'::text, 'O218'::text, 'O219'::text, 'O220'::text, 'O221'::text, 'O222'::text, 'O223'::text, 'O224'::text, 'O225'::text, 'O228'::text, 'O229'::text, 'O230'::text, 'O231'::text, 'O232'::text, 'O233'::text, 'O234'::text, 'O235'::text, 'O239'::text, 'O299'::text, 'O300'::text, 'O301'::text, 'O302'::text, 'O308'::text, 'O309'::text, 'O311'::text, 'O312'::text, 'O318'::text, 'O320'::text, 'O321'::text, 'O322'::text, 'O323'::text, 'O324'::text, 'O325'::text, 'O326'::text, 'O328'::text, 'O329'::text, 'O330'::text, 'O331'::text, 'O332'::text, 'O333'::text, 'O334'::text, 'O335'::text, 'O336'::text, 'O337'::text, 'O338'::text, 'O752'::text, 'O753'::text, 'O990'::text, 'O991'::text, 'O992'::text, 'O993'::text, 'O994'::text, 'O240'::text, 'O241'::text, 'O242'::text, 'O243'::text, 'O244'::text, 'O249'::text, 'O25'::text, 'O260'::text, 'O261'::text, 'O263'::text, 'O264'::text, 'O265'::text, 'O268'::text, 'O269'::text, 'O280'::text, 'O281'::text, 'O282'::text, 'O283'::text, 'O284'::text, 'O285'::text, 'O288'::text, 'O289'::text, 'O290'::text, 'O291'::text, 'O292'::text, 'O293'::text, 'O294'::text, 'O295'::text, 'O296'::text, 'O298'::text, 'O009'::text, 'O339'::text, 'O340'::text, 'O341'::text, 'O342'::text, 'O343'::text, 'O344'::text, 'O345'::text, 'O346'::text, 'O347'::text, 'O348'::text, 'O349'::text, 'O350'::text, 'O351'::text, 'O352'::text, 'O353'::text, 'O354'::text, 'O355'::text, 'O356'::text, 'O357'::text, 'O358'::text, 'O359'::text, 'O360'::text, 'O361'::text, 'O362'::text, 'O363'::text, 'O365'::text, 'O366'::text, 'O367'::text, 'O368'::text, 'O369'::text, 'O40'::text, 'O410'::text, 'O411'::text, 'O418'::text, 'O419'::text, 'O430'::text, 'O431'::text, 'O438'::text, 'O439'::text, 'O440'::text, 'O441'::text, 'O460'::text, 'O468'::text, 'O469'::text, 'O470'::text, 'O471'::text, 'O479'::text, 'O48'::text, 'O995'::text, 'O996'::text, 'O997'::text, 'Z640'::text, 'O00'::text, 'O10'::text, 'O12'::text, 'O14'::text, 'O15'::text, 'O20'::text, 'O21'::text, 'O22'::text, 'O23'::text, 'O24'::text, 'O26'::text, 'O28'::text, 'O29'::text, 'O30'::text, 'O31'::text, 'O32'::text, 'O33'::text, 'O34'::text, 'O35'::text, 'O36'::text, 'O41'::text, 'O43'::text, 'O44'::text, 'O46'::text, 'O47'::text, 'O98'::text, 'Z34'::text, 'Z35'::text, 'Z36'::text, 'Z321'::text, 'Z33'::text, 'Z340'::text, 'Z348'::text, 'Z349'::text, 'Z350'::text, 'Z351'::text, 'Z352'::text, 'Z353'::text, 'Z354'::text, 'Z357'::text, 'Z358'::text, 'Z359'::text]))) AND tdt.dt_registro >= (( SELECT
                        CASE
                            WHEN date_part('month'::text, CURRENT_DATE) >= 1::double precision AND date_part('month'::text, CURRENT_DATE) <= 4::double precision THEN concat(date_part('year'::text, (CURRENT_DATE - '365 days'::interval)::date), '-09-01')
                            WHEN date_part('month'::text, CURRENT_DATE) >= 5::double precision AND date_part('month'::text, CURRENT_DATE) <= 8::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-01-01')
                            WHEN date_part('month'::text, CURRENT_DATE) >= 9::double precision AND date_part('month'::text, CURRENT_DATE) <= 12::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-05-01')
                            ELSE NULL::text
                        END::date - '294 days'::interval))
        ), gestantes_unicas AS (
         SELECT apn.chave_gestante,
            min(apn.data_registro) AS consulta_prenatal_primeira_data
           FROM atendimentos_pre_natal apn
          GROUP BY apn.chave_gestante
        ), registros_parto AS (
         SELECT DISTINCT tfaiparto.co_seq_fat_atd_ind::text AS id_registro,
            'registro_de_parto'::text AS tipo_registro,
            tdtempoparto.dt_registro AS data_registro,
            gu.chave_gestante,
            tfcp.no_cidadao AS gestante_nome,
            NULL::date AS gestante_data_de_nascimento,
            tfcp.nu_cpf_cidadao AS gestante_documento_cpf,
            tfcp.nu_cns AS gestante_documento_cns,
            tfcp.nu_telefone_celular AS gestante_telefone,
            NULL::date AS data_dum,
            NULL::integer AS idade_gestacional_atendimento,
            tdprof.nu_cns AS profissional_cns_atendimento,
            tdprof.no_profissional AS profissional_nome_atendimento,
            uns.nu_cnes AS estabelecimento_cnes_atendimento,
            uns.no_unidade_saude AS estabelecimento_nome_atendimento,
            eq.nu_ine AS equipe_ine_atendimento,
            eq.no_equipe AS equipe_nome_atendimento
           FROM public.tb_fat_atendimento_individual tfaiparto
             JOIN public.tb_fat_atd_ind_problemas tfaipparto ON tfaiparto.co_seq_fat_atd_ind = tfaipparto.co_fat_atd_ind
             JOIN public.tb_dim_tempo tdtempoparto ON tdtempoparto.co_seq_dim_tempo = tfaiparto.co_dim_tempo
             LEFT JOIN public.tb_dim_profissional tdprof ON tdprof.co_seq_dim_profissional = tfaiparto.co_dim_profissional_1
             LEFT JOIN public.tb_dim_equipe eq ON eq.co_seq_dim_equipe = tfaiparto.co_dim_equipe_1
             LEFT JOIN public.tb_dim_unidade_saude uns ON uns.co_seq_dim_unidade_saude = tfaiparto.co_dim_unidade_saude_1
             LEFT JOIN public.tb_dim_cid tdcidparto ON tdcidparto.co_seq_dim_cid = tfaipparto.co_dim_cid
             LEFT JOIN public.tb_dim_ciap tdciapparto ON tdciapparto.co_seq_dim_ciap = tfaipparto.co_dim_ciap
             LEFT JOIN public.tb_fat_cidadao_pec tfcp ON tfaiparto.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec
             JOIN gestantes_unicas gu ON gu.chave_gestante = (tfcp.no_cidadao::text || tfcp.co_dim_tempo_nascimento)
          WHERE ((tdciapparto.nu_ciap::text = ANY (ARRAY['W90'::text, 'W91'::text, 'W92'::text, 'W93'::text])) OR (tdcidparto.nu_cid::text = ANY (ARRAY['O80'::text, 'Z370'::text, 'Z379'::text, 'Z38'::text, 'Z39'::text, 'Z371'::text, 'Z379'::text, 'O42'::text, 'O45'::text, 'O60'::text, 'O61'::text, 'O62'::text, 'O63'::text, 'O64'::text, 'O65'::text, 'O66'::text, 'O67'::text, 'O68'::text, 'O69'::text, 'O70'::text, 'O71'::text, 'O73'::text, 'O750'::text, 'O751'::text, 'O754'::text, 'O755'::text, 'O756'::text, 'O757'::text, 'O758'::text, 'O759'::text, 'O81'::text, 'O82'::text, 'O83'::text, 'O84'::text, 'Z372'::text, 'Z375'::text, 'Z379'::text, 'Z38'::text, 'Z39'::text]))) AND tdtempoparto.dt_registro >= gu.consulta_prenatal_primeira_data
        ), registros_aborto AS (
         SELECT DISTINCT tfaiaborto.co_seq_fat_atd_ind::text AS id_registro,
            'registro_de_aborto'::text AS tipo_registro,
            tdtempoaborto.dt_registro AS data_registro,
            gu.chave_gestante,
            tfcp.no_cidadao AS gestante_nome,
            NULL::date AS gestante_data_de_nascimento,
            tfcp.nu_cpf_cidadao AS gestante_documento_cpf,
            tfcp.nu_cns AS gestante_documento_cns,
            tfcp.nu_telefone_celular AS gestante_telefone,
            NULL::date AS data_dum,
            NULL::integer AS idade_gestacional_atendimento,
            tdprof.nu_cns AS profissional_cns_atendimento,
            tdprof.no_profissional AS profissional_nome_atendimento,
            uns.nu_cnes AS estabelecimento_cnes_atendimento,
            uns.no_unidade_saude AS estabelecimento_nome_atendimento,
            eq.nu_ine AS equipe_ine_atendimento,
            eq.no_equipe AS equipe_nome_atendimento
           FROM public.tb_fat_atendimento_individual tfaiaborto
             JOIN public.tb_fat_atd_ind_problemas tfaipaborto ON tfaiaborto.co_seq_fat_atd_ind = tfaipaborto.co_fat_atd_ind
             JOIN public.tb_dim_tempo tdtempoaborto ON tdtempoaborto.co_seq_dim_tempo = tfaiaborto.co_dim_tempo
             LEFT JOIN public.tb_dim_profissional tdprof ON tdprof.co_seq_dim_profissional = tfaiaborto.co_dim_profissional_1
             LEFT JOIN public.tb_dim_equipe eq ON eq.co_seq_dim_equipe = tfaiaborto.co_dim_equipe_1
             LEFT JOIN public.tb_dim_unidade_saude uns ON uns.co_seq_dim_unidade_saude = tfaiaborto.co_dim_unidade_saude_1
             LEFT JOIN public.tb_dim_cid tdcidaborto ON tdcidaborto.co_seq_dim_cid = tfaipaborto.co_dim_cid
             LEFT JOIN public.tb_dim_ciap tdciapaborto ON tdciapaborto.co_seq_dim_ciap = tfaipaborto.co_dim_ciap
             LEFT JOIN public.tb_fat_cidadao_pec tfcp ON tfaiaborto.co_fat_cidadao_pec = tfcp.co_seq_fat_cidadao_pec
             JOIN gestantes_unicas gu ON gu.chave_gestante = (tfcp.no_cidadao::text || tfcp.co_dim_tempo_nascimento)
          WHERE ((tdciapaborto.nu_ciap::text = ANY (ARRAY['W82'::text, 'W83'::text])) OR (tdcidaborto.nu_cid::text = ANY (ARRAY['O02'::text, 'O03'::text, 'O05'::text, 'O06'::text, 'O04'::text, 'Z303'::text]))) AND tdtempoaborto.dt_registro >= gu.consulta_prenatal_primeira_data
        ), atendimento_odonto AS (
         SELECT DISTINCT tfaodont.co_seq_fat_atd_odnt::text AS id_registro,
            'atendimento_odontologico'::text AS tipo_registro,
            otdtempo.dt_registro AS data_registro,
            gu.chave_gestante,
            tfcp.no_cidadao AS gestante_nome,
            NULL::date AS gestante_data_de_nascimento,
            tfcp.nu_cpf_cidadao AS gestante_documento_cpf,
            tfcp.nu_cns AS gestante_documento_cns,
            tfcp.nu_telefone_celular AS gestante_telefone,
            NULL::date AS data_dum,
            NULL::integer AS idade_gestacional_atendimento,
            tdprof.nu_cns AS profissional_cns_atendimento,
            tdprof.no_profissional AS profissional_nome_atendimento,
            uns.nu_cnes AS estabelecimento_cnes_atendimento,
            uns.no_unidade_saude AS estabelecimento_nome_atendimento,
            eq.nu_ine AS equipe_ine_atendimento,
            eq.no_equipe AS equipe_nome_atendimento
           FROM public.tb_fat_atendimento_odonto tfaodont
             JOIN public.tb_fat_cidadao_pec tfcp ON tfcp.co_seq_fat_cidadao_pec = tfaodont.co_fat_cidadao_pec
             JOIN gestantes_unicas gu ON gu.chave_gestante = (tfcp.no_cidadao::text || tfcp.co_dim_tempo_nascimento)
             LEFT JOIN public.tb_dim_profissional tdprof ON tdprof.co_seq_dim_profissional = tfaodont.co_dim_profissional_1
             LEFT JOIN public.tb_dim_equipe eq ON eq.co_seq_dim_equipe = tfaodont.co_dim_equipe_1
             LEFT JOIN public.tb_dim_unidade_saude uns ON uns.co_seq_dim_unidade_saude = tfaodont.co_dim_unidade_saude_1
             JOIN public.tb_dim_cbo otdcbo ON otdcbo.co_seq_dim_cbo = tfaodont.co_dim_cbo_1
             JOIN public.tb_dim_tempo otdtempo ON otdtempo.co_seq_dim_tempo = tfaodont.co_dim_tempo
          WHERE otdcbo.nu_cbo::text ~~ '2232%'::text AND otdtempo.dt_registro >= (( SELECT
                        CASE
                            WHEN date_part('month'::text, CURRENT_DATE) >= 1::double precision AND date_part('month'::text, CURRENT_DATE) <= 4::double precision THEN concat(date_part('year'::text, (CURRENT_DATE - '365 days'::interval)::date), '-09-01')
                            WHEN date_part('month'::text, CURRENT_DATE) >= 5::double precision AND date_part('month'::text, CURRENT_DATE) <= 8::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-01-01')
                            WHEN date_part('month'::text, CURRENT_DATE) >= 9::double precision AND date_part('month'::text, CURRENT_DATE) <= 12::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-05-01')
                            ELSE NULL::text
                        END::date - '294 days'::interval))
        ), exame_hiv AS (
         SELECT DISTINCT tfpap.co_seq_fat_proced_atend_proced::text AS id_registro,
            'teste_rapido_exame_hiv'::text AS tipo_registro,
            tdtempo.dt_registro AS data_registro,
            gu.chave_gestante,
            tfcp.no_cidadao AS gestante_nome,
            NULL::date AS gestante_data_de_nascimento,
            tfcp.nu_cpf_cidadao AS gestante_documento_cpf,
            tfcp.nu_cns AS gestante_documento_cns,
            tfcp.nu_telefone_celular AS gestante_telefone,
            NULL::date AS data_dum,
            NULL::integer AS idade_gestacional_atendimento,
            tdprof.nu_cns AS profissional_cns_atendimento,
            tdprof.no_profissional AS profissional_nome_atendimento,
            uns.nu_cnes AS estabelecimento_cnes_atendimento,
            uns.no_unidade_saude AS estabelecimento_nome_atendimento,
            eq.nu_ine AS equipe_ine_atendimento,
            eq.no_equipe AS equipe_nome_atendimento
           FROM public.tb_fat_proced_atend_proced tfpap
             JOIN public.tb_fat_cidadao_pec tfcp ON tfcp.co_seq_fat_cidadao_pec = tfpap.co_fat_cidadao_pec
             JOIN gestantes_unicas gu ON gu.chave_gestante = (tfcp.no_cidadao::text || tfcp.co_dim_tempo_nascimento)
             JOIN public.tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfpap.co_dim_procedimento
             LEFT JOIN public.tb_dim_profissional tdprof ON tdprof.co_seq_dim_profissional = tfpap.co_dim_profissional
             LEFT JOIN public.tb_dim_equipe eq ON eq.co_seq_dim_equipe = tfpap.co_dim_equipe
             LEFT JOIN public.tb_dim_unidade_saude uns ON uns.co_seq_dim_unidade_saude = tfpap.co_dim_unidade_saude
             JOIN public.tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfpap.co_dim_cbo
             JOIN public.tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfpap.co_dim_tempo
          WHERE (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND (tdp.co_proced::text = ANY (ARRAY['0214010058'::text, '0214010040'::text, 'ABPG024'::text])) AND tdtempo.dt_registro >= (( SELECT
                        CASE
                            WHEN date_part('month'::text, CURRENT_DATE) >= 1::double precision AND date_part('month'::text, CURRENT_DATE) <= 4::double precision THEN concat(date_part('year'::text, (CURRENT_DATE - '365 days'::interval)::date), '-09-01')
                            WHEN date_part('month'::text, CURRENT_DATE) >= 5::double precision AND date_part('month'::text, CURRENT_DATE) <= 8::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-01-01')
                            WHEN date_part('month'::text, CURRENT_DATE) >= 9::double precision AND date_part('month'::text, CURRENT_DATE) <= 12::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-05-01')
                            ELSE NULL::text
                        END::date - '294 days'::interval))
        UNION ALL
         SELECT DISTINCT tfaip.co_seq_fat_atend_ind_proced::text AS id_registro,
            'exame_hiv_avaliado'::text AS tipo_registro,
            tdtempo.dt_registro AS data_registro,
            gu.chave_gestante,
            tfcp.no_cidadao AS gestante_nome,
            NULL::date AS gestante_data_de_nascimento,
            tfcp.nu_cpf_cidadao AS gestante_documento_cpf,
            tfcp.nu_cns AS gestante_documento_cns,
            tfcp.nu_telefone_celular AS gestante_telefone,
            NULL::date AS data_dum,
            NULL::integer AS idade_gestacional_atendimento,
            tdprof.nu_cns AS profissional_cns_atendimento,
            tdprof.no_profissional AS profissional_nome_atendimento,
            uns.nu_cnes AS estabelecimento_cnes_atendimento,
            uns.no_unidade_saude AS estabelecimento_nome_atendimento,
            eq.nu_ine AS equipe_ine_atendimento,
            eq.no_equipe AS equipe_nome_atendimento
           FROM public.tb_fat_atd_ind_procedimentos tfaip
             JOIN public.tb_fat_cidadao_pec tfcp ON tfcp.co_seq_fat_cidadao_pec = tfaip.co_fat_cidadao_pec
             JOIN gestantes_unicas gu ON gu.chave_gestante = (tfcp.no_cidadao::text || tfcp.co_dim_tempo_nascimento)
             JOIN public.tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfaip.co_dim_procedimento_avaliado
             LEFT JOIN public.tb_dim_profissional tdprof ON tdprof.co_seq_dim_profissional = tfaip.co_dim_profissional_1
             LEFT JOIN public.tb_dim_equipe eq ON eq.co_seq_dim_equipe = tfaip.co_dim_equipe_1
             LEFT JOIN public.tb_dim_unidade_saude uns ON uns.co_seq_dim_unidade_saude = tfaip.co_dim_unidade_saude_1
             JOIN public.tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfaip.co_dim_cbo_1
             JOIN public.tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfaip.co_dim_tempo
          WHERE (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND (tdp.co_proced::text = ANY (ARRAY['0202030300'::text, 'ABEX018'::text])) AND tdtempo.dt_registro >= (( SELECT
                        CASE
                            WHEN date_part('month'::text, CURRENT_DATE) >= 1::double precision AND date_part('month'::text, CURRENT_DATE) <= 4::double precision THEN concat(date_part('year'::text, (CURRENT_DATE - '365 days'::interval)::date), '-09-01')
                            WHEN date_part('month'::text, CURRENT_DATE) >= 5::double precision AND date_part('month'::text, CURRENT_DATE) <= 8::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-01-01')
                            WHEN date_part('month'::text, CURRENT_DATE) >= 9::double precision AND date_part('month'::text, CURRENT_DATE) <= 12::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-05-01')
                            ELSE NULL::text
                        END::date - '294 days'::interval))
        ), exame_sifilis AS (
         SELECT DISTINCT tfpap.co_seq_fat_proced_atend_proced::text AS id_registro,
            'teste_rapido_exame_sifilis'::text AS tipo_registro,
            tdtempo.dt_registro AS data_registro,
            gu.chave_gestante,
            tfcp.no_cidadao AS gestante_nome,
            NULL::date AS gestante_data_de_nascimento,
            tfcp.nu_cpf_cidadao AS gestante_documento_cpf,
            tfcp.nu_cns AS gestante_documento_cns,
            tfcp.nu_telefone_celular AS gestante_telefone,
            NULL::date AS data_dum,
            NULL::integer AS idade_gestacional_atendimento,
            tdprof.nu_cns AS profissional_cns_atendimento,
            tdprof.no_profissional AS profissional_nome_atendimento,
            uns.nu_cnes AS estabelecimento_cnes_atendimento,
            uns.no_unidade_saude AS estabelecimento_nome_atendimento,
            eq.nu_ine AS equipe_ine_atendimento,
            eq.no_equipe AS equipe_nome_atendimento
           FROM public.tb_fat_proced_atend_proced tfpap
             JOIN public.tb_fat_cidadao_pec tfcp ON tfcp.co_seq_fat_cidadao_pec = tfpap.co_fat_cidadao_pec
             JOIN gestantes_unicas gu ON gu.chave_gestante = (tfcp.no_cidadao::text || tfcp.co_dim_tempo_nascimento)
             JOIN public.tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfpap.co_dim_procedimento
             LEFT JOIN public.tb_dim_profissional tdprof ON tdprof.co_seq_dim_profissional = tfpap.co_dim_profissional
             LEFT JOIN public.tb_dim_equipe eq ON eq.co_seq_dim_equipe = tfpap.co_dim_equipe
             LEFT JOIN public.tb_dim_unidade_saude uns ON uns.co_seq_dim_unidade_saude = tfpap.co_dim_unidade_saude
             JOIN public.tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfpap.co_dim_cbo
             JOIN public.tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfpap.co_dim_tempo
          WHERE (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND (tdp.co_proced::text = ANY (ARRAY['0214010074'::text, '0214010082'::text, 'ABPG026'::text])) AND tdtempo.dt_registro >= (( SELECT
                        CASE
                            WHEN date_part('month'::text, CURRENT_DATE) >= 1::double precision AND date_part('month'::text, CURRENT_DATE) <= 4::double precision THEN concat(date_part('year'::text, (CURRENT_DATE - '365 days'::interval)::date), '-09-01')
                            WHEN date_part('month'::text, CURRENT_DATE) >= 5::double precision AND date_part('month'::text, CURRENT_DATE) <= 8::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-01-01')
                            WHEN date_part('month'::text, CURRENT_DATE) >= 9::double precision AND date_part('month'::text, CURRENT_DATE) <= 12::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-05-01')
                            ELSE NULL::text
                        END::date - '294 days'::interval))
        UNION ALL
         SELECT DISTINCT tfaip.co_seq_fat_atend_ind_proced::text AS id_registro,
            'exame_sifilis_avaliado'::text AS tipo_registro,
            tdtempo.dt_registro AS data_registro,
            gu.chave_gestante,
            tfcp.no_cidadao AS gestante_nome,
            NULL::date AS gestante_data_de_nascimento,
            tfcp.nu_cpf_cidadao AS gestante_documento_cpf,
            tfcp.nu_cns AS gestante_documento_cns,
            tfcp.nu_telefone_celular AS gestante_telefone,
            NULL::date AS data_dum,
            NULL::integer AS idade_gestacional_atendimento,
            tdprof.nu_cns AS profissional_cns_atendimento,
            tdprof.no_profissional AS profissional_nome_atendimento,
            uns.nu_cnes AS estabelecimento_cnes_atendimento,
            uns.no_unidade_saude AS estabelecimento_nome_atendimento,
            eq.nu_ine AS equipe_ine_atendimento,
            eq.no_equipe AS equipe_nome_atendimento
           FROM public.tb_fat_atd_ind_procedimentos tfaip
             JOIN public.tb_fat_cidadao_pec tfcp ON tfcp.co_seq_fat_cidadao_pec = tfaip.co_fat_cidadao_pec
             JOIN gestantes_unicas gu ON gu.chave_gestante = (tfcp.no_cidadao::text || tfcp.co_dim_tempo_nascimento)
             JOIN public.tb_dim_procedimento tdp ON tdp.co_seq_dim_procedimento = tfaip.co_dim_procedimento_avaliado
             LEFT JOIN public.tb_dim_profissional tdprof ON tdprof.co_seq_dim_profissional = tfaip.co_dim_profissional_1
             LEFT JOIN public.tb_dim_equipe eq ON eq.co_seq_dim_equipe = tfaip.co_dim_equipe_1
             LEFT JOIN public.tb_dim_unidade_saude uns ON uns.co_seq_dim_unidade_saude = tfaip.co_dim_unidade_saude_1
             JOIN public.tb_dim_cbo tdcbo ON tdcbo.co_seq_dim_cbo = tfaip.co_dim_cbo_1
             JOIN public.tb_dim_tempo tdtempo ON tdtempo.co_seq_dim_tempo = tfaip.co_dim_tempo
          WHERE (tdcbo.nu_cbo::text ~~ ANY (ARRAY['2251%'::text, '2252%'::text, '2253%'::text, '2231%'::text, '2235%'::text, '3222%'::text])) AND (tdp.co_proced::text = ANY (ARRAY['0202031110'::text, '0202031179'::text, 'ABEX019'::text])) AND tdtempo.dt_registro >= (( SELECT
                        CASE
                            WHEN date_part('month'::text, CURRENT_DATE) >= 1::double precision AND date_part('month'::text, CURRENT_DATE) <= 4::double precision THEN concat(date_part('year'::text, (CURRENT_DATE - '365 days'::interval)::date), '-09-01')
                            WHEN date_part('month'::text, CURRENT_DATE) >= 5::double precision AND date_part('month'::text, CURRENT_DATE) <= 8::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-01-01')
                            WHEN date_part('month'::text, CURRENT_DATE) >= 9::double precision AND date_part('month'::text, CURRENT_DATE) <= 12::double precision THEN concat(date_part('year'::text, CURRENT_DATE), '-05-01')
                            ELSE NULL::text
                        END::date - '294 days'::interval))
        ), uniao_registros AS (
         SELECT atendimentos_pre_natal.id_registro,
            atendimentos_pre_natal.tipo_registro,
            atendimentos_pre_natal.data_registro,
            atendimentos_pre_natal.chave_gestante,
            atendimentos_pre_natal.gestante_nome,
            atendimentos_pre_natal.gestante_data_de_nascimento,
            atendimentos_pre_natal.gestante_documento_cpf,
            atendimentos_pre_natal.gestante_documento_cns,
            atendimentos_pre_natal.gestante_telefone,
            atendimentos_pre_natal.data_dum,
            atendimentos_pre_natal.idade_gestacional_atendimento,
            atendimentos_pre_natal.profissional_cns_atendimento,
            atendimentos_pre_natal.profissional_nome_atendimento,
            atendimentos_pre_natal.estabelecimento_cnes_atendimento,
            atendimentos_pre_natal.estabelecimento_nome_atendimento,
            atendimentos_pre_natal.equipe_ine_atendimento,
            atendimentos_pre_natal.equipe_nome_atendimento
           FROM atendimentos_pre_natal
        UNION ALL
         SELECT registros_parto.id_registro,
            registros_parto.tipo_registro,
            registros_parto.data_registro,
            registros_parto.chave_gestante,
            registros_parto.gestante_nome,
            registros_parto.gestante_data_de_nascimento,
            registros_parto.gestante_documento_cpf,
            registros_parto.gestante_documento_cns,
            registros_parto.gestante_telefone,
            registros_parto.data_dum,
            registros_parto.idade_gestacional_atendimento,
            registros_parto.profissional_cns_atendimento,
            registros_parto.profissional_nome_atendimento,
            registros_parto.estabelecimento_cnes_atendimento,
            registros_parto.estabelecimento_nome_atendimento,
            registros_parto.equipe_ine_atendimento,
            registros_parto.equipe_nome_atendimento
           FROM registros_parto
        UNION ALL
         SELECT registros_aborto.id_registro,
            registros_aborto.tipo_registro,
            registros_aborto.data_registro,
            registros_aborto.chave_gestante,
            registros_aborto.gestante_nome,
            registros_aborto.gestante_data_de_nascimento,
            registros_aborto.gestante_documento_cpf,
            registros_aborto.gestante_documento_cns,
            registros_aborto.gestante_telefone,
            registros_aborto.data_dum,
            registros_aborto.idade_gestacional_atendimento,
            registros_aborto.profissional_cns_atendimento,
            registros_aborto.profissional_nome_atendimento,
            registros_aborto.estabelecimento_cnes_atendimento,
            registros_aborto.estabelecimento_nome_atendimento,
            registros_aborto.equipe_ine_atendimento,
            registros_aborto.equipe_nome_atendimento
           FROM registros_aborto
        UNION ALL
         SELECT atendimento_odonto.id_registro,
            atendimento_odonto.tipo_registro,
            atendimento_odonto.data_registro,
            atendimento_odonto.chave_gestante,
            atendimento_odonto.gestante_nome,
            atendimento_odonto.gestante_data_de_nascimento,
            atendimento_odonto.gestante_documento_cpf,
            atendimento_odonto.gestante_documento_cns,
            atendimento_odonto.gestante_telefone,
            atendimento_odonto.data_dum,
            atendimento_odonto.idade_gestacional_atendimento,
            atendimento_odonto.profissional_cns_atendimento,
            atendimento_odonto.profissional_nome_atendimento,
            atendimento_odonto.estabelecimento_cnes_atendimento,
            atendimento_odonto.estabelecimento_nome_atendimento,
            atendimento_odonto.equipe_ine_atendimento,
            atendimento_odonto.equipe_nome_atendimento
           FROM atendimento_odonto
        UNION ALL
         SELECT exame_sifilis.id_registro,
            exame_sifilis.tipo_registro,
            exame_sifilis.data_registro,
            exame_sifilis.chave_gestante,
            exame_sifilis.gestante_nome,
            exame_sifilis.gestante_data_de_nascimento,
            exame_sifilis.gestante_documento_cpf,
            exame_sifilis.gestante_documento_cns,
            exame_sifilis.gestante_telefone,
            exame_sifilis.data_dum,
            exame_sifilis.idade_gestacional_atendimento,
            exame_sifilis.profissional_cns_atendimento,
            exame_sifilis.profissional_nome_atendimento,
            exame_sifilis.estabelecimento_cnes_atendimento,
            exame_sifilis.estabelecimento_nome_atendimento,
            exame_sifilis.equipe_ine_atendimento,
            exame_sifilis.equipe_nome_atendimento
           FROM exame_sifilis
        UNION ALL
         SELECT exame_hiv.id_registro,
            exame_hiv.tipo_registro,
            exame_hiv.data_registro,
            exame_hiv.chave_gestante,
            exame_hiv.gestante_nome,
            exame_hiv.gestante_data_de_nascimento,
            exame_hiv.gestante_documento_cpf,
            exame_hiv.gestante_documento_cns,
            exame_hiv.gestante_telefone,
            exame_hiv.data_dum,
            exame_hiv.idade_gestacional_atendimento,
            exame_hiv.profissional_cns_atendimento,
            exame_hiv.profissional_nome_atendimento,
            exame_hiv.estabelecimento_cnes_atendimento,
            exame_hiv.estabelecimento_nome_atendimento,
            exame_hiv.equipe_ine_atendimento,
            exame_hiv.equipe_nome_atendimento
           FROM exame_hiv
        ), cadastro_individual_recente AS (
         WITH base AS (
                 SELECT gu.chave_gestante,
                    tdt.dt_registro AS data_cadastro_individual,
                    tfci.nu_micro_area AS micro_area_cad_individual,
                    uns.nu_cnes AS estabelecimento_cnes_cad_indivual,
                    uns.no_unidade_saude AS estabelecimento_nome_cad_individual,
                    eq.nu_ine AS equipe_ine_cad_individual,
                    eq.no_equipe AS equipe_nome_cad_individual,
                    acs.no_profissional AS acs_cad_individual,
                    row_number() OVER (PARTITION BY gu.chave_gestante ORDER BY tdt.dt_registro DESC) = 1 AS ultimo_cadastro_individual
                   FROM public.tb_fat_cad_individual tfci
                     JOIN public.tb_fat_cidadao_pec tfcpec ON tfcpec.co_seq_fat_cidadao_pec = tfci.co_fat_cidadao_pec
                     JOIN gestantes_unicas gu ON gu.chave_gestante = (tfcpec.no_cidadao::text || tfcpec.co_dim_tempo_nascimento)
                     LEFT JOIN public.tb_dim_tempo tdt ON tdt.co_seq_dim_tempo = tfci.co_dim_tempo
                     LEFT JOIN public.tb_dim_equipe eq ON eq.co_seq_dim_equipe = tfci.co_dim_equipe
                     LEFT JOIN public.tb_dim_profissional acs ON acs.co_seq_dim_profissional = tfci.co_dim_profissional
                     LEFT JOIN public.tb_dim_unidade_saude uns ON uns.co_seq_dim_unidade_saude = tfci.co_dim_unidade_saude
                     where eq.nu_ine not in ('0000071722', '0000071730', '0001511912', '0001846892', '0001847236', '0002275872')  
                )
         SELECT base.chave_gestante,
            base.data_cadastro_individual,
            base.micro_area_cad_individual,
            base.estabelecimento_cnes_cad_indivual,
            base.estabelecimento_nome_cad_individual,
            base.equipe_ine_cad_individual,
            base.equipe_nome_cad_individual,
            base.acs_cad_individual,
            base.ultimo_cadastro_individual
           FROM base
          WHERE base.ultimo_cadastro_individual IS TRUE
        ), visita_domiciliar_recente AS (
         WITH base AS (
                 SELECT gu.chave_gestante,
                    tfcpec.co_seq_fat_cidadao_pec,
                    tdt.dt_registro AS data_visita_acs,
                    acs.no_profissional AS acs_visita_domiciliar,
                    row_number() OVER (PARTITION BY gu.chave_gestante ORDER BY tdt.dt_registro DESC) = 1 AS ultima_visita_domiciliar
                   FROM public.tb_fat_visita_domiciliar visitadomiciliar
                     JOIN public.tb_fat_cidadao_pec tfcpec ON tfcpec.co_seq_fat_cidadao_pec = visitadomiciliar.co_fat_cidadao_pec
                     JOIN gestantes_unicas gu ON gu.chave_gestante = (tfcpec.no_cidadao::text || tfcpec.co_dim_tempo_nascimento)
                     LEFT JOIN public.tb_dim_profissional acs ON acs.co_seq_dim_profissional = visitadomiciliar.co_dim_profissional
                     LEFT JOIN public.tb_dim_tempo tdt ON tdt.co_seq_dim_tempo = visitadomiciliar.co_dim_tempo
                )
         SELECT base.chave_gestante,
            base.co_seq_fat_cidadao_pec,
            base.data_visita_acs,
            base.acs_visita_domiciliar,
            base.ultima_visita_domiciliar
           FROM base
          WHERE base.ultima_visita_domiciliar IS TRUE
        ), cadastro_domiciliar_recente AS (
         WITH base AS (
                 SELECT gu.chave_gestante,
                    tdt.dt_registro AS data_cadastro_dom_familia,
                    caddomiciliarfamilia.nu_micro_area AS micro_area_domicilio,
                    uns.nu_cnes AS cnes_estabelecimento_cad_dom_familia,
                    uns.no_unidade_saude AS estabelecimento_cad_dom_familia,
                    eq.nu_ine AS ine_equipe_cad_dom_familia,
                    eq.no_equipe AS equipe_cad_dom_familia,
                    acs.no_profissional AS acs_cad_dom_familia,
                    NULLIF(concat(cadomiciliar.no_logradouro, ', ', cadomiciliar.nu_num_logradouro), ', '::text) AS gestante_endereco,
                    row_number() OVER (PARTITION BY gu.chave_gestante ORDER BY tdt.dt_registro DESC) = 1 AS ultimo_cadastro_domiciliar_familia
                   FROM public.tb_fat_cad_dom_familia caddomiciliarfamilia
                     JOIN public.tb_fat_cad_domiciliar cadomiciliar ON cadomiciliar.co_seq_fat_cad_domiciliar = caddomiciliarfamilia.co_fat_cad_domiciliar
                     JOIN public.tb_fat_cidadao_pec tfcpec ON tfcpec.co_seq_fat_cidadao_pec = caddomiciliarfamilia.co_fat_cidadao_pec
                     JOIN gestantes_unicas gu ON gu.chave_gestante = (tfcpec.no_cidadao::text || tfcpec.co_dim_tempo_nascimento)
                     LEFT JOIN public.tb_dim_tempo tdt ON tdt.co_seq_dim_tempo = caddomiciliarfamilia.co_dim_tempo
                     LEFT JOIN public.tb_dim_equipe eq ON eq.co_seq_dim_equipe = caddomiciliarfamilia.co_dim_equipe
                     LEFT JOIN public.tb_dim_profissional acs ON acs.co_seq_dim_profissional = caddomiciliarfamilia.co_dim_profissional
                     LEFT JOIN public.tb_dim_unidade_saude uns ON uns.co_seq_dim_unidade_saude = caddomiciliarfamilia.co_dim_unidade_saude
                )
         SELECT base.chave_gestante,
            base.data_cadastro_dom_familia,
            base.micro_area_domicilio,
            base.cnes_estabelecimento_cad_dom_familia,
            base.estabelecimento_cad_dom_familia,
            base.ine_equipe_cad_dom_familia,
            base.equipe_cad_dom_familia,
            base.acs_cad_dom_familia,
            base.gestante_endereco,
            base.ultimo_cadastro_domiciliar_familia
           FROM base
          WHERE base.ultimo_cadastro_domiciliar_familia IS TRUE
        )
 SELECT b.id_registro,
    b.tipo_registro,
    b.data_registro,
    b.chave_gestante,
    b.gestante_nome,
    b.gestante_data_de_nascimento,
    b.gestante_documento_cpf,
    b.gestante_documento_cns,
    b.gestante_telefone,
    b.data_dum,
    b.idade_gestacional_atendimento,
    b.profissional_cns_atendimento,
    b.profissional_nome_atendimento,
    b.estabelecimento_cnes_atendimento,
    b.estabelecimento_nome_atendimento,
    b.equipe_ine_atendimento,
    b.equipe_nome_atendimento,
    cir.data_cadastro_individual AS data_ultimo_cadastro_individual,
    cir.estabelecimento_cnes_cad_indivual,
    cir.estabelecimento_nome_cad_individual,
    cir.equipe_ine_cad_individual,
    cir.equipe_nome_cad_individual,
    vdr.data_visita_acs AS data_ultima_visita_acs,
    vdr.acs_visita_domiciliar,
    cdr.acs_cad_dom_familia,
    cir.acs_cad_individual,
    current_date as criacao_data,
	current_date as atualizacao_data
   FROM uniao_registros b
     LEFT JOIN cadastro_individual_recente cir ON cir.chave_gestante = b.chave_gestante
     LEFT JOIN visita_domiciliar_recente vdr ON vdr.chave_gestante = b.chave_gestante
     LEFT JOIN cadastro_domiciliar_recente cdr ON cdr.chave_gestante = b.chave_gestante
