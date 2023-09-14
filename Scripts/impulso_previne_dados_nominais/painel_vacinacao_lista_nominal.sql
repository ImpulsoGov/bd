with sumarizacao_criancas as (
select 
	h.municipio_id_sus,
	h.chave_cidadao,
	h.cidadao_cpf,
	h.cidadao_cns,
	h.cidadao_nome,
	h.dt_nascimento,
	h.cidadao_nome_responsavel,
	h.cidadao_cns_responsavel,
	h.cidadao_cpf_responsavel,
	h.cidadao_idade_meses_atual,
	h.estabelecimento_cnes_atendimento,
	h.estabelecimento_cnes_cadastro,
	h.estabelecimento_nome_atendimento,
	h.estabelecimento_nome_cadastro,
	h.equipe_ine_atendimento,
	h.equipe_ine_cadastro,
	h.equipe_nome_atendimento,
	h.equipe_nome_cadastro,
	h.acs_nome_cadastro,
	h.acs_nome_visita,
	h.data_ultimo_cadastro_individual,
	h.data_ultimo_atendimento_individual,
	h.data_ultima_vista_domiciliar,
case 
	WHEN date_part('month', date(h.dt_nascimento  + interval '1 year')) >= 1 AND date_part('month', date(h.dt_nascimento  + interval '1 year'))<= 4 THEN concat(date_part('year', date(h.dt_nascimento  + interval '1 year')::date), '-01-01')::date
    WHEN date_part('month', date(h.dt_nascimento  + interval '1 year')) >= 5 AND date_part('month', date(h.dt_nascimento + interval '1 year')) <= 8 THEN concat(date_part('year', date(h.dt_nascimento + interval '1 year')::date), '-05-01')::date
    WHEN date_part('month', date(h.dt_nascimento + interval '1 year')) >= 9 AND date_part('month', date(h.dt_nascimento  + interval '1 year')) <= 12 THEN concat(date_part('year', date(h.dt_nascimento + interval '1 year')::date), '-09-01')::date
	ELSE NULL
END AS inicio_quadri_completa_1_ano,
case 
	when h.cidadao_idade_meses_atual < 2 then 'vacinacao_nao_iniciada'
	when h.cidadao_idade_meses_atual between 2 and 12 then 'vacinacao_em_andamento'
	when h.cidadao_idade_meses_atual > 12  then 'periodo_vacinacao_encerrado'
end as status_idade
from dados_nominais_am_labrea.lista_nominal_vacinacao h
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23
), 
quantidade_vacinas_polio_registradas as ( 
	with base as ( 
	select
		st.chave_cidadao,
	    CASE 
		   WHEN codigo_vacina = '22' THEN 1  
		   ELSE 0
		end as cont_vacina
	    FROM dados_nominais_am_labrea.lista_nominal_vacinacao st
		) select  
		b.chave_cidadao,
		sum(b.cont_vacina) over (partition by b.chave_cidadao) as quantidade_polio_registradas	
		from base b
),
primeira_dose_polio as (
with base as (
	select
		st.chave_cidadao,
		st.dt_nascimento,
		st.data_registro_vacina as data_1dose_polio,
		st.co_seq_fat_vacinacao_vacina,
		st.dose_vacina,
		row_number() over (partition by st.chave_cidadao order by st.data_registro_vacina, st.co_seq_fat_vacinacao_vacina asc) as ordem_aplicacao 
		FROM dados_nominais_am_labrea.lista_nominal_vacinacao st
		where  st.dose_vacina = '1ª DOSE' and st.codigo_vacina = '22'
	) select * from base 
		where ordem_aplicacao = 1
), segunda_dose_polio as (
with base as (
	select
		st.chave_cidadao,
		st.dt_nascimento,
		st.data_registro_vacina as data_2dose_polio,
		st.co_seq_fat_vacinacao,
		st.dose_vacina,
		row_number() over (partition by st.chave_cidadao order by st.data_registro_vacina, st.co_seq_fat_vacinacao asc) as ordem_aplicacao
		FROM dados_nominais_am_labrea.lista_nominal_vacinacao st
		where  st.dose_vacina = '2ª DOSE' and st.codigo_vacina = '22'
	) select * from base 
		where ordem_aplicacao = 1
), terceira_dose_polio as (
with base as (
	select
		st.chave_cidadao,
		st.dt_nascimento,
		st.data_registro_vacina as data_3dose_polio,
		st.co_seq_fat_vacinacao,
		st.dose_vacina,
		row_number() over (partition by st.chave_cidadao order by st.data_registro_vacina, st.co_seq_fat_vacinacao asc) as ordem_aplicacao
		FROM dados_nominais_am_labrea.lista_nominal_vacinacao st
		where  st.dose_vacina = '3ª DOSE' and st.codigo_vacina = '22'
	) select * from base 
		where ordem_aplicacao = 1
), aplicacoes_polio as(
select 
	polio1.chave_cidadao,
	polio1.data_1dose_polio,
	polio2.data_2dose_polio,
	polio3.data_3dose_polio,
	age(polio1.data_1dose_polio::timestamp WITH time zone, polio1.dt_nascimento::timestamp WITH time zone) as idade_1dose_polio,
	age(polio2.data_2dose_polio::timestamp WITH time zone, polio2.dt_nascimento::timestamp WITH time zone) as idade_2dose_polio,
	age(polio3.data_3dose_polio::timestamp WITH time zone, polio3.dt_nascimento::timestamp WITH time zone) as idade_3dose_polio
	from primeira_dose_polio polio1
	left join segunda_dose_polio polio2 on polio1.chave_cidadao = polio2.chave_cidadao
	left join terceira_dose_polio polio3 on polio1.chave_cidadao = polio3.chave_cidadao
), sumarizacao_polio as (
select h.chave_cidadao,
	h.dt_nascimento,
	ap.data_1dose_polio,
	ap.data_2dose_polio,
	ap.data_3dose_polio,
	q.quantidade_polio_registradas,
	((case when ap.data_1dose_polio is not null then 1 else 0 end) + (case when ap.data_2dose_polio is not null then 1 else 0 end) + (case when ap.data_3dose_polio is not null then 1 else 0 end)) as quantidade_polio_validas,
	idade_1dose_polio,
	idade_2dose_polio,
	idade_3dose_polio,
	date(h.dt_nascimento + interval '2 months') as prazo_1dose_polio,
	date(h.dt_nascimento + interval '8 months') as prazo_limite_1dose_polio,
	date(ap.data_1dose_polio + interval '2 months') as prazo_2dose_polio,
	date(ap.data_2dose_polio+ interval '2 months') as prazo_3dose_polio
	from dados_nominais_am_labrea.lista_nominal_vacinacao h
	left join aplicacoes_polio ap on h.chave_cidadao = ap.chave_cidadao
	left join quantidade_vacinas_polio_registradas q on q.chave_cidadao = h.chave_cidadao
	group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),
quantidade_vacinas_penta_registradas as ( 
	with base as ( 
	select
		st.chave_cidadao,
	    CASE 
		   WHEN codigo_vacina = '42' THEN 1  
		   ELSE 0
		end as cont_vacina
	    FROM dados_nominais_am_labrea.lista_nominal_vacinacao st
		) select  
		b.chave_cidadao,
		sum(b.cont_vacina) over (partition by b.chave_cidadao) as quantidade_penta_registradas	
		from base b
),
primeira_dose_penta as (
with base as (
	select
		st.chave_cidadao,
		st.dt_nascimento,
		st.data_registro_vacina as data_1dose_penta,
		st.co_seq_fat_vacinacao_vacina,
		st.dose_vacina,
		row_number() over (partition by st.chave_cidadao order by st.data_registro_vacina, st.co_seq_fat_vacinacao_vacina asc) as ordem_aplicacao 
		FROM dados_nominais_am_labrea.lista_nominal_vacinacao st
		where  st.dose_vacina = '1ª DOSE' and st.codigo_vacina = '42'
	) select * from base 
		where ordem_aplicacao = 1
), segunda_dose_penta as (
with base as (
	select
		st.chave_cidadao,
		st.dt_nascimento,
		st.data_registro_vacina as data_2dose_penta,
		st.co_seq_fat_vacinacao,
		st.dose_vacina,
		row_number() over (partition by st.chave_cidadao order by st.data_registro_vacina, st.co_seq_fat_vacinacao asc) as ordem_aplicacao
		FROM dados_nominais_am_labrea.lista_nominal_vacinacao st
		where  st.dose_vacina = '2ª DOSE' and st.codigo_vacina = '42'
	) select * from base 
		where ordem_aplicacao = 1
), terceira_dose_penta as (
with base as (
	select
		st.chave_cidadao,
		st.dt_nascimento,
		st.data_registro_vacina as data_3dose_penta,
		st.co_seq_fat_vacinacao,
		st.dose_vacina,
		row_number() over (partition by st.chave_cidadao order by st.data_registro_vacina, st.co_seq_fat_vacinacao asc) as ordem_aplicacao
		FROM dados_nominais_am_labrea.lista_nominal_vacinacao st
		where  st.dose_vacina = '3ª DOSE' and st.codigo_vacina = '42'
	) select * from base 
		where ordem_aplicacao = 1
), aplicacoes_penta as(
select 
	penta1.chave_cidadao,
	penta1.data_1dose_penta,
	penta2.data_2dose_penta,
	penta3.data_3dose_penta,
	age(penta1.data_1dose_penta::timestamp WITH time zone, penta1.dt_nascimento::timestamp WITH time zone) as idade_1dose_penta,
	age(penta2.data_2dose_penta::timestamp WITH time zone, penta2.dt_nascimento::timestamp WITH time zone) as idade_2dose_penta,
	age(penta3.data_3dose_penta::timestamp WITH time zone, penta3.dt_nascimento::timestamp WITH time zone) as idade_3dose_penta
	from primeira_dose_penta penta1
	left join segunda_dose_penta penta2 on penta1.chave_cidadao = penta2.chave_cidadao
	left join terceira_dose_penta penta3 on penta1.chave_cidadao = penta3.chave_cidadao
), sumarizacao_penta as (
select h.chave_cidadao,
	h.dt_nascimento,
	ap.data_1dose_penta,
	ap.data_2dose_penta,
	ap.data_3dose_penta,
	q.quantidade_penta_registradas,
	((case when ap.data_1dose_penta is not null then 1 else 0 end) + (case when ap.data_2dose_penta is not null then 1 else 0 end) + (case when ap.data_3dose_penta is not null then 1 else 0 end)) as quantidade_penta_validas,
	idade_1dose_penta,
	idade_2dose_penta,
	idade_3dose_penta,
	date(h.dt_nascimento + interval '2 months') as prazo_1dose_penta,
	date(h.dt_nascimento + interval '8 months') as prazo_limite_1dose_penta,
	date(ap.data_1dose_penta + interval '2 months') as prazo_2dose_penta,
	date(ap.data_2dose_penta+ interval '2 months') as prazo_3dose_penta
	from dados_nominais_am_labrea.lista_nominal_vacinacao h
	left join aplicacoes_penta ap on h.chave_cidadao = ap.chave_cidadao
	left join quantidade_vacinas_penta_registradas q on q.chave_cidadao = h.chave_cidadao
	group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
), selecao_final as (
select 
h.municipio_id_sus,
h.chave_cidadao,
h.cidadao_nome,
h.cidadao_cpf,
h.cidadao_cns,
h.dt_nascimento,
h.cidadao_idade_meses_atual,
h.status_idade,
p.codigo as quadrimestre_completa_1_ano,
polio.data_1dose_polio,
polio.data_2dose_polio,
polio.data_3dose_polio,
polio.quantidade_polio_registradas,
polio.quantidade_polio_validas,
polio.idade_1dose_polio,
polio.idade_2dose_polio,
polio.idade_3dose_polio,
polio.prazo_1dose_polio,
polio.prazo_limite_1dose_polio,
polio.prazo_2dose_polio,
polio.prazo_3dose_polio,
penta.data_1dose_penta,
penta.data_2dose_penta,
penta.data_3dose_penta,
penta.quantidade_penta_registradas,
penta.quantidade_penta_validas,
penta.idade_1dose_penta,
penta.idade_2dose_penta,
penta.idade_3dose_penta,
penta.prazo_1dose_penta,
penta.prazo_limite_1dose_penta,
penta.prazo_2dose_penta,
penta.prazo_3dose_penta,
h.cidadao_nome_responsavel,
h.cidadao_cns_responsavel,
h.cidadao_cpf_responsavel,
h.estabelecimento_cnes_atendimento,
h.estabelecimento_cnes_cadastro,
h.estabelecimento_nome_atendimento,
h.estabelecimento_nome_cadastro,
h.equipe_ine_atendimento,
h.equipe_ine_cadastro,
h.equipe_nome_atendimento,
h.equipe_nome_cadastro,
h.acs_nome_cadastro,
h.acs_nome_visita,
h.data_ultimo_cadastro_individual,
h.data_ultimo_atendimento_individual,
h.data_ultima_vista_domiciliar
from sumarizacao_criancas h
left join sumarizacao_polio polio on polio.chave_cidadao = h.chave_cidadao
left join sumarizacao_penta penta on penta.chave_cidadao = h.chave_cidadao
left join listas_de_codigos.periodos p on p.data_inicio = h.inicio_quadri_completa_1_ano
where p.tipo = 'Quadrimestral'
) select * from selecao_final
