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
SELECT
        st.chave_cidadao,
        COUNT(DISTINCT st.co_seq_fat_vacinacao_vacina) AS qtde_vacinas_polio_registradas
  FROM dados_nominais_am_labrea.lista_nominal_vacinacao st
  WHERE codigo_vacina = '22'
  GROUP BY 1
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
),
sumarizacao_polio as (
select h.chave_cidadao,
	h.dt_nascimento,
	polio1.data_1dose_polio,
	polio2.data_2dose_polio,
	polio3.data_3dose_polio,
	q.qtde_vacinas_polio_registradas,
	((case when polio1.data_1dose_polio is not null then 1 else 0 end) + (case when polio2.data_2dose_polio is not null then 1 else 0 end) + (case when polio3.data_3dose_polio is not null then 1 else 0 end)) as quantidade_polio_validas,
	extract (month from age(polio1.data_1dose_polio::timestamp WITH time zone, h.dt_nascimento::timestamp WITH time zone)) as idade_meses_1dose_polio, 
	extract (month from age(polio2.data_2dose_polio::timestamp WITH time zone, h.dt_nascimento::timestamp WITH time zone)) as idade_meses_2dose_polio,
	extract (month from age(polio3.data_3dose_polio::timestamp WITH time zone, h.dt_nascimento::timestamp WITH time zone) )as idade_meses_3dose_polio,
	date(h.dt_nascimento + interval '2 months') as prazo_1dose_polio,
	date(h.dt_nascimento + interval '8 months') as prazo_limite_1dose_polio,
	date(polio1.data_1dose_polio + interval '2 months') as prazo_2dose_polio,
	date(polio2.data_2dose_polio+ interval '2 months') as prazo_3dose_polio
	from dados_nominais_am_labrea.lista_nominal_vacinacao h
    left join primeira_dose_polio polio1 on h.chave_cidadao = polio1.chave_cidadao
    left join segunda_dose_polio polio2 on h.chave_cidadao = polio2.chave_cidadao
    left join terceira_dose_polio polio3 on h.chave_cidadao = polio3.chave_cidadao
    left join quantidade_vacinas_polio_registradas q on q.chave_cidadao = h.chave_cidadao
	group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
), quantidade_vacinas_penta_registradas as ( 
SELECT
        st.chave_cidadao,
        COUNT(DISTINCT st.co_seq_fat_vacinacao_vacina) AS qtde_vacinas_penta_registradas
  FROM dados_nominais_am_labrea.lista_nominal_vacinacao st
  WHERE codigo_vacina = '42'
  GROUP BY 1
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
),
sumarizacao_penta as (
select h.chave_cidadao,
	h.dt_nascimento,
	penta1.data_1dose_penta,
	penta2.data_2dose_penta,
	penta3.data_3dose_penta,
	q.qtde_vacinas_penta_registradas,
	((case when penta1.data_1dose_penta is not null then 1 else 0 end) + (case when penta2.data_2dose_penta is not null then 1 else 0 end) + (case when penta3.data_3dose_penta is not null then 1 else 0 end)) as quantidade_penta_validas,
	extract (month from age(penta1.data_1dose_penta::timestamp WITH time zone, h.dt_nascimento::timestamp WITH time zone)) as idade_meses_1dose_penta, 
	extract (month from age(penta2.data_2dose_penta::timestamp WITH time zone, h.dt_nascimento::timestamp WITH time zone)) as idade_meses_2dose_penta,
	extract (month from age(penta3.data_3dose_penta::timestamp WITH time zone, h.dt_nascimento::timestamp WITH time zone) )as idade_meses_3dose_penta,
	date(h.dt_nascimento + interval '2 months') as prazo_1dose_penta,
	date(h.dt_nascimento + interval '8 months') as prazo_limite_1dose_penta,
	date(penta1.data_1dose_penta + interval '2 months') as prazo_2dose_penta,
	date(penta2.data_2dose_penta+ interval '2 months') as prazo_3dose_penta
	from dados_nominais_am_labrea.lista_nominal_vacinacao h
    left join primeira_dose_penta penta1 on h.chave_cidadao = penta1.chave_cidadao
    left join segunda_dose_penta penta2 on h.chave_cidadao = penta2.chave_cidadao
    left join terceira_dose_penta penta3 on h.chave_cidadao = penta3.chave_cidadao
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
polio.qtde_vacinas_polio_registradas,
polio.quantidade_polio_validas,
polio.idade_meses_1dose_polio,
polio.idade_meses_2dose_polio,
polio.idade_meses_3dose_polio,
polio.prazo_1dose_polio,
polio.prazo_limite_1dose_polio,
polio.prazo_2dose_polio,
polio.prazo_3dose_polio,
penta.data_1dose_penta,
penta.data_2dose_penta,
penta.data_3dose_penta,
penta.qtde_vacinas_penta_registradas,
penta.quantidade_penta_validas,
penta.idade_meses_1dose_penta,
penta.idade_meses_2dose_penta,
penta.idade_meses_3dose_penta,
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
