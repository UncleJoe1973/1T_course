--# Вопрос 3 - простой отчет №--

create or replace view public.question_pro_3 as
select
	concat(extract("year" from i.return_dt), '-Q0', extract("quarter" from i.return_dt)) as "Квартал",
	sum(b.price) as "Общая стоимость"
from issues i
	left join books b on i.fk_book_id = b.book_id
	left join readers r on i.fk_reader_id = r.reader_id
where not i.returned and i.return_dt is not null
group by concat(extract("year" from i.return_dt), '-Q0', extract("quarter" from i.return_dt))
order by 1;

--# Вопрос 3 - отчет в виде сводной таблицы №--

create extension if not exists tablefunc;

create or replace view public.cte as -- вспомогательное представление для расчета сводной таблицы
select
	r.reader_name as reader,
	concat(extract("year" from i.return_dt), '-Q0', extract("quarter" from i.return_dt)) as quarter,
	b.price as price
from issues i
	left join books b on i.fk_book_id = b.book_id
	left join readers r on i.fk_reader_id = r.reader_id
where not i.returned and i.return_dt is not null
order by 2;

create or replace view public.question_pro_3_pivot as
select * 
from crosstab(
	concat(
		'select coalesce(t.reader, ' 
		'''Итого''', 
		') as reader,
		coalesce(t.quarter, ',
		'''Итого''',
		') as quarter,
		sum(t.price) as agg
		from cte t
		group by cube(t.reader, t.quarter)
		order by reader, quarter'),
	concat(
		'(select distinct tt.quarter as quarter from cte tt order by quarter) union all select ',
		'''Итого''')
     )
as cst1("reader" varchar, "2023-Q01" numeric, "2023-Q02" numeric, "Итого" numeric);	
