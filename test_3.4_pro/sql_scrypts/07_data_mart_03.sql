drop table if exists mart.question_3;

create table if not exists mart.question_3 as
select
	extract(year from o.order_dt::timestamp)::varchar as "Год",
	category_name as "Категория",
	subcategory_name as "Подкатегория",
	sum(sales) as "Сумма"
from core.orders o
	join core.categories c using(category_id)
	join core.subcategories s using(subcategory_id)
where extract(year from o.order_dt::timestamp)::varchar like '{{ params.year }}' and category_name like '{{params.category}}%'
group by extract(year from o.order_dt::timestamp)::varchar, category_name, subcategory_name
order by 2, 3;