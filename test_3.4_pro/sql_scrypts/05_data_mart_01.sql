drop schema if exists mart cascade;

create schema if not exists mart;

drop table if exists mart.question_1;

create table mart.question_1 as
select
	extract(year from o.order_dt::timestamp)::varchar as "Год",
	segment_name as "Сегмент",
	sum(sales) as "Сумма"
from core.orders o
	join core.customer_segments c using(segment_id)
group by extract(year from o.order_dt::timestamp)::varchar, segment_name
order by 1, 2;
