create table if not exists exam.mart_1f as
with tmp_1 (tr_dt, psg_cnt, psg_grp, trp_dist, fare, tip, total) as (
select 
	to_date(trip_datetime, 'YYYY-MM-DD HH24:MI:SS'),
	passenger_count,
	case 
		when passenger_count = 0 then '0'
		when passenger_count = 1 then '1'
		when passenger_count = 2 then '2'
		when passenger_count = 3 then '3'
		else '4_plus'
	end as psg_grp,
	trip_distance, 
	fare_amount, 
	tip_amount, 
	total_amount
from exam.stage_data
),

tmp_21 (tr_dt, psg_grp, psg_cnt ) as (
select 
	tr_dt,
	psg_grp,
	count(psg_cnt)
from tmp_1
group by tr_dt, psg_grp
),

tmp_22 (tr_dt, psg_grp, pct) as (
select 
	tr_dt,
	psg_grp,
	to_char(round(psg_cnt / sum(psg_cnt) over (partition by tr_dt), 4) * 100, '999D99')	
from tmp_21
),

tmp_23 (tr_dt, psg_grp, percentage_zero, percentage_1p, percentage_2p, percentage_3p, percentage_4p_plus) as (
select
	tr_dt,
	psg_grp,
	case when psg_grp like '0' then pct else '-' end,
	case when psg_grp like '1' then pct else '-' end,
	case when psg_grp like '2' then pct else '-' end,
	case when psg_grp like '3' then pct else '-' end,
	case when psg_grp like '4_plus' then pct else '-' end
from tmp_22
),

tmp_31 (tr_dt, psg_grp, fare_min, fare_max) as (
select distinct
	tr_dt,
	psg_grp,
	min(fare) over (partition by tr_dt, psg_grp),
	max(fare) over (partition by tr_dt, psg_grp)
from tmp_1
)

select 
	tr_dt as "date",  
	percentage_zero, 
	percentage_1p, 
	percentage_2p, 
	percentage_3p, 
	percentage_4p_plus,
	to_char(fare_min, '999D99') as fare_min, 
	to_char(fare_max, '999D99') as fare_max
from tmp_23 join tmp_31 using(tr_dt, psg_grp)
order by 1;

-- расчет по полной стоимости поездки
create table if not exists exam.mart_1t as
with tmp_1 (tr_dt, psg_cnt, psg_grp, trp_dist, fare, tip, total) as (
select 
	to_date(trip_datetime, 'YYYY-MM-DD HH24:MI:SS'),
	passenger_count,
	case 
		when passenger_count = 0 then '0'
		when passenger_count = 1 then '1'
		when passenger_count = 2 then '2'
		when passenger_count = 3 then '3'
		else '4_plus'
	end as psg_grp,
	trip_distance, 
	fare_amount, 
	tip_amount, 
	total_amount
from exam.stage_data
),

tmp_21 (tr_dt, psg_grp, psg_cnt ) as (
select 
	tr_dt,
	psg_grp,
	count(psg_cnt)
from tmp_1
group by tr_dt, psg_grp
),

tmp_22 (tr_dt, psg_grp, pct) as (
select 
	tr_dt,
	psg_grp,
	to_char(round(psg_cnt / sum(psg_cnt) over (partition by tr_dt), 4) * 100, '999D99')	
from tmp_21
),

tmp_23 (tr_dt, psg_grp, percentage_zero, percentage_1p, percentage_2p, percentage_3p, percentage_4p_plus) as (
select
	tr_dt,
	psg_grp,
	case when psg_grp like '0' then pct else '-' end,
	case when psg_grp like '1' then pct else '-' end,
	case when psg_grp like '2' then pct else '-' end,
	case when psg_grp like '3' then pct else '-' end,
	case when psg_grp like '4_plus' then pct else '-' end
from tmp_22
),

tmp_31 (tr_dt, psg_grp, total_min, total_max) as (
select distinct
	tr_dt,
	psg_grp,
	min(total) over (partition by tr_dt, psg_grp),
	max(total) over (partition by tr_dt, psg_grp)
from tmp_1
)

select 
	tr_dt as "date",  
	percentage_zero, 
	percentage_1p, 
	percentage_2p, 
	percentage_3p, 
	percentage_4p_plus,
	to_char(total_min, '999D99') as total_min, 
	to_char(total_max, '999D99') as total_max
from tmp_23 join tmp_31 using(tr_dt, psg_grp)
order by 1;

