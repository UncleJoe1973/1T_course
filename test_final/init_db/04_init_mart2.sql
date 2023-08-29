create table if not exists exam.mart_2 as
with tmp_2 (psg_cnt, trp_dst, trip_grp, trip_thr, tip) as (
select 
	passenger_count,
	trip_distance,
	case 
		when trip_distance < 10 then '0-10'
		when trip_distance < 20 then '10-20'
		when trip_distance < 30 then '20-30'
		when trip_distance < 40 then '30-40'
		when trip_distance < 50 then '40-50'
		when trip_distance < 60 then '50-60'
		when trip_distance < 70 then '60-70'
		when trip_distance < 80 then '70-80'
		when trip_distance < 90 then '80-90'
		when trip_distance < 100 then '90-100'
		else 'более 100'
	end,	
		case 
		when trip_distance < 10 then 10
		when trip_distance < 20 then 20
		when trip_distance < 30 then 30
		when trip_distance < 40 then 40
		when trip_distance < 50 then 50
		when trip_distance < 60 then 60
		when trip_distance < 70 then 70
		when trip_distance < 80 then 80
		when trip_distance < 90 then 90
		when trip_distance < 100 then 100
		else 110
	end as trip_thr,
	tip_amount
from exam.stage_data
)

select
	psg_cnt, 
	trip_grp,
	trip_thr,
	avg(tip) as avg_tip
from tmp_2
group by psg_cnt, trip_grp, trip_thr
order by 1, 2