create table if not exists exam.stage_data (
	trip_datetime varchar(32),
	passenger_count int,
	trip_distance float,
	fare_amount float,
	tip_amount float,
	total_amount float
);

insert into exam.stage_data (trip_datetime, passenger_count, trip_distance, fare_amount, tip_amount, total_amount)
select tpep_dropoff_datetime, passenger_count, trip_distance, fare_amount, tip_amount, total_amount
from exam.raw_data
-- анализ исходных данных показал, что они имеют артефакты (null, отрицательные значения (нелогично) и длина поездки в 240 тыс. единиц за несколько минут),
-- поэтому на stage записываем очищенные данные
where passenger_count is not null and 
	  trip_distance > 0 and trip_distance < 300 and --длина поездки без выброса не превышает 300
	  fare_amount > 0 and 
	  tip_amount > 0 and 
	  total_amount > 0;
	 


	  