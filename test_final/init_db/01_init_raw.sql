drop schema if exists exam;

create schema if not exists exam;

create table if not exists exam.raw_data (
	VendorID varchar(8),
	tpep_pickup_datetime varchar(32),
	tpep_dropoff_datetime varchar(32),
	passenger_count int,
	trip_distance float,
	RatecodeID varchar(8),
	store_and_fwd_flag varchar(8),
	PULocationID varchar(8),
	DOLocationID varchar(8),
	payment_type varchar(8),
	fare_amount float,
	extra float,
	mta_tax float,
	tip_amount float,
	tolls_amount float,
	improvement_surcharge float,
	total_amount float,
	congestion_surcharge float
);

COPY exam.raw_data
FROM '/tmp/exam/row_data/yellow_tripdata_2020-01.csv'
DELIMITER ','
ENCODING 'UTF8'
CSV HEADER;