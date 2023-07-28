drop schema if exists core cascade;

create schema if not exists core;

create table if not exists core.ship_mode
(
	ship_mode_id varchar(64),
	ship_mode_name varchar(64),
	PRIMARY KEY (ship_mode_id)
);

create table if not exists core.order_ids
(
	order_id_id varchar(64),
	order_id_name varchar(200),
	PRIMARY KEY (order_id_id)
);
	
create table if not exists core.customer_ids
(
	customer_id_id varchar(64),
	customer_id_name varchar(200),
	PRIMARY KEY (customer_id_id)
);
	
create table if not exists core.customer_names
(
	customer_name_id varchar(64),
	customer_name varchar(200),
	PRIMARY KEY (customer_name_id)
);
	
create table if not exists core.customer_segments
(
	segment_id varchar(64),
	segment_name varchar(200),
	PRIMARY KEY (segment_id)
);

create table if not exists core.countries
(
	country_id varchar(64),
	country_name varchar(200),
	PRIMARY KEY (country_id)
);

create table if not exists core.cities
(
	city_id varchar(64),
	city_name varchar(200),
	PRIMARY KEY (city_id)
);

create table if not exists core.states
(
	state_id varchar(64),
	state_name varchar(200),
	PRIMARY KEY (state_id)
);

create table if not exists core.regions
(
	region_id varchar(64),
	region_name varchar(200),
	PRIMARY KEY (region_id)
);

create table if not exists core.product_ids
(
	product_id_id varchar(64),
	product_id_name varchar(200),
	PRIMARY KEY (product_id_id)
);


create table if not exists core.categories
(
	category_id varchar(64),
	category_name varchar(200),
	PRIMARY KEY (category_id)
);

create table if not exists core.subcategories
(
	subcategory_id varchar(64),
	subcategory_name varchar(200),
	PRIMARY KEY (subcategory_id)
);

create table if not exists core.product_names
(
	product_name_id varchar(64),
	product_name varchar(200),
	PRIMARY KEY (product_name_id)
);


create table if not exists core.orders
(
	row_id varchar(64),
	order_id_id varchar(64),
	order_dt date,
	ship_dt date,
	ship_mode_id varchar(64),
	customer_id_id varchar(64),
	customer_name_id varchar(64),
	segment_id varchar(64),
	country_id varchar(64),
	city_id varchar(64),
	state_id varchar(64),
	postal_code varchar(64),
	region_id varchar(64),
	product_id_id varchar(64),
	category_id varchar(64),
	subcategory_id varchar(64),
	product_name_id varchar(64),
	sales real,
	quantity integer,
	discount real,
	profit real, 
	PRIMARY KEY (row_id),
	FOREIGN KEY (order_id_id) references core.order_ids (order_id_id),	
	FOREIGN KEY (ship_mode_id) references core.ship_mode (ship_mode_id),
	FOREIGN KEY (customer_id_id) references core.customer_ids (customer_id_id),
	FOREIGN KEY (customer_name_id) references core.customer_names (customer_name_id),
	FOREIGN KEY (segment_id) references core.customer_segments (segment_id),
	FOREIGN KEY (country_id) references core.countries (country_id),
	FOREIGN KEY (city_id) references core.cities (city_id),
	FOREIGN KEY (state_id) references core.states (state_id),
	FOREIGN KEY (region_id) references core.regions (region_id),
	FOREIGN KEY (product_id_id) references core.product_ids (product_id_id),
	FOREIGN KEY (category_id) references core.categories (category_id),
	FOREIGN KEY (subcategory_id) references core.subcategories (subcategory_id),
	FOREIGN KEY (product_name_id) references core.product_names (product_name_id)	
);


