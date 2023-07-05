CREATE TABLE IF NOT EXISTS ch_task_25_pro
(    
    "Магазин" String NULL,
	"Продукт" String NULL,
	"Месяц продаж" String NULL,
	sales_fact UInt8 NULL,
	sales_plan UInt8 NULL,
	"sales_fact/sales_plan" Float NULL,
	income_fact Float NULL,
	income_plan Float NULL,
	"income_fact/income_plan" Float NULL,
	"avg(sales/date)" UInt8 NULL,
	max_sales UInt8 NULL,
	date_max_sales String NULL,
	date_max_sales_is_promo UInt8 NULL,
	"avg(sales/date) / max_sales" Float NULL,
	promo_len UInt8 NULL,
	"promo_sales_cnt/fact_sales" Float NULL,
	promo_income Float NULL,
	"promo_income/fact_income" Float NULL
) 
ENGINE = PostgreSQL('postgres:5432', 'testdb', 'task_2.5_pro', 'postgres', 'postgres');
