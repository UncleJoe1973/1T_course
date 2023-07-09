CREATE TABLE IF NOT EXISTS data_mart
(    
	"личный кабинет" String, 
	"пол" String, 
	"возрастная категория" String, 
	"e-mail" String, 
	"телефон" String,
	"самый покупаемый за месяц товар" String, 
	"самый часто покупаемый бренд" String, 
	"категория самых частых покупок" String, 
	"сумма, потраченная за месяц" Float, 
	"количество товаров со скидкой" UInt8
) 
ENGINE = PostgreSQL('postgres:5432', 'testdb', 'data_mart', 'postgres', 'postgres');