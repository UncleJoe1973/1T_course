CREATE TABLE IF NOT EXISTS plan AS
file('plan.csv', 'CSV', 'product_id UInt32, shop_id UInt32, plan_cnt UInt64, plan_date Date');

CREATE TABLE IF NOT EXISTS shops AS
file('shops.csv', 'CSV', 'shop_id UInt32, shop_name String');

CREATE TABLE IF NOT EXISTS products AS
file('products.csv', 'CSV', 'product_id UInt32, product_name String, price Float');

CREATE TABLE IF NOT EXISTS shop_dns AS
file('shop_dns.csv', 'CSV', 'date Date, shop_id UInt32, product_id UInt32, sales_cnt Float');

CREATE TABLE IF NOT EXISTS shop_mvideo AS
file('shop_mvideo.csv', 'CSV', 'date Date, shop_id UInt32, product_id UInt32, sales_cnt Float');

CREATE TABLE IF NOT EXISTS shop_sitilink AS
file('shop_sitilink.csv', 'CSV', 'date Date, shop_id UInt32, product_id UInt32, sales_cnt Float');
