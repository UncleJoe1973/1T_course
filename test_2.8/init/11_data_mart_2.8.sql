CREATE OR REPLACE VIEW "task_2.5" AS 

WITH fct_tbl AS (
	SELECT shop_id AS shop,
			date,
			product_id,
			sales_cnt
	FROM shop_dns
	UNION ALL
	SELECT shop_id,
			date,
			product_id,
			sales_cnt
	FROM shop_mvideo
	UNION ALL
	SELECT shop_id,
			date,
			product_id,
			sales_cnt
	FROM shop_sitilink
	ORDER BY 2
), 
       
fct_tbl_agg AS (
    SELECT fct_tbl.shop,
         	date_trunc('month', fct_tbl.date) as first_day,
            concat(dateName('month', date_trunc('month', fct_tbl.date)), '-', dateName('year', date_trunc('month', fct_tbl.date))) AS plan_month, -- здесь должна быть дата
            fct_tbl.product_id,
            sum(fct_tbl.sales_cnt) AS fct_cnt
    FROM fct_tbl
    GROUP BY fct_tbl.shop, date_trunc('month', date), fct_tbl.product_id
)
        
SELECT f.plan_month AS "Месяц продаж",
	    sh.shop_name,
	    pr.product_name,
	    f.fct_cnt AS sales_fact,
	    p.plan_cnt AS sales_plan,
	    round(f.fct_cnt / p.plan_cnt, 2) AS "sales_fact/sales_plan",
	    round(f.fct_cnt * pr.price, 2) AS income_fact,
	    round(p.plan_cnt * pr.price, 2) AS income_plan,
	    round((f.fct_cnt * pr.price) / (p.plan_cnt * pr.price), 2) AS "income_fact/income_plan"
FROM plan p
     LEFT JOIN fct_tbl_agg f ON p.shop_id = f.shop AND date_trunc('month', p.plan_date) = f.first_day AND p.product_id = f.product_id
     LEFT JOIN products pr ON p.product_id = pr.product_id
     LEFT JOIN shops sh ON p.shop_id = sh.shop_id;