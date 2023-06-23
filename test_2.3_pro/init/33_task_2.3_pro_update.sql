--# Вопрос 2 #--

drop table if exists upd_res;
drop table if exists del_res;
drop table if exists question_pro_2;

create temporary table upd_res as
with tmp as (
	UPDATE books 
	set amount = amount - 1 
	where book_id in (select book_id 
				  from books b
				  left join issues i on b.book_id = i.fk_book_id 
				  where return_dt is not null and not returned)								  
				  RETURNING 'updated' as "operation", book_name, amount
)
SELECT * FROM tmp;


create temporary table del_res as
with tmp as (
	DELETE FROM books WHERE amount <= 0
	RETURNING 'deleted' as "operation", book_name, amount)
SELECT * FROM tmp;


create table if not exists public.question_pro_2 as
SELECT operation, book_name FROM upd_res
union
select operation, book_name from del_res
order by 1 desc;