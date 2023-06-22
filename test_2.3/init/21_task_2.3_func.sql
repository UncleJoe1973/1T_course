
create or replace function public.question_3(book_id integer) returns table (reader_name varchar(100))
language sql
as
$$
select
	r.reader_name as "Читатель"
from issues i
	left join readers r on i.fk_reader_id = r.reader_id
where i.fk_book_id = @book_id and i.issue_dt <= current_date and i.return_dt is null
order by 1
$$;
