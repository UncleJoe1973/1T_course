create or replace view public.question_1 as
select
	dense_rank() over(order by count(i.issue_id) desc) as "Место в рейтинге",
	r.reader_name as "Читатель",
	count(i.issue_id) as "Прочитано книг"	
from issues i
	left join readers r on i.fk_reader_id = r.reader_id
where i.return_dt >= make_date(date_part('year', current_date)::integer, 1, 1)
group by r.reader_name;

create or replace view public.question_2 as
select
	r.reader_name as "Читатель",
	count(i.issue_id) as "Книг на руках"	
from issues i
	left join readers r on i.fk_reader_id = r.reader_id
where i.issue_dt <= current_date and i.return_dt is null
group by r.reader_name
order by 2 desc;

create or replace view public.question_3 as --можно использовать одноименную функцию
select
	r.reader_name as "Читатель"
from issues i
	left join readers r on i.fk_reader_id = r.reader_id
where i.fk_book_id = 1 and i.issue_dt <= current_date and i.return_dt is null --идентификатор книги задан числом
order by 1;

create or replace view public.question_4 as
select distinct
	b.book_name as "Наименование"
from issues i
	left join books b on i.fk_book_id = b.book_id
where i.issue_dt <= current_date and i.return_dt is null
order by 1;

create or replace view public.question_5 as
select
	count(distinct r.reader_id) as "Количество должников"
from issues i
	left join readers r on i.fk_reader_id = r.reader_id
where not i.returned and current_date - i.issue_dt > 14;


create or replace view public.question_6 as
select
	dense_rank() over(order by count(i.issue_id) desc) as "Место в рейтинге",
	pub.publisher_name as "Издательство",
	count(i.issue_id) as "Взято книг"	
from issues i
	left join books b on i.fk_book_id = b.book_id
	left join publishers pub on b.fk_publisher_id = pub.publisher_id
group by pub.publisher_name;

create or replace view public.question_7 as
with tmp as(
	select count(b.book_id)
	from books b
		left join books_to_authors ba on b.book_id = ba.fk_book_id
		left join authors au on ba.fk_author_id = au.author_id 
	group by au.author_id order by 1 desc limit 1
)

select
	au.author_name as "Автор",
	count(b.book_id) as "Издано книг"	
from books b
	left join books_to_authors ba on b.book_id = ba.fk_book_id
	left join authors au on ba.fk_author_id = au.author_id
group by au.author_name
having count(b.book_id) = (select * from tmp);

create or replace view public.question_8 as
select
	dense_rank() over(order by round(avg(b.pages / case when i.return_dt - i.issue_dt != 0 then i.return_dt - i.issue_dt else 1 end ), 0) desc),
	r.reader_name as "Читатель",
	round(avg(b.pages / case when i.return_dt - i.issue_dt != 0 then i.return_dt - i.issue_dt else 1 end), 0)  as "Страниц в день"
from issues i
	left join readers r on i.fk_reader_id = r.reader_id
	left join books b on i.fk_book_id = b.book_id
where i.return_dt is not null and returned
group by r.reader_name;