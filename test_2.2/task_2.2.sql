--#Database stucture --

create table if not exists publishers
(
	publisher_id integer PRIMARY KEY,
	publisher_name varchar(100),
	publisher_city varchar(50)
);

create table if not exists employees 
(
	employee_id integer PRIMARY KEY,
	employee_name varchar(100),
	staff_position varchar(50)
);

create table if not exists readers 
(
	reader_id integer PRIMARY KEY,
	reader_ticket varchar(6),
	reader_name varchar(100),
	address varchar(200),
	phone varchar(15)
);

create table if not exists authors
(
	author_id integer PRIMARY KEY,
	author_name varchar(100)	
);

create table if not exists books
(
	book_id integer PRIMARY KEY,
	fk_publisher_id integer references publishers (publisher_id) on delete cascade on update cascade,
	book_name varchar(200),
	publ_year smallint,
	pages smallint,
	price numeric(7, 2),
	amount smallint
	
);

create table if not exists books_to_authors
(
	row_id serial PRIMARY KEY,
	fk_author_id integer references authors (author_id) on delete cascade on update cascade,
	fk_book_id integer references books (book_id) on delete cascade on update cascade,
	unique (fk_author_id, fk_book_id)
);

create table if not exists issues 
(
	issue_id serial PRIMARY KEY,
	fk_employee_id integer references employees (employee_id) on delete cascade on update cascade,
	fk_book_id integer references books (book_id) on delete cascade on update cascade,
	fk_reader_id integer references readers (reader_id) on delete cascade on update cascade,
	issue_dt date,
	return_dt date, -- default '3000-01-01',
	returned boolean -- default false
);

--#Remove old data --

truncate table issues RESTART IDENTITY;
truncate table books_to_authors RESTART IDENTITY;
truncate table books cascade;
truncate table publishers cascade;
truncate table employees cascade;
truncate table readers cascade;
truncate table authors cascade;

-- #Data load --

COPY publishers
FROM 'c:\work\publishers.csv'
DELIMITER ';'
ENCODING 'UTF8'
CSV HEADER;

COPY employees
FROM 'c:\work\employees.csv'
DELIMITER ';'
ENCODING 'UTF8'
CSV HEADER;

COPY readers
FROM 'c:\work\readers.csv'
DELIMITER ';'
ENCODING 'UTF8'
CSV HEADER;

COPY authors
FROM 'c:\work\authors.csv'
DELIMITER ';'
ENCODING 'UTF8'
CSV HEADER;

COPY books
FROM 'c:\work\books.csv'
DELIMITER ';'
ENCODING 'UTF8'
CSV HEADER;

COPY books_to_authors(fk_book_id, fk_author_id)
FROM 'c:\work\books-to-authors.csv'
DELIMITER ';'
ENCODING 'UTF8'
CSV HEADER;

COPY issues (fk_employee_id, fk_book_id, fk_reader_id, issue_dt, return_dt, returned)
FROM 'c:\work\issues.csv'
DELIMITER ';'
ENCODING 'UTF8'
CSV HEADER;


