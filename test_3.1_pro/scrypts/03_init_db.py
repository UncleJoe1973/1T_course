#!/usr/bin/env python
# coding: utf-8

import psycopg2

def func(a):
    return a.strip('\n').replace("'",'')


print(r"Database initializing's starting")

db_name = 'testdb'
db_user = 'postgres'
db_pass = 'postgres'
db_host = 'postgres'
db_port = '5432'

db_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)

# publishers table
sql_string = '''
            create table if not exists publishers
            (
	            publisher_id varchar(32) PRIMARY KEY,
	            publisher_name varchar(100)
            );'''

insert_string = '''insert into publishers (publisher_id, publisher_name) values (%s, %s)'''

with open(r'data/publishers.csv', 'r') as f:
    data = f.readlines()
    tmp = [_.split(';') for _ in data]
    tmp = [tuple(map(func, [_ for _ in lst])) for lst in tmp]

with psycopg2.connect(db_string) as conn:
    cursor = conn.cursor()
    cursor.execute(sql_string)
    cursor.executemany(insert_string, tmp)
    conn.commit()
conn.close()    

# authors table
sql_string = '''
            create table if not exists authors
            (
	            author_id varchar(32) PRIMARY KEY,
	            author_name varchar(100)
            );'''

insert_string = '''insert into authors (author_id, author_name) values (%s, %s)'''

with open(r'data/authors.csv', 'r') as f:
    data = f.readlines()
    tmp = [_.split(';') for _ in data]
    tmp = [tuple(map(func, [_ for _ in lst])) for lst in tmp]

with psycopg2.connect(db_string) as conn:
    cursor = conn.cursor()
    cursor.execute(sql_string)
    cursor.executemany(insert_string, tmp)
    conn.commit()
conn.close()

#readers table
sql_string = '''
            create table if not exists readers
            (
	            reader_id varchar(32) PRIMARY KEY,
                reader_tct varchar(16),
                reader_name varchar(100),
                reader_address varchar(200),
                reader_phone varchar(30)
            );'''

insert_string = '''insert into readers (reader_id, reader_tct, reader_name, reader_address, reader_phone) values (%s, %s, %s, %s, %s)'''

with open(r'data/readers.csv', 'r') as f:
    data = f.readlines()
    tmp = [_.split(';') for _ in data]
    tmp = [tuple(map(func, [_ for _ in lst])) for lst in tmp]

with psycopg2.connect(db_string) as conn:
    cursor = conn.cursor()
    cursor.execute(sql_string)
    cursor.executemany(insert_string, tmp)
    conn.commit()
conn.close()

#employees table
sql_string = '''
            create table if not exists employees
            (
	            employee_id varchar(32) PRIMARY KEY,
	            employee_name varchar(100),
                employee_occup varchar(100)
            );'''

insert_string = '''insert into employees (employee_id, employee_name, employee_occup) values (%s, %s, %s)'''

with open(r'data/employees.csv', 'r') as f:
    data = f.readlines()
    tmp = [_.split(';') for _ in data]
    tmp = [tuple(map(func, [_ for _ in lst])) for lst in tmp]

with psycopg2.connect(db_string) as conn:
    cursor = conn.cursor()
    cursor.execute(sql_string)
    cursor.executemany(insert_string, tmp)
    conn.commit()
conn.close()

#books table
sql_string = '''
            create table if not exists books
            (
	            link text,
                title text,
                price float,
                book_id varchar(32) PRIMARY KEY,
                publisher_id varchar(32) references publishers (publisher_id) on delete cascade on update cascade,
                publ_year varchar(10),
                num_of_pages smallint,
                amount smallint,
                author_id varchar(32) references authors (author_id) on delete cascade on update cascade
            );'''

insert_string = '''insert into books (link, title, price, book_id, publisher_id, publ_year, num_of_pages, amount, author_id) 
                    values (%s, %s, %s, %s, %s, %s, %s, %s, %s)'''

with open(r'data/books.csv', 'r') as f:
    data = f.readlines()
    
    tmp = [_.split(';') for _ in data]
    #tmp = [tmp[i] for i in range(len(tmp)) if not i % 2]
    tmp = [tuple(map(func, [_ for _ in lst])) for lst in tmp]

    with psycopg2.connect(db_string) as conn:
        cursor = conn.cursor()
        cursor.execute(sql_string)
        cursor.executemany(insert_string, tmp)
        conn.commit()
    conn.close()

#books to authors table
sql_string_1 = '''
        create table if not exists books_authors as
        select distinct 
	                author_id,
	                book_id
        from books;
        '''

sql_string_2 = '''
        alter table books_authors
                add foreign key (author_id) references authors (author_id) on delete cascade on update cascade,
                add foreign key	(book_id) references books (book_id) on delete cascade on update cascade;
        '''

sql_string_3 = '''
            alter table books
            drop column author_id ;
            '''

with psycopg2.connect(db_string) as conn:
        cursor = conn.cursor()
        cursor.execute(sql_string_1)
        conn.commit()
        cursor.execute(sql_string_2)
        conn.commit()
        cursor.execute(sql_string_3)
        conn.commit()
conn.close()

print(r"Database initializing has been finished")

