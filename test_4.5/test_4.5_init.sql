set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.max.dynamic.partitions.pernode = 1000;

set hive.enforce.bucketing = true;

CREATE TABLE IF NOT EXISTS customers_tmp(
        row_nbr int,
        Customer_Id string,
        First_Name string,
        Last_Name string,
        Company string,
        City string,
        Country string,
        Phone_1 string,
        Phone_2 string,
        Email string,
        Subscription_Date date,
        Website string,
        year_subs string,
        cust_group string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/dmitry/customers_new.csv' INTO TABLE customers_tmp;

CREATE TABLE IF NOT EXISTS customers(
        row_nbr int,
        Customer_Id string,
        First_Name string,
        Last_Name string,
        Company string,
        City string,
        Country string,
        Phone_1 string,
        Phone_2 string,
        Email string,
        Subscription_Date date,
        Website string,
        year_subs string)
PARTITIONED BY(cust_group string)
CLUSTERED BY (Customer_Id) INTO 64 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE customers  PARTITION (cust_group) 
SELECT *  FROM default.customers_tmp;

drop table customers_tmp;

CREATE TABLE IF NOT EXISTS oranizations_tmp(
        row_nbr int,
        Organization_Id string,
        Name string,
        Website string,
        Country string,
        Description string,
        Founded string,
        Industry string,
        Number_of_employees int,
        cust_group string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/dmitry/organizations_new.csv' INTO TABLE oranizations_tmp;

CREATE TABLE IF NOT EXISTS oranizations(
        row_nbr int,
        Organization_Id string,
        Name string,
        Website string,
        Country string,
        Description string,
        Founded string,
        Industry string,
        Number_of_employees int)
PARTITIONED BY(cust_group string)
CLUSTERED BY (Name) INTO 64 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE oranizations PARTITION (cust_group) 
SELECT *  FROM default.oranizations_tmp;

drop table oranizations_tmp;

CREATE TABLE IF NOT EXISTS people_tmp(
        row_nbr int,
        User_Id string,
        First_Name string,
        Last_Name string,
        Sex string,
        Email string,
        Phone string,
        Date_of_birth date,
        Job_Title string,
        cust_group string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
TBLPROPERTIES ("skip.header.line.count"="1");

LOAD DATA INPATH '/user/dmitry/people_new.csv' INTO TABLE people_tmp;

CREATE TABLE IF NOT EXISTS people(
        row_nbr int,
        User_Id string,
        First_Name string,
        Last_Name string,
        Sex string,
        Email string,
        Phone string,
        Date_of_birth date,
        Job_Title string)
PARTITIONED BY(cust_group string)
CLUSTERED BY (User_Id) INTO 64 BUCKETS
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

INSERT OVERWRITE TABLE people  PARTITION (cust_group) 
SELECT *  FROM default.people_tmp;

drop table people_tmp;

CREATE TABLE IF NOT EXISTS age_grp(
        low_thr int,
        high_thr int,
        age_grp string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA INPATH '/user/dmitry/age_groups.csv' INTO TABLE age_grp;





