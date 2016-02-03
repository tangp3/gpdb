DROP DATABASE IF EXISTS " DB`~@#$%^&*()_-+[{]}|\;: \'/?><;1 ";

CREATE DATABASE " DB`~@#$%^&*()_-+[{]}|\;: \'/?><;1 ";

\c " DB`~@#$%^&*()_-+[{]}|\;: \'/?><;1 ";

CREATE SCHEMA " S`~@#$%^&*()-+[{]}|\;: \'""/?><1 ";

SET SEARCH_PATH=" S`~@#$%^&*()-+[{]}|\;: \'""/?><1 ";

Create table co (Column1 int, Column2 varchar(20), Column3 date) 
    WITH(appendonly = true, orientation = column)  
    Distributed Randomly Partition by list(Column2)
    Subpartition by range(Column3) Subpartition Template
        (start (date '2014-01-01') end (date '2016-01-01') every (interval '1 year'))
    (Partition p1 values('backup') , Partition p2 values('restore')) ;

Create table ao (Column1 int, Column2 varchar(20), Column3 date) 
    WITH(appendonly = true, orientation = row, compresstype = quicklz)  
    Distributed Randomly Partition by list(Column2) 
    Subpartition by range(Column3) Subpartition Template
        (start (date '2014-01-01') end (date '2016-01-01') every (interval '1 year'))
    (Partition p1 values('backup') , Partition p2 values('restore')) ;

Create table heap (Column1 int, Column2 varchar(20), Column3 date);

INSERT INTO co VALUES (2, 'backup', '2015-01-01');
INSERT INTO co VALUES (3, 'restore', '2015-01-01');

INSERT INTO ao VALUES (1, 'backup', '2014-01-01');
INSERT INTO ao VALUES (2, 'backup', '2015-01-01');
INSERT INTO ao VALUES (3, 'restore', '2015-01-01');

INSERT INTO heap VALUES (101, 'backup-restore', '2016-01-27');
