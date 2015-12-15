-- list partition type, column key: id
DROP TABLE IF EXISTS id_employee;

CREATE TABLE id_employee(id int, rank int, gender char(1))
DISTRIBUTED BY (id)
PARTITION BY list (id)
(PARTITION main VALUES (1),
 PARTITION private VALUES (2),
 DEFAULT PARTITION other );
