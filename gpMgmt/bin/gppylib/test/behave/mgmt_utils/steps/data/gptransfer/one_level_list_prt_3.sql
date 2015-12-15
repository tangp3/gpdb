-- list partition type, column key: gender
DROP TABLE IF EXISTS gender_employee;

CREATE TABLE gender_employee(id int, gender char(1))
DISTRIBUTED BY (id) 
PARTITION BY list (gender)
(PARTITION girls VALUES ('F'),
 PARTITION boys VALUES ('M'), 
 DEFAULT PARTITION other );
