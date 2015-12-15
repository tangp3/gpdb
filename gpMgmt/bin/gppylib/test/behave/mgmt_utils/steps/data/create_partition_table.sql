-- multi level partition table
CREATE TABLE src_sales (src_trans_id int, src_date date, src_amount decimal(9,2), src_region text) 
DISTRIBUTED BY (src_trans_id)
PARTITION BY RANGE (src_date)
SUBPARTITION BY LIST (src_region)
SUBPARTITION TEMPLATE
( SUBPARTITION usa VALUES ('usa'), 
  SUBPARTITION asia VALUES ('asia'), 
  SUBPARTITION europe VALUES ('europe'), 
  DEFAULT SUBPARTITION other_regions)
(START (date '2011-01-01') INCLUSIVE
END (date '2012-01-01') EXCLUSIVE
EVERY (INTERVAL '1 month'),
DEFAULT PARTITION outlying_dates );

-- list partition on source system
CREATE TABLE gender_employee(id int, rank int, gender char(1))
DISTRIBUTED BY (id)
PARTITION BY list (gender)
(PARTITION girls VALUES ('F')
 PARTITION boys VALUES ('M'), 
 DEFAULT PARTITION other );

-- list partition on source system
CREATE TABLE id_employee(id int, rank int, gender char(1))
DISTRIBUTED BY (id)
PARTITION BY list (id)
(PARTITION main VALUES (1)
 PARTITION private VALUES (2), 
 DEFAULT PARTITION other );

-- two level partitions
CREATE TABLE two_level_partition (id int, year int, month int, day int, 
region text)
DISTRIBUTED BY (id)
PARTITION BY RANGE (year)
SUBPARTITION BY RANGE (month)
SUBPARTITION TEMPLATE (
    START (1) END (13) EVERY (1), 
    DEFAULT SUBPARTITION other_months )
(START (2002) END (2004) EVERY (1), 
  DEFAULT PARTITION outlying_years );


-- multi level partition, list part followed by range partition, china
CREATE TABLE dest_sales (dest_trans_id int, dest_date date, dest_amount decimal(9,2), dest_region text)
DISTRIBUTED BY (dest_trans_id)
PARTITION BY LIST (dest_region)
SUBPARTITION BY RANGE (dest_date)
SUBPARTITION TEMPLATE
(START (date '2011-01-01') INCLUSIVE
END (date '2012-01-01') EXCLUSIVE
EVERY (INTERVAL '1 month'),
DEFAULT SUBPARTITION outlying_dates )
( PARTITION usa VALUES ('usa'),
PARTITION china VALUES ('china'),
PARTITION other_asia VALUES ('asia'),
PARTITION europe VALUES ('europe'),
DEFAULT PARTITION other_regions);

-- multi level partition, list part followed by range partition, every 2 months
CREATE TABLE dest_sales (dest_trans_id int, dest_date date, dest_amount decimal(9,2), dest_region text)
DISTRIBUTED BY (dest_trans_id)
PARTITION BY LIST (dest_region)
SUBPARTITION BY RANGE (dest_date)
SUBPARTITION TEMPLATE
(START (date '2011-01-01') INCLUSIVE
END (date '2012-01-01') EXCLUSIVE
EVERY (INTERVAL '2 months'),
DEFAULT SUBPARTITION outlying_dates )
( PARTITION usa VALUES ('usa'),
PARTITION asia VALUES ('asia'),
PARTITION europe VALUES ('europe'),
DEFAULT PARTITION other_regions);

-- range partition on destination
CREATE TABLE employee(id int, rank int, gender char(1))
DISTRIBUTED BY (id)
PARTITION BY RANGE (rank)
( START (id 1) INCLUSIVE
  END (id 10) EXCLUSIVE
  EVERY (INTERVAL 1) );

-- range partition on destination
CREATE TABLE employee(eid int, erank int, egender char(1))
DISTRIBUTED BY (eid)
PARTITION BY RANGE (erank)
( START (id 1) INCLUSIVE
  END (id 10) EXCLUSIVE
  EVERY (INTERVAL 1) );

-- range partition on destination
CREATE TABLE heap_employee(id int, rank int, gender char(1));

-- two level partitions
CREATE TABLE two_level_partition (id int, year int, month int, day int, 
region text)
DISTRIBUTED BY (id)
PARTITION BY RANGE (year)
SUBPARTITION BY RANGE (month)
SUBPARTITION TEMPLATE (
    START (1) END (13) EVERY (1), 
    DEFAULT SUBPARTITION other_months )
(START (2002) END (2004) EVERY (1), 
  DEFAULT PARTITION outlying_years );

-- one level partition
CREATE TABLE one_level_partition (id int, year int, month int, day int, 
region text)
DISTRIBUTED BY (id)
PARTITION BY RANGE (month)
(START (1) END (13) EVERY (1), 
DEFAULT PARTITION other_months )


-- partition table with four columns
CREATE TABLE one_level_partition (id int, year int, month int, day int)
DISTRIBUTED BY (id)
PARTITION BY RANGE (month)
(START (1) END (13) EVERY (1), 
DEFAULT PARTITION other_months )


-- list partition on rank 
CREATE TABLE rank_employee(id int, rank int, gender char(1))
DISTRIBUTED BY (id)
PARTITION BY list (rank)
(PARTITION senior VALUES (1)
 PARTITION newbie VALUES (2), 
 DEFAULT PARTITION other );
