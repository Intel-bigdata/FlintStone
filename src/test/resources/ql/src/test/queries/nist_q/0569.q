drop database IF EXISTS SUN CASCADE;
create database SUN;
use SUN;
CREATE TABLE WORKS3(EMPNUM STRING,PNUM STRING,HOURS DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '_datapath_/plusd/0569/WORKS3.csv' OVERWRITE INTO TABLE WORKS3;
SELECT COUNT(*) FROM WORKS3 WHERE PNUM = 'P2';