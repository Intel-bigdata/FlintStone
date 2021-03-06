drop database IF EXISTS HU CASCADE;
create database HU;
use HU;
CREATE TABLE LONGINT(LONG_INT DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH 'src/test/resources/plusd/0924/LONGINT.csv' OVERWRITE INTO TABLE LONGINT;
SELECT LONG_INT, LONG_INT /1000000, LONG_INT - 123456789000000. FROM LONGINT;
