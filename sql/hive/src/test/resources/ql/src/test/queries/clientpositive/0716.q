drop database IF EXISTS FLATER CASCADE;
create database FLATER;
use FLATER;
CREATE TABLE CORRQUALSTAR(EMPNUM STRING,EMPNAME STRING,GRADE DOUBLE,CITY STRING,HOURS DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH 'src/test/resources/plusd/0716/CORRQUALSTAR.csv' OVERWRITE INTO TABLE CORRQUALSTAR;
SELECT COUNT(*) FROM CORRQUALSTAR;
