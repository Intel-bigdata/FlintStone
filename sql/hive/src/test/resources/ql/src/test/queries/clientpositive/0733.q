drop database IF EXISTS FLATER CASCADE;
create database FLATER;
use FLATER;
CREATE TABLE INCOMPLETES(ITEMTEXT STRING,CONDTEXT STRING,COSTTEXT STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH 'src/test/resources/plusd/0733/INCOMPLETES.csv' OVERWRITE INTO TABLE INCOMPLETES;
SELECT COUNT(*) FROM INCOMPLETES;
