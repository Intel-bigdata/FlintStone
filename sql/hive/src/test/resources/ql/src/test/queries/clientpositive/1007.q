drop database IF EXISTS FLATER CASCADE;
create database FLATER;
use FLATER;
CREATE TABLE NOMAIL(C1 DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH 'src/test/resources/plusd/1007/NOMAIL.csv' OVERWRITE INTO TABLE NOMAIL;
SELECT C1 FROM NOMAIL WHERE C1 < 0;