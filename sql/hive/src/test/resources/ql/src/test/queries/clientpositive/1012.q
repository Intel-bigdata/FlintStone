drop database IF EXISTS FLATER CASCADE;
create database FLATER;
use FLATER;
CREATE TABLE YESMAIL(C1 DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH 'src/test/resources/plusd/1012/YESMAIL.csv' OVERWRITE INTO TABLE YESMAIL;
SELECT C1 FROM YESMAIL WHERE C1 < 0;
