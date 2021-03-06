drop database IF EXISTS HU CASCADE;
create database HU;
use HU;
CREATE TABLE TEMP_SS(EMPNUM STRING,GRADE DOUBLE,CITY STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH 'src/test/resources/plusd/0116/TEMP_SS.csv' OVERWRITE INTO TABLE TEMP_SS;
SELECT COUNT(*) FROM TEMP_SS WHERE GRADE = 15;
