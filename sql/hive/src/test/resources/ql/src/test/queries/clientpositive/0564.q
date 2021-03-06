drop database IF EXISTS SUN CASCADE;
create database SUN;
use SUN;
CREATE TABLE STAFF11(EMPNUM STRING,EMPNAME STRING,GRADE DOUBLE,CITY STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH 'src/test/resources/plusd/0564/STAFF11.csv' OVERWRITE INTO TABLE STAFF11;
SELECT COUNT(*) FROM STAFF11 WHERE EMPNAME = 'Susan' AND GRADE = 11;
