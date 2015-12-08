drop database IF EXISTS SUN CASCADE;
create database SUN;
use SUN;
CREATE TABLE CHAR_DEFAULT(SEX_CODE STRING,NICKNAME STRING,INSURANCE1 STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '_datapath_/plusd/0616/CHAR_DEFAULT.csv' OVERWRITE INTO TABLE CHAR_DEFAULT;
SELECT SEX_CODE FROM CHAR_DEFAULT WHERE INSURANCE1 = 'Kaise';