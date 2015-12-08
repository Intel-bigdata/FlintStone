drop database IF EXISTS SCHANZLE CASCADE;
create database SCHANZLE;
use SCHANZLE;
CREATE TABLE FOUR_TYPES(T_INT DOUBLE,T_CHAR STRING,T_DECIMAL DOUBLE,T_REAL FLOAT) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '_datapath_/plusd/0470/FOUR_TYPES.csv' OVERWRITE INTO TABLE FOUR_TYPES;
SELECT T_DECIMAL / .000000001 FROM FOUR_TYPES WHERE T_CHAR = 'X';