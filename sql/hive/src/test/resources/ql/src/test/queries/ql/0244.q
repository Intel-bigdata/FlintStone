drop database IF EXISTS HU CASCADE;
create database HU;
use HU;
CREATE TABLE PP_7(NUMTEST DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '/home/cherry/sotc_cloud-panthera-nist-test/plusd/0244/PP_7.csv' OVERWRITE INTO TABLE PP_7;
CREATE TABLE PP_15(NUMTEST DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '/home/cherry/sotc_cloud-panthera-nist-test/plusd/0244/PP_15.csv' OVERWRITE INTO TABLE PP_15;
CREATE TABLE PP(NUMTEST DOUBLE) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '/home/cherry/sotc_cloud-panthera-nist-test/plusd/0244/PP.csv' OVERWRITE INTO TABLE PP;
SELECT * FROM PP;