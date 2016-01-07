drop database IF EXISTS CUGINI CASCADE;
create database CUGINI;
use CUGINI;
CREATE TABLE VAA(CHARTEST STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '/home/cherry/sotc_cloud-panthera-nist-test/plusd/0006/VAA.csv' OVERWRITE INTO TABLE VAA;
CREATE TABLE AA(CHARTEST STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '/home/cherry/sotc_cloud-panthera-nist-test/plusd/0006/AA.csv' OVERWRITE INTO TABLE AA;
SELECT COUNT(*) FROM CUGINI.AA;