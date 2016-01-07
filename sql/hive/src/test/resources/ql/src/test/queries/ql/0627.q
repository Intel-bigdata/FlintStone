drop database IF EXISTS SUN CASCADE;
create database SUN;
use SUN;
CREATE TABLE EXPERIENCE(EXP_NAME STRING,BTH_DATE DOUBLE,WK_DATE DOUBLE,DESCR STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH '/home/cherry/sotc_cloud-panthera-nist-test/plusd/0627/EXPERIENCE.csv' OVERWRITE INTO TABLE EXPERIENCE;
SELECT EXP_NAME, DESCR, BTH_DATE FROM EXPERIENCE ORDER BY EXP_NAME, BTH_DATE;