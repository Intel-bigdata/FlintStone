drop database IF EXISTS CANWEPARSELENGTH18 CASCADE;
create database CANWEPARSELENGTH18;
use CANWEPARSELENGTH18;
CREATE TABLE CHARACTER18TABLE18(CHARS18NAME18CHARS STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE;
LOAD DATA LOCAL INPATH 'src/test/resources/plusd/0061/CHARACTER18TABLE18.csv' OVERWRITE INTO TABLE CHARACTER18TABLE18;
SELECT CORRELATIONNAMES18.CHARS18NAME18CHARS FROM CHARACTER18TABLE18 CORRELATIONNAMES18 WHERE CORRELATIONNAMES18.CHARS18NAME18CHARS = 'VAL4';
