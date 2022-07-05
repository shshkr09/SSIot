use iotdemo;
CREATE OR REPLACE PIPELINE `ami_reads`
AS LOAD DATA KAFKA '18.213.94.143:9092/ami_reads'
INTO PROCEDURE `data_validation`
FIELDS TERMINATED BY ',' ENCLOSED BY '' ESCAPED BY '\\'
LINES TERMINATED BY '\n' STARTING BY '';

CREATE OR REPLACE PIPELINE `voltage_reads`
AS LOAD DATA KAFKA '18.213.94.143:9092/voltage_reads'
INTO PROCEDURE `voltage_validation`
FIELDS TERMINATED BY ',' ENCLOSED BY '' ESCAPED BY '\\'
LINES TERMINATED BY '\n' STARTING BY '';

CREATE OR REPLACE PIPELINE `transformer_scada`
AS LOAD DATA KAFKA '18.213.94.143:9092/transformer_scada'
INTO table transformer_reads
FIELDS TERMINATED BY ',' ENCLOSED BY '' ESCAPED BY '\\'
LINES TERMINATED BY '\n' STARTING BY '';

create or replace pipeline meter_to_transformer1
AS LOAD DATA S3 's3://singlestore-test/mt.csv'
CONFIG '{\"region\":\"us-west-1\"}'
CREDENTIALS '{"aws_access_key_id": "AKIAVL66XGD5RLFJQC6F", "aws_secret_access_key": "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"}'
SKIP DUPLICATE KEY ERRORS
INTO TABLE `meter_to_transformer`
FIELDS TERMINATED BY '\t' ENCLOSED BY '"';
start pipeline meter_to_transformer1;
start pipeline ami_reads;
start pipeline voltage_reads;
start pipeline transformer_scada;
