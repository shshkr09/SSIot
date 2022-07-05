create database iotdemo;
use iotdemo;
CREATE TABLE `ami_alarms` (
  `meter_id` int(11) DEFAULT NULL,
  `alarm_dt` datetime DEFAULT NULL,
  `alarm` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `value` decimal(10,4) DEFAULT NULL,
  `create_dt` datetime DEFAULT NULL,
  `written_to_kafka` datetime(6) DEFAULT NULL,
  SHARD KEY `meter_id` (`meter_id`),
  KEY `alarm_dt` (`alarm_dt`) USING CLUSTERED COLUMNSTORE
) AUTOSTATS_CARDINALITY_MODE=INCREMENTAL AUTOSTATS_HISTOGRAM_MODE=CREATE AUTOSTATS_SAMPLING=ON SQL_MODE='STRICT_ALL_TABLES';
CREATE TABLE `ami_reads` (
  `meter_id` int(11) DEFAULT NULL,
  `read_dt` datetime DEFAULT NULL,
  `read_kwh` decimal(10,4) DEFAULT NULL,
  SHARD KEY `meter_id` (`meter_id`),
  KEY `read_dt` (`read_dt`) USING CLUSTERED COLUMNSTORE
) AUTOSTATS_CARDINALITY_MODE=INCREMENTAL AUTOSTATS_HISTOGRAM_MODE=CREATE AUTOSTATS_SAMPLING=ON SQL_MODE='STRICT_ALL_TABLES';
CREATE TABLE `daily_meter_aggregation` (
  `meter_id` int(11) NOT NULL DEFAULT '0',
  `agg_date` date NOT NULL DEFAULT '0000-00-00 00:00:00',
  `m_average` decimal(12,2) DEFAULT NULL,
  `m_sum` decimal(12,2) DEFAULT NULL,
  `create_dt` datetime DEFAULT NULL,
  UNIQUE KEY `PRIMARY` (`meter_id`,`agg_date`) USING HASH,
  SHARD KEY `meter_id` (`meter_id`),
  KEY `agg_date` (`agg_date`) USING HASH,
  KEY `__UNORDERED` () USING CLUSTERED COLUMNSTORE
) AUTOSTATS_CARDINALITY_MODE=PERIODIC AUTOSTATS_HISTOGRAM_MODE=CREATE AUTOSTATS_SAMPLING=ON SQL_MODE='STRICT_ALL_TABLES';
CREATE TABLE `daily_transformer_aggregation` (
  `transformer_id` int(11) NOT NULL DEFAULT '0',
  `agg_date` date NOT NULL DEFAULT '0000-00-00 00:00:00',
  `m_min` decimal(12,2) DEFAULT NULL,
  `m_max` decimal(12,2) DEFAULT NULL,
  `m_average` decimal(12,2) DEFAULT NULL,
  `m_sum` decimal(12,2) DEFAULT NULL,
  `m_count` int(11) DEFAULT NULL,
  `m_stddev` decimal(12,2) DEFAULT NULL,
  `create_dt` datetime DEFAULT NULL,
  `kva` as m_sum/1000/24/.9 PERSISTED decimal(12,2),
  UNIQUE KEY `PRIMARY` (`transformer_id`,`agg_date`) USING HASH,
  SHARD KEY `transformer_id` (`transformer_id`),
  KEY `agg_date` (`agg_date`) USING HASH,
  KEY `__UNORDERED` () USING CLUSTERED COLUMNSTORE
) AUTOSTATS_CARDINALITY_MODE=PERIODIC AUTOSTATS_HISTOGRAM_MODE=CREATE AUTOSTATS_SAMPLING=ON SQL_MODE='STRICT_ALL_TABLES';
CREATE TABLE `hourly_transformer_aggregation` (
  `transformer_id` int(11) NOT NULL DEFAULT '0',
  `agg_date` datetime NOT NULL,
  `m_min` decimal(12,2) DEFAULT NULL,
  `m_max` decimal(12,2) DEFAULT NULL,
  `m_average` decimal(12,2) DEFAULT NULL,
  `m_sum` decimal(12,2) DEFAULT NULL,
  `m_count` int(11) DEFAULT NULL,
  `m_stddev` decimal(12,2) DEFAULT NULL,
  `create_dt` datetime DEFAULT NULL,
  `kva` as m_sum/1000/1/.9 PERSISTED decimal(12,2),
  UNIQUE KEY `PRIMARY` (`transformer_id`,`agg_date`) USING HASH,
  SHARD KEY `transformer_id` (`transformer_id`),
  KEY `agg_date` (`agg_date`) USING HASH,
  KEY `__UNORDERED` () USING CLUSTERED COLUMNSTORE
) AUTOSTATS_CARDINALITY_MODE=PERIODIC AUTOSTATS_HISTOGRAM_MODE=CREATE AUTOSTATS_SAMPLING=ON SQL_MODE='STRICT_ALL_TABLES';
CREATE TABLE `interval_transformer_aggregation` (
  `transformer_id` int(11) NOT NULL DEFAULT '0',
  `agg_date` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `m_min` decimal(12,2) DEFAULT NULL,
  `m_max` decimal(12,2) DEFAULT NULL,
  `m_average` decimal(12,2) DEFAULT NULL,
  `m_sum` decimal(12,2) DEFAULT NULL,
  `m_count` int(11) DEFAULT NULL,
  `m_stddev` decimal(12,2) DEFAULT NULL,
  `create_dt` datetime DEFAULT NULL,
  `kva` as m_sum/1000/.167/.9 PERSISTED decimal(12,2),
  UNIQUE KEY `PRIMARY` (`transformer_id`,`agg_date`) USING HASH,
  SHARD KEY `transformer_id` (`transformer_id`),
  KEY `agg_date` (`agg_date`) USING HASH,
  KEY `__UNORDERED` () USING CLUSTERED COLUMNSTORE
) AUTOSTATS_CARDINALITY_MODE=PERIODIC AUTOSTATS_HISTOGRAM_MODE=CREATE AUTOSTATS_SAMPLING=ON SQL_MODE='STRICT_ALL_TABLES';
CREATE TABLE `meter_aggregation` (
  `meter_id` int(11) NOT NULL DEFAULT '0',
  `m_average` decimal(12,2) DEFAULT NULL,
  `m_sum` decimal(12,2) DEFAULT NULL,
  `create_dt` datetime DEFAULT NULL,
  UNIQUE KEY `PRIMARY` (`meter_id`) USING HASH,
  SHARD KEY `meter_id` (`meter_id`),
  KEY `__UNORDERED` () USING CLUSTERED COLUMNSTORE
) AUTOSTATS_CARDINALITY_MODE=PERIODIC AUTOSTATS_HISTOGRAM_MODE=CREATE AUTOSTATS_SAMPLING=ON SQL_MODE='STRICT_ALL_TABLES';
CREATE REFERENCE TABLE `meter_to_transformer` (
  `meter_id` int(11) NOT NULL DEFAULT '0',
  `transformer_id` int(11) NOT NULL DEFAULT '0',
  `join_dt` datetime DEFAULT NULL,
  UNIQUE KEY `PRIMARY` (`meter_id`,`transformer_id`) USING HASH,
  KEY `index2` (`transformer_id`) USING HASH,
  KEY `__UNORDERED` () USING CLUSTERED COLUMNSTORE
) AUTOSTATS_CARDINALITY_MODE=OFF AUTOSTATS_HISTOGRAM_MODE=OFF AUTOSTATS_SAMPLING=ON SQL_MODE='STRICT_ALL_TABLES';
CREATE TABLE `monthly_meter_aggregation` (
  `meter_id` int(11) NOT NULL DEFAULT '0',
  `agg_date` date NOT NULL DEFAULT '0000-00-00 00:00:00',
  `m_average` decimal(12,2) DEFAULT NULL,
  `m_sum` decimal(12,2) DEFAULT NULL,
  `create_dt` datetime DEFAULT NULL,
  UNIQUE KEY `PRIMARY` (`meter_id`,`agg_date`) USING HASH,
  SHARD KEY `meter_id` (`meter_id`),
  KEY `agg_date` (`agg_date`) USING HASH,
  KEY `__UNORDERED` () USING CLUSTERED COLUMNSTORE
) AUTOSTATS_CARDINALITY_MODE=PERIODIC AUTOSTATS_HISTOGRAM_MODE=CREATE AUTOSTATS_SAMPLING=ON SQL_MODE='STRICT_ALL_TABLES';
CREATE TABLE `transformer_aggregation` (
  `transformer_id` int(11) NOT NULL DEFAULT '0',
  `m_min` decimal(12,2) DEFAULT NULL,
  `m_max` decimal(12,2) DEFAULT NULL,
  `m_average` decimal(12,2) DEFAULT NULL,
  `m_sum` decimal(12,2) DEFAULT NULL,
  `m_count` int(11) DEFAULT NULL,
  `m_stddev` decimal(12,2) DEFAULT NULL,
  `create_dt` datetime DEFAULT NULL,
  UNIQUE KEY `PRIMARY` (`transformer_id`) USING HASH,
  SHARD KEY `transformer_id` (`transformer_id`),
  KEY `__UNORDERED` () USING CLUSTERED COLUMNSTORE
) AUTOSTATS_CARDINALITY_MODE=PERIODIC AUTOSTATS_HISTOGRAM_MODE=CREATE AUTOSTATS_SAMPLING=ON SQL_MODE='STRICT_ALL_TABLES';
CREATE TABLE `transformer_alarms` (
  `transformer_id` int(11) NOT NULL DEFAULT '0',
  `alarm_dt` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `interval` varchar(24) CHARACTER SET utf8 COLLATE utf8_general_ci DEFAULT NULL,
  `alarm` varchar(25) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '',
  `value` decimal(10,4) DEFAULT NULL,
  `create_dt` datetime DEFAULT NULL,
  UNIQUE KEY `PRIMARY` (`transformer_id`,`alarm_dt`,`alarm`) USING HASH,
  SHARD KEY `transformer_id` (`transformer_id`),
  KEY `alarm_dt` (`alarm_dt`) USING HASH,
  KEY `__UNORDERED` () USING CLUSTERED COLUMNSTORE
) AUTOSTATS_CARDINALITY_MODE=OFF AUTOSTATS_HISTOGRAM_MODE=OFF AUTOSTATS_SAMPLING=ON SQL_MODE='STRICT_ALL_TABLES';
CREATE TABLE `transformer_reads` (
  `transformer_id` int(11) DEFAULT NULL,
  `read_dt` datetime DEFAULT NULL,
  `read_kwh` decimal(10,4) DEFAULT NULL,
  SHARD KEY `transformer_id` (`transformer_id`) USING CLUSTERED COLUMNSTORE
) AUTOSTATS_CARDINALITY_MODE=INCREMENTAL AUTOSTATS_HISTOGRAM_MODE=CREATE AUTOSTATS_SAMPLING=ON SQL_MODE='STRICT_ALL_TABLES';
CREATE TABLE `voltage_reads` (
  `meter_id` int(11) DEFAULT NULL,
  `read_dt` datetime DEFAULT NULL,
  `read_volt` decimal(10,4) DEFAULT NULL,
  SHARD KEY `meter_id` (`meter_id`),
  KEY `read_dt` (`read_dt`) USING CLUSTERED COLUMNSTORE
) AUTOSTATS_CARDINALITY_MODE=INCREMENTAL AUTOSTATS_HISTOGRAM_MODE=CREATE AUTOSTATS_SAMPLING=ON SQL_MODE='STRICT_ALL_TABLES';
DELIMITER // 
CREATE OR REPLACE PROCEDURE `compute_scada_deltas`(i int(11) NULL) RETURNS void AS 
declare x int;
begin
while True loop
replace into transformer_alarms select tr.transformer_id, tr.read_dt, "interval" as "interval", "SCADA_10%_HIGHER_THEFT",  tr.read_kwh / a.m_sum  as value, current_timestamp()  from transformer_reads tr, interval_transformer_aggregation  a where tr.transformer_id = a.transformer_id  and tr.read_dt = a.agg_date and a.m_count >= 25  and (a.m_sum*1.001) < tr.read_kwh ;
replace into transformer_alarms select tr.transformer_id, tr.read_dt, "interval" as "interval", "SCADA_20%_LOWER_ORPHAN", tr.read_kwh / a.m_sum  as value, current_timestamp()  from transformer_reads tr, interval_transformer_aggregation  a where tr.transformer_id = a.transformer_id  and tr.read_dt = a.agg_date and a.m_count >= 25 and  a.m_sum > (tr.read_kwh*1.50);
	x = sleep(i);
end loop;
end; // 
DELIMITER ;
DELIMITER // 
CREATE OR REPLACE PROCEDURE `data_validation`(batch query(`_meter_id` int(11) NULL, `_read_dt` datetime NULL, `_read_kwh` decimal(10,4) NULL)) RETURNS void AS 
declare 
check_query query(meter_id int, read_dt datetime, read_kwh decimal(10,2), 14daycount int, 14dayavg decimal(10,2), delta decimal(10,2)) = select b._meter_id, b._read_dt, b._read_kwh, count(a.m_average) as "14daycount" , avg(a.m_average) as "14dayavg", b._read_kwh/avg(a.m_average) as "delta"
from batch b
left join daily_meter_aggregation a on (b._meter_id = a.meter_id and a.agg_date >= date(b._read_dt) - interval 14 day and a.agg_date < date(b._read_dt))
where b._read_kwh > 10
group by 1,2
having 14daycount >= 14 and delta not between .1 and 10;
begin
insert into ami_reads select * from batch;
insert into ami_alarms(meter_id, alarm_dt, alarm, value, create_dt)
select meter_id, read_dt,case when delta <= .1 then "low_read_alarm" when delta >= 10 then "high_read_alarm" end as alarm_name ,delta, current_timestamp() from check_query;
EXCEPTION
	when OTHERS then
    end; // 
DELIMITER ;
DELIMITER // 
CREATE OR REPLACE PROCEDURE `derive_aggs`(i int(11) NULL) RETURNS void AS 
declare x int;
begin
while True loop
	echo
  select concat(current_timestamp, ": deriving aggs") as "status"; 
  replace into meter_aggregation select meter_id, avg(read_kwh), sum(read_kwh), current_timestamp from ami_reads group by meter_id;
  replace into daily_meter_aggregation select meter_id, date(read_dt) ,avg(read_kwh), sum(read_kwh), current_timestamp from ami_reads group by meter_id,date(read_dt);
  replace into monthly_meter_aggregation select meter_id, str_to_date(concat(year(read_dt), "-", month(read_dt)),
	'%Y-%m') as "agg" ,avg(read_kwh), sum(read_kwh), current_timestamp From ami_reads group by meter_id, year(read_dt), month(read_dt) ;
  x = sleep(i);
 end loop;
end; // 
DELIMITER ;
DELIMITER // 
CREATE OR REPLACE PROCEDURE `derive_transformer_aggs`(i int(11) NULL) RETURNS void AS 
declare x int;
begin
while True loop
	echo select concat(current_timestamp,": deriving aggs") as "status";
	replace into transformer_aggregation select transformer_id, min(read_kwh), max(read_kwh), avg(read_kwh), sum(read_kwh), count(read_kwh), stddev(read_kwh), current_timestamp from ami_reads a, meter_to_transformer m where a.meter_id = m.meter_id group by transformer_id;
	replace into daily_transformer_aggregation select transformer_id, date(read_dt) , min(read_kwh), max(read_kwh), avg(read_kwh), sum(read_kwh), count(read_kwh), stddev(read_kwh), current_timestamp from ami_reads a, meter_to_transformer m where a.meter_id = m.meter_id group by transformer_id,date(read_dt);
  replace into interval_transformer_aggregation select transformer_id, read_dt , min(read_kwh) , max(read_kwh), avg(read_kwh) , sum(read_kwh), count(read_kwh), stddev(read_kwh) , current_timestamp from ami_reads a, meter_to_transformer m where a.meter_id = m.meter_id  and minute(read_dt)%10=0  group by transformer_id,read_dt;
 replace into hourly_transformer_aggregation select transformer_id, date_format(read_dt,"%Y-%m-%d %H:00:00") as read_dt, min(read_kwh) , max(read_kwh), avg(read_kwh) , sum(read_kwh), count(read_kwh), stddev(read_kwh) , current_timestamp from ami_reads a, meter_to_transformer m where a.meter_id = m.meter_id group by transformer_id,date_format(read_dt,"%Y-%m-%d %H:00:00") ;
x = sleep(i);
end loop;
end; // 
DELIMITER ;
DELIMITER // 
CREATE OR REPLACE PROCEDURE `voltage_validation`(batch query(`_meter_id` int(11) NULL, `_read_dt` datetime NULL, `_read_volt` decimal(10,4) NULL)) RETURNS void AS 
declare val decimal(10,4);
declare high_alarm_q query(_val decimal(10,4)) =
select 00.00;
declare v_meter_id int;
declare v_read_dt datetime;
declare v_read_kwh decimal(10,4);
begin
insert into voltage_reads select * from batch;
insert into ami_alarms(meter_id, alarm_dt, alarm, value,create_dt)
 select _meter_id, _read_dt,"voltage_out_of_bounds" , _read_volt, current_timestamp() from batch where _read_volt not between 110 and 130;
end; // 
DELIMITER ;
