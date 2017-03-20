CREATE EXTERNAL TABLE IF NOT EXISTS marketdata.test (
  `Random` int,
  `RandomInRange` int,
  `Incrementing` int,
  `SetNumber` int,
  `Double` double,
  `Float` float,
  `Dec` decimal(18,6),
  `Timestamp` timestamp,
  `Str` string,
  `DictionaryStr` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://ergon.warehouse/test/';