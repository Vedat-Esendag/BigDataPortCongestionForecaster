-- F2: split raw Danish Maritime Authority rows into the two history topics.
-- One DMA CSV row can carry a position, static data, or both, and a Kafka Connect source writes
-- to one topic only, so the FilePulse connector writes every row to ais.history.raw and this
-- file fans it out. It touches only ais.history.* topics.
--
-- STATUS: not yet run against real DMA data. TODO(F2): map the text columns navigational_status
-- and ship_type to AIS codes (nav_status and ship_type are NULL until then).

CREATE STREAM IF NOT EXISTS history_raw (
  `mmsi_key` STRING KEY
) WITH (
  KAFKA_TOPIC = 'ais.history.raw',
  KEY_FORMAT = 'KAFKA',
  VALUE_FORMAT = 'AVRO'
);

CREATE STREAM IF NOT EXISTS history_position WITH (
  KAFKA_TOPIC = 'ais.history.position',
  KEY_FORMAT = 'KAFKA',
  VALUE_FORMAT = 'AVRO',
  VALUE_AVRO_SCHEMA_FULL_NAME = 'com.sdu.portcongestion.AisPosition'
) AS
SELECT
  `mmsi_key`,
  CAST(`mmsi` AS BIGINT) AS `mmsi`,
  PARSE_TIMESTAMP(`timestamp`, 'dd/MM/yyyy HH:mm:ss') AS `event_time`,
  FROM_UNIXTIME(ROWTIME) AS `received_time`,
  CAST(`latitude` AS DOUBLE) AS `lat`,
  CAST(`longitude` AS DOUBLE) AS `lon`,
  CAST(`sog` AS DOUBLE) AS `sog`,
  CAST(`cog` AS DOUBLE) AS `cog`,
  CAST(`heading` AS INT) AS `heading`,
  CAST(`rot` AS DOUBLE) AS `rot`,
  CAST(NULL AS INT) AS `nav_status`,
  CAST(NULL AS INT) AS `message_type`,
  'dma' AS `source`
FROM history_raw
WHERE `mmsi` IS NOT NULL AND `latitude` IS NOT NULL AND `longitude` IS NOT NULL
EMIT CHANGES;

CREATE STREAM IF NOT EXISTS history_static WITH (
  KAFKA_TOPIC = 'ais.history.static',
  KEY_FORMAT = 'KAFKA',
  VALUE_FORMAT = 'AVRO',
  VALUE_AVRO_SCHEMA_FULL_NAME = 'com.sdu.portcongestion.AisStatic'
) AS
SELECT
  `mmsi_key`,
  CAST(`mmsi` AS BIGINT) AS `mmsi`,
  PARSE_TIMESTAMP(`timestamp`, 'dd/MM/yyyy HH:mm:ss') AS `event_time`,
  FROM_UNIXTIME(ROWTIME) AS `received_time`,
  CAST(`imo` AS BIGINT) AS `imo`,
  `callsign` AS `call_sign`,
  `name` AS `name`,
  CAST(NULL AS INT) AS `ship_type`,
  CAST(`length` AS DOUBLE) AS `length_m`,
  CAST(`width` AS DOUBLE) AS `beam_m`,
  CAST(`draught` AS DOUBLE) AS `draught_m`,
  `destination` AS `destination`,
  PARSE_TIMESTAMP(`eta`, 'dd/MM/yyyy HH:mm:ss') AS `eta_broadcast`,
  'dma' AS `source`
FROM history_raw
WHERE `mmsi` IS NOT NULL
  AND (`imo` IS NOT NULL OR `destination` IS NOT NULL OR `eta` IS NOT NULL OR `ship_type` IS NOT NULL)
EMIT CHANGES;
