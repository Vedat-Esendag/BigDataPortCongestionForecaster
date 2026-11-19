-- F2: split raw Danish Maritime Authority rows into the two history topics.
-- One DMA CSV row can carry a position, static data, or both, and a Kafka Connect source writes
-- to one topic only, so the FilePulse connector writes every row to ais.history.raw and this
-- file fans it out. It touches only ais.history.* topics.
--
-- DMA writes a missing value as an empty field or as the text 'Unknown' / 'Undefined' (IMO is
-- 'Unknown' on about half the rows), so those count as NULL here. Casting them directly would fail
-- per row, and a text 'Unknown' would pass every IS NOT NULL test.
-- DMA repeats a ship's static data on most of its position rows, so about 58% of rows still carry
-- static data; keeping one row per ship and change is left to the cleaning job (F4).
--
-- STATUS: tested on ksqlDB with sample rows, not yet with the FilePulse source. TODO(F2): map the
-- text columns navigational_status and ship_type to AIS codes (nav_status and ship_type are NULL
-- until then).

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
  CASE WHEN `sog` IN ('', 'Unknown', 'Undefined') THEN NULL ELSE CAST(`sog` AS DOUBLE) END AS `sog`,
  CASE WHEN `cog` IN ('', 'Unknown', 'Undefined') THEN NULL ELSE CAST(`cog` AS DOUBLE) END AS `cog`,
  CASE WHEN `heading` IN ('', 'Unknown', 'Undefined') THEN NULL ELSE CAST(`heading` AS INT) END AS `heading`,
  CASE WHEN `rot` IN ('', 'Unknown', 'Undefined') THEN NULL ELSE CAST(`rot` AS DOUBLE) END AS `rot`,
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
  CASE WHEN `imo` IN ('', 'Unknown', 'Undefined') THEN NULL ELSE CAST(`imo` AS BIGINT) END AS `imo`,
  CASE WHEN `callsign` IN ('', 'Unknown', 'Undefined') THEN NULL ELSE `callsign` END AS `call_sign`,
  CASE WHEN `name` IN ('', 'Unknown', 'Undefined') THEN NULL ELSE `name` END AS `name`,
  CAST(NULL AS INT) AS `ship_type`,
  CASE WHEN `length` IN ('', 'Unknown', 'Undefined') THEN NULL ELSE CAST(`length` AS DOUBLE) END AS `length_m`,
  CASE WHEN `width` IN ('', 'Unknown', 'Undefined') THEN NULL ELSE CAST(`width` AS DOUBLE) END AS `beam_m`,
  CASE WHEN `draught` IN ('', 'Unknown', 'Undefined') THEN NULL ELSE CAST(`draught` AS DOUBLE) END AS `draught_m`,
  CASE WHEN `destination` IN ('', 'Unknown', 'Undefined') THEN NULL ELSE `destination` END AS `destination`,
  CASE WHEN `eta` IN ('', 'Unknown') THEN NULL ELSE PARSE_TIMESTAMP(`eta`, 'dd/MM/yyyy HH:mm:ss') END AS `eta_broadcast`,
  'dma' AS `source`
FROM history_raw
WHERE `mmsi` IS NOT NULL
  -- Real static values only. ship_type is not used: DMA fills it in on every row.
  AND ((`imo` IS NOT NULL AND `imo` NOT IN ('', 'Unknown'))
    OR (`destination` IS NOT NULL AND `destination` NOT IN ('', 'Unknown'))
    OR (`eta` IS NOT NULL AND `eta` NOT IN ('', 'Unknown')))
EMIT CHANGES;
