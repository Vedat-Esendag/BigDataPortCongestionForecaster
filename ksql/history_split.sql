-- F2: split raw Danish Maritime Authority rows into the two history topics.
-- One DMA CSV row can carry a position, static data, or both, and a Kafka Connect source writes
-- to one topic only, so the FilePulse connector writes every row to ais.history.raw and this
-- file fans it out. It touches only ais.history.* topics.
--
-- ksqlDB upper-cases the column names it reads from the Avro schema of ais.history.raw, so input
-- columns are written `MMSI`, `LATITUDE` and so on (backticks keep the case exactly). Output names
-- are lower case in backticks (AS `mmsi`) to match schemas/avro/ais.history.*.avsc.
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
  CAST(`MMSI` AS BIGINT) AS `mmsi`,
  PARSE_TIMESTAMP(`TIMESTAMP`, 'dd/MM/yyyy HH:mm:ss') AS `event_time`,
  FROM_UNIXTIME(ROWTIME) AS `received_time`,
  CAST(`LATITUDE` AS DOUBLE) AS `lat`,
  CAST(`LONGITUDE` AS DOUBLE) AS `lon`,
  CASE WHEN `SOG` IN ('', 'Unknown', 'Undefined') THEN NULL ELSE CAST(`SOG` AS DOUBLE) END AS `sog`,
  CASE WHEN `COG` IN ('', 'Unknown', 'Undefined') THEN NULL ELSE CAST(`COG` AS DOUBLE) END AS `cog`,
  CASE WHEN `HEADING` IN ('', 'Unknown', 'Undefined') THEN NULL ELSE CAST(`HEADING` AS INT) END AS `heading`,
  CASE WHEN `ROT` IN ('', 'Unknown', 'Undefined') THEN NULL ELSE CAST(`ROT` AS DOUBLE) END AS `rot`,
  CAST(NULL AS INT) AS `nav_status`,
  CAST(NULL AS INT) AS `message_type`,
  'dma' AS `source`
FROM history_raw
WHERE `MMSI` IS NOT NULL AND `LATITUDE` IS NOT NULL AND `LONGITUDE` IS NOT NULL
EMIT CHANGES;

CREATE STREAM IF NOT EXISTS history_static WITH (
  KAFKA_TOPIC = 'ais.history.static',
  KEY_FORMAT = 'KAFKA',
  VALUE_FORMAT = 'AVRO',
  VALUE_AVRO_SCHEMA_FULL_NAME = 'com.sdu.portcongestion.AisStatic'
) AS
SELECT
  `mmsi_key`,
  CAST(`MMSI` AS BIGINT) AS `mmsi`,
  PARSE_TIMESTAMP(`TIMESTAMP`, 'dd/MM/yyyy HH:mm:ss') AS `event_time`,
  FROM_UNIXTIME(ROWTIME) AS `received_time`,
  CASE WHEN `IMO` IN ('', 'Unknown', 'Undefined') THEN NULL ELSE CAST(`IMO` AS BIGINT) END AS `imo`,
  CASE WHEN `CALLSIGN` IN ('', 'Unknown', 'Undefined') THEN NULL ELSE `CALLSIGN` END AS `call_sign`,
  CASE WHEN `NAME` IN ('', 'Unknown', 'Undefined') THEN NULL ELSE `NAME` END AS `name`,
  CAST(NULL AS INT) AS `ship_type`,
  CASE WHEN `LENGTH` IN ('', 'Unknown', 'Undefined') THEN NULL ELSE CAST(`LENGTH` AS DOUBLE) END AS `length_m`,
  CASE WHEN `WIDTH` IN ('', 'Unknown', 'Undefined') THEN NULL ELSE CAST(`WIDTH` AS DOUBLE) END AS `beam_m`,
  CASE WHEN `DRAUGHT` IN ('', 'Unknown', 'Undefined') THEN NULL ELSE CAST(`DRAUGHT` AS DOUBLE) END AS `draught_m`,
  CASE WHEN `DESTINATION` IN ('', 'Unknown', 'Undefined') THEN NULL ELSE `DESTINATION` END AS `destination`,
  CASE WHEN `ETA` IN ('', 'Unknown') THEN NULL ELSE PARSE_TIMESTAMP(`ETA`, 'dd/MM/yyyy HH:mm:ss') END AS `eta_broadcast`,
  'dma' AS `source`
FROM history_raw
WHERE `MMSI` IS NOT NULL
  -- Real static values only. ship_type is not used: DMA fills it in on every row.
  AND ((`IMO` IS NOT NULL AND `IMO` NOT IN ('', 'Unknown'))
    OR (`DESTINATION` IS NOT NULL AND `DESTINATION` NOT IN ('', 'Unknown'))
    OR (`ETA` IS NOT NULL AND `ETA` NOT IN ('', 'Unknown')))
EMIT CHANGES;
