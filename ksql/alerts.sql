-- F13: threshold alerts on the port outlook.
-- Reads port.outlook, writes alerts. ksqlDB upper-cases the column names it reads from the Avro
-- schema, so input columns are written `LATE_COUNT` and so on. Output names are lower case in
-- backticks (AS `metric`), which matches schemas/avro/alerts.avsc.
--
-- The numbers below repeat alerts.warning and alerts.critical in config/thresholds.yaml, because
-- ksqlDB cannot read that file. Keep them in sync (TODO(F13): render this file from the YAML).
--   warning:  late_count >= 3  or expected_waiting_ships >= 3
--   critical: late_count >= 6  or expected_waiting_ships >= 6

CREATE STREAM IF NOT EXISTS outlook_in (
  `port_key` STRING KEY
) WITH (
  KAFKA_TOPIC = 'port.outlook',
  KEY_FORMAT = 'KAFKA',
  VALUE_FORMAT = 'AVRO'
);

CREATE STREAM IF NOT EXISTS alerts_out WITH (
  KAFKA_TOPIC = 'alerts',
  KEY_FORMAT = 'KAFKA',
  VALUE_FORMAT = 'AVRO',
  VALUE_AVRO_SCHEMA_FULL_NAME = 'com.sdu.portcongestion.Alert'
) AS
SELECT
  `port_key`,
  `PORT` AS `port`,
  -- Level, metric and threshold all come from the rule that fired, checked from critical to
  -- warning and late_count before expected_waiting_ships, so the message names the real cause.
  CASE WHEN `LATE_COUNT` >= 6 OR `EXPECTED_WAITING_SHIPS` >= 6 THEN 'critical' ELSE 'warning' END
    AS `alert_level`,
  `SLOT_START` AS `slot_start`,
  CASE
    WHEN `LATE_COUNT` >= 6 THEN 'late_count'
    WHEN `EXPECTED_WAITING_SHIPS` >= 6 THEN 'expected_waiting_ships'
    WHEN `LATE_COUNT` >= 3 THEN 'late_count'
    ELSE 'expected_waiting_ships'
  END AS `metric`,
  CASE
    WHEN `LATE_COUNT` >= 6 THEN CAST(`LATE_COUNT` AS DOUBLE)
    WHEN `EXPECTED_WAITING_SHIPS` >= 6 THEN `EXPECTED_WAITING_SHIPS`
    WHEN `LATE_COUNT` >= 3 THEN CAST(`LATE_COUNT` AS DOUBLE)
    ELSE `EXPECTED_WAITING_SHIPS`
  END AS `value`,
  CASE WHEN `LATE_COUNT` >= 6 OR `EXPECTED_WAITING_SHIPS` >= 6 THEN CAST(6 AS DOUBLE)
    ELSE CAST(3 AS DOUBLE) END AS `threshold`,
  CONCAT(
    `PORT`, ': ',
    CASE
      WHEN `LATE_COUNT` >= 6 THEN CONCAT('late ships = ', CAST(`LATE_COUNT` AS STRING))
      WHEN `EXPECTED_WAITING_SHIPS` >= 6
        THEN CONCAT('expected waiting ships = ', CAST(`EXPECTED_WAITING_SHIPS` AS STRING))
      WHEN `LATE_COUNT` >= 3 THEN CONCAT('late ships = ', CAST(`LATE_COUNT` AS STRING))
      ELSE CONCAT('expected waiting ships = ', CAST(`EXPECTED_WAITING_SHIPS` AS STRING))
    END
  ) AS `message`,
  FROM_UNIXTIME(ROWTIME) AS `created_at`
FROM outlook_in
WHERE `LATE_COUNT` >= 3 OR `EXPECTED_WAITING_SHIPS` >= 3
EMIT CHANGES;
