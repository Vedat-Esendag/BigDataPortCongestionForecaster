-- F13: threshold alerts on the port outlook.
-- Reads port.outlook, writes alerts. Identifiers are in backticks so ksqlDB keeps them lower case,
-- which matches schemas/avro/alerts.avsc.
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
  `port` AS `port`,
  CASE
    WHEN `late_count` >= 6 OR `expected_waiting_ships` >= 6 THEN 'critical'
    ELSE 'warning'
  END AS `alert_level`,
  `slot_start` AS `slot_start`,
  CASE WHEN `late_count` >= 3 THEN 'late_count' ELSE 'expected_waiting_ships' END AS `metric`,
  CASE WHEN `late_count` >= 3 THEN CAST(`late_count` AS DOUBLE) ELSE `expected_waiting_ships` END AS `value`,
  CAST(3 AS DOUBLE) AS `threshold`,
  CONCAT(
    `port`, ': ',
    CASE WHEN `late_count` >= 3 THEN 'late ships = ' ELSE 'expected waiting ships = ' END,
    CASE WHEN `late_count` >= 3 THEN CAST(`late_count` AS STRING) ELSE CAST(`expected_waiting_ships` AS STRING) END
  ) AS `message`,
  FROM_UNIXTIME(ROWTIME) AS `created_at`
FROM outlook_in
WHERE `late_count` >= 3 OR `expected_waiting_ships` >= 3
EMIT CHANGES;
