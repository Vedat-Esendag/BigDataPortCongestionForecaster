-- Keyspace and tables. Applied by the cassandra-init container (make up, storage profile).
-- Column names match the field names of the Avro schemas in schemas/avro, because the Cassandra
-- sink connector maps value.<field> to <column> (see connect/cassandra-sink.json).
-- `day` is the UTC date as yyyy-MM-dd text; it keeps partitions to one day each.
-- SimpleStrategy with factor 1 is for a single-machine demo. Use NetworkTopologyStrategy and a
-- higher factor on a real cluster.

CREATE KEYSPACE IF NOT EXISTS portcongestion
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE portcongestion;

-- Every prediction made for a port on a day, newest first (ship.prediction topic).
CREATE TABLE IF NOT EXISTS ship_prediction_by_port_day (
  port            text,
  day             text,
  event_time      timestamp,
  mmsi            bigint,
  broadcast_eta   timestamp,
  predicted_eta   timestamp,
  eta_lower       timestamp,
  eta_upper       timestamp,
  model_version   text,
  reason_codes    list<text>,
  lat             double,
  lon             double,
  processing_time timestamp,
  PRIMARY KEY ((port, day), event_time, mmsi)
) WITH CLUSTERING ORDER BY (event_time DESC, mmsi ASC);

-- Every outlook computation for a port on a day, newest first (port.outlook topic).
CREATE TABLE IF NOT EXISTS port_outlook_by_port_day (
  port                   text,
  day                    text,
  computed_at            timestamp,
  slot_start             timestamp,
  slot_end               timestamp,
  expected_arrivals      int,
  late_count             int,
  bunching_flag          boolean,
  expected_waiting_ships double,
  PRIMARY KEY ((port, day), computed_at, slot_start)
) WITH CLUSTERING ORDER BY (computed_at DESC, slot_start ASC);

-- Weather-to-delay analytics (analytics.weather_delay topic). A new run overwrites the
-- previous row for the same (port, variable, bucket).
CREATE TABLE IF NOT EXISTS weather_delay_by_port (
  port             text,
  weather_variable text,
  bucket           text,
  n_arrivals       int,
  mean_delay_min   double,
  median_delay_min double,
  p90_delay_min    double,
  period_start     timestamp,
  period_end       timestamp,
  computed_at      timestamp,
  PRIMARY KEY ((port), weather_variable, bucket)
);

-- Forecast evaluation metrics (eval.metrics topic).
CREATE TABLE IF NOT EXISTS eval_metrics_by_port_day (
  port          text,
  day           text,
  model_version text,
  horizon_hours int,
  metric        text,
  metric_value  double,
  n_samples     int,
  computed_at   timestamp,
  PRIMARY KEY ((port, day), model_version, horizon_hours, metric)
);
