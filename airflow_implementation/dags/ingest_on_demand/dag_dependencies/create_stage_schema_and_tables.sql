CREATE SCHEMA IF NOT EXISTS {PG_STAGE_SCHEMA};

DROP TABLE IF EXISTS {PG_STAGE_SCHEMA}.trips;

CREATE TABLE {PG_STAGE_SCHEMA}.trips(
    region VARCHAR(50),
    origin_coord VARCHAR(255),
    destination_coord VARCHAR(255),
    datetime  timestamp,
    datasource VARCHAR(50));


DROP TABLE IF EXISTS {PG_STAGE_SCHEMA}.fact_delta_trips;

CREATE TABLE IF NOT EXISTS {PG_STAGE_SCHEMA}.fact_delta_trips(
    event_timestamp TIMESTAMP,
    event_id bytea,
    region_id INT,
    datasource_id INT,
    origin_coord point,
    destination_coord point,
    PRIMARY KEY(event_timestamp, event_id)
);