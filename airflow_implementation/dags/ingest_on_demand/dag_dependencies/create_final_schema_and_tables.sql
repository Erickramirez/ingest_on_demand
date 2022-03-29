CREATE SCHEMA IF NOT EXISTS {PG_FINAL_SCHEMA};

CREATE TABLE IF NOT EXISTS {PG_FINAL_SCHEMA}.dim_region(
        region_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        region VARCHAR(50));

CREATE TABLE IF NOT EXISTS {PG_FINAL_SCHEMA}.dim_datasource(
    datasource_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    datasource VARCHAR(50));

CREATE TABLE IF NOT EXISTS {PG_FINAL_SCHEMA}.dim_event_time(
        event_timestamp TIMESTAMP PRIMARY KEY,
        hour INT,
        day INT,
        week INT,
        month INT,
        year INT,
        weekday INT);

CREATE TABLE IF NOT EXISTS {PG_FINAL_SCHEMA}.fact_trips(
        event_timestamp TIMESTAMP,
        event_id bytea,
        region_id INT,
        datasource_id INT,
        origin_coord point,
        destination_coord point,
        PRIMARY KEY(event_timestamp, event_id)
    ) PARTITION BY RANGE (event_timestamp);

    CREATE TABLE IF NOT EXISTS {PG_FINAL_SCHEMA}.fact_trips_p2018_05 PARTITION OF {PG_FINAL_SCHEMA}.fact_trips
        FOR VALUES FROM ('2018-05-01 00:00:00') TO ('2018-06-01 00:00:00')
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS {PG_FINAL_SCHEMA}.fact_group_trips(
        event_date TIMESTAMP,
        region_id INT,
        box_coord BOX,
        number_of_trips INT,
        PRIMARY KEY(event_date, region_id)
    ) PARTITION BY RANGE (event_date);

CREATE TABLE IF NOT EXISTS {PG_FINAL_SCHEMA}.fact_group_trips_p2018_05 PARTITION OF {PG_FINAL_SCHEMA}.fact_group_trips
    FOR VALUES FROM ('2018-05-01 00:00:00') TO ('2018-06-01 00:00:00')
TABLESPACE pg_default;