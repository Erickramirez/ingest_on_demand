class DMLScripts:
    datasource_table_insert = """
        WITH dim_datasource_cte AS (
            SELECT DISTINCT datasource
            FROM {PG_STAGE_SCHEMA}.trips
        )
        INSERT INTO {PG_FINAL_SCHEMA}.dim_datasource
         (datasource)
        SELECT dc.datasource FROM dim_datasource_cte dc
            LEFT OUTER JOIN {PG_FINAL_SCHEMA}.dim_datasource d
                ON d.datasource = dc.datasource
        WHERE d.datasource IS NULL
    """

    region_table_insert = """
        WITH dim_region_cte AS (
            SELECT DISTINCT region
            FROM {PG_STAGE_SCHEMA}.trips
        )
        INSERT INTO {PG_FINAL_SCHEMA}.dim_region
         (region)
        SELECT dc.region FROM dim_region_cte dc
            LEFT OUTER JOIN {PG_FINAL_SCHEMA}.dim_region d
                ON d.region = dc.region
        WHERE d.region IS NULL
    """

    time_table_insert = """
        WITH dim_time_cte AS (
            SELECT DISTINCT datetime AS event_timestamp
            FROM {PG_STAGE_SCHEMA}.trips
            UNION
            SELECT DISTINCT datetime::date::timestamp AS event_timestamp
            FROM {PG_STAGE_SCHEMA}.trips
        )
        INSERT INTO {PG_FINAL_SCHEMA}.dim_event_time (
            event_timestamp,
            hour,
            day,
            week,
            month,
            year,
            weekday
        )
        SELECT DISTINCT 
            dc.event_timestamp,
            EXTRACT(hour from dc.event_timestamp) as hour,
            EXTRACT(day from dc.event_timestamp) as day,
            EXTRACT(week from dc.event_timestamp) as week,
            EXTRACT(month from dc.event_timestamp) as month,
            EXTRACT(year from dc.event_timestamp) as year,
            EXTRACT(isodow from dc.event_timestamp) as weekday
        FROM dim_time_cte dc
            LEFT OUTER JOIN {PG_FINAL_SCHEMA}.dim_event_time d
                ON d.event_timestamp = dc.event_timestamp
        WHERE d.event_timestamp IS NULL """

    fact_delta_trips_table_insert = """
        TRUNCATE TABLE {PG_STAGE_SCHEMA}.fact_delta_trips;
        INSERT INTO  {PG_STAGE_SCHEMA}.fact_delta_trips(
            event_timestamp,
            event_id,
            region_id,
            datasource_id,
            origin_coord,
            destination_coord
        )
        SELECT 
            datetime,
            decode(md5(concat(t.region,'-',t.datasource,'-', t.origin_coord,'-',t.destination_coord)), 'hex'),
            dr.region_id,
            dd.datasource_id,
            point(split_part(REPLACE(REPLACE(origin_coord, 'POINT (',''),')',''), ' ',1)::float,
            split_part(REPLACE(REPLACE(origin_coord, 'POINT (',''),')',''), ' ',2)::float),
            point(split_part(REPLACE(REPLACE(destination_coord, 'POINT (',''),')',''), ' ',1)::float,
            split_part(REPLACE(REPLACE(destination_coord, 'POINT (',''),')',''), ' ',2)::float)  
        FROM {PG_STAGE_SCHEMA}.trips t
        INNER JOIN {PG_FINAL_SCHEMA}.dim_region dr
            ON dr.region = t.region
        INNER JOIN {PG_FINAL_SCHEMA}.dim_datasource dd
            ON dd.datasource = t.datasource
    """

    fact_trips_table_insert = """
        INSERT INTO  {PG_FINAL_SCHEMA}.fact_trips(
            event_timestamp,
            event_id,
            region_id,
            datasource_id,
            origin_coord,
            destination_coord
        )
        SELECT 
            event_timestamp,
            event_id,
            region_id,
            datasource_id,
            origin_coord,
            destination_coord
        FROM {PG_STAGE_SCHEMA}.fact_delta_trips
        ON CONFLICT DO NOTHING
    """

    fact_group_trips_table_insert = """
        TRUNCATE TABLE {PG_FINAL_SCHEMA}.fact_group_trips;
        INSERT INTO  {PG_FINAL_SCHEMA}.fact_group_trips(
            event_date,
            region_id,
            box_coord,
            number_of_trips
        )
        SELECT 
            event_timestamp::date::timestamp,
            region_id,
            box(point(MIN(CASE WHEN origin_coord[0] <destination_coord[0] THEN origin_coord[0] ELSE destination_coord[0] END  ), 
                      MIN(CASE WHEN origin_coord[1] <destination_coord[1] THEN origin_coord[1] ELSE destination_coord[1] END  )), 
                point(MAX(CASE WHEN origin_coord[0] <destination_coord[0] THEN origin_coord[0] ELSE destination_coord[0] END  ), 
                      MAX(CASE WHEN origin_coord[1] <destination_coord[1] THEN origin_coord[1] ELSE destination_coord[1] END  ))),
            count(*)
        FROM {PG_FINAL_SCHEMA}.fact_trips
        GROUP BY event_timestamp::date::timestamp,
            region_id
    
    """
