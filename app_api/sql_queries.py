regions = "SELECT * FROM trips.dim_region"
datasources = "SELECT * FROM trips.dim_datasource"
last_log_event = """
    SELECT
      event,
      event_type,
      to_char(execution_date_time, 'YYYY-MM-DD HH:mm:ss') 
    FROM logs.trips_log
    ORDER BY log_id DESC
    LIMIT 1
"""

get_trips_base_on_point = """
    SELECT
      r.region,
      to_char(t.event_date, 'YYYY-MM-DD') event_date,
      t.number_of_trips
    FROM trips.fact_group_trips t
        INNER JOIN trips.dim_region r
          ON r.region_id = t.region_id
    WHERE box_coord && box(point(%(latitude)s,%(longitude)s),point(%(latitude)s,%(longitude)s))
"""
