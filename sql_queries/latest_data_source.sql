--From the two most commonly appearing regions, which is the latest datasource?
WITH region_data_cte
AS (SELECT
      t.region_id,
      COUNT(t.region_id) number_of_trips,
      ROW_NUMBER() OVER (ORDER BY COUNT(t.region_id) DESC) rownumber,
      MAX(event_timestamp) event_timestamp
    FROM trips.fact_trips t
    GROUP BY t.region_id)
SELECT
  dr.region,
  MAX(dd.datasource),
  MAX(t.event_timestamp)
FROM region_data_cte c
    INNER JOIN trips.fact_trips t
      ON c.region_id = t.region_id
      AND c.event_timestamp = t.event_timestamp
    INNER JOIN trips.dim_region dr
      ON dr.region_id = c.region_id
    INNER JOIN trips.dim_datasource dd
      ON dd.datasource_id = t.datasource_id
WHERE c.rownumber <= 2
GROUP BY dr.region