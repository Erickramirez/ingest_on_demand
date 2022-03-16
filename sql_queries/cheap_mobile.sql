--What regions has the "cheap_mobile" datasource appeared in?
SELECT DISTINCT
  region
FROM trips.dim_datasource dd
    INNER JOIN trips.fact_trips t
      ON t.datasource_id = dd.datasource_id
    INNER JOIN trips.dim_region dr
      ON dr.region_id = t.region_id
WHERE datasource = 'cheap_mobile'