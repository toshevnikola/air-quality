with measurements as (select * from {{ ref("stg_measurements_grouped") }})

SELECT 
station,
EXTRACT(DATE FROM dt) date, 
max(PM10) PM10_max
FROM measurements
GROUP BY date, station
ORDER BY date desc, station 