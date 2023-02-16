with measurements as (select * from {{ ref("stg_measurements_grouped") }})

select
extract (date from dt) date,
station,
max(PM25) PM25_max
from measurements
group by date, station
order by max(PM25) desc
limit 20