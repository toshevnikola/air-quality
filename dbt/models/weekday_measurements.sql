with measurements as (select * from {{ ref("stg_measurements_grouped") }})

select 
{{ dbt_date.day_of_week("dt") }} as day_of_week, 
avg(PM25) PM25_avg, max(PM25) PM25_max,
avg(PM10) PM10_avg, max(PM10) PM10_max,
avg(CO) CO_avg,     max(CO) CO_max,
avg(SO2) SO2_avg,   max(SO2) SO2_max,
avg(NO2) NO2_avg,   max(NO2) NO2_max,
avg(O3) O3_avg,     max(O3) O3_max
from measurements
group by day_of_week
order by day_of_week