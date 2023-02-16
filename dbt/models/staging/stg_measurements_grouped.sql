{{ config(materialized='view') }}

select dt, station, max(SO2) SO2, max(CO) CO, max(PM10) PM10, max(PM25) PM25, max(NO2) NO2, max(O3) O3 from (
select dt,station,
       case
         when parameter = 'SO2' then value
       end as SO2,
       case
         when parameter = 'CO' then value
       end as CO,
       case
         when parameter = 'PM10' then value
       end as PM10,
       case
         when parameter = 'PM25' then value
       end as PM25,
       case
         when parameter = 'NO2' then value
       end as NO2,
       case
         when parameter = 'O3' then value
       end as O3
       
from {{ ref("stg_measurements") }}
) 
group by dt, station
order by dt, station