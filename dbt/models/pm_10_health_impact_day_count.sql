with measurements as (select * from {{ ref("pm_10_max_by_day") }})

select *,
good + satisfactory + moderately_polutted + poor + very_poor + severe as total_days_measured ,
round(good/(good + satisfactory + moderately_polutted + poor + very_poor + severe)*100 ,2) good_pct,
round(satisfactory/(good + satisfactory + moderately_polutted + poor + very_poor + severe)*100,2)  satisfactory_pct,
round(moderately_polutted/(good + satisfactory + moderately_polutted + poor + very_poor + severe) *100,2) moderately_polutted_pct,
round(poor/(good + satisfactory + moderately_polutted + poor + very_poor + severe)*100,2)  poor_pct,
round(very_poor/(good + satisfactory + moderately_polutted + poor + very_poor + severe)*100,2)  very_poor_pct,
round(severe/(good + satisfactory + moderately_polutted + poor + very_poor + severe) *100,2) severe_pct
from (
    select
    extract (year from date) year,
    station,
    count(case when PM10_max <= 50 then 1 else null end) good,
    count(case when PM10_max > 50 and PM10_max <= 100 then 1 else null end) satisfactory ,
    count(case when PM10_max > 101 and PM10_max <= 250 then 1 else null end) moderately_polutted,
    count(case when PM10_max > 251 and PM10_max <= 350 then 1 else null end) poor,
    count(case when PM10_max > 351 and PM10_max <= 430 then 1 else null end) very_poor,
    count(case when PM10_max > 431 then 1 else null end) severe
    from measurements
    group by year, station)
where good + satisfactory + moderately_polutted + poor + very_poor + severe >=300