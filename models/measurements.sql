with measurements as (

    select * from {{ ref('stg_measurements') }}

)

select * from measurements