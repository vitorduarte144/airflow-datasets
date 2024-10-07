{{
  config(
    materialized = 'table',
    )
}}

select 
    * replace(id::varchar as id)
,   current_timestamp as created_at
,   count(1) over (order by 0) as total_lines
from {{ source('raw', 'leagues') }}