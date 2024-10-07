select 
    null as id 
,   null as name
,   name as country_name
,   code as country_code
,   created_at
,   total_lines
,   'countries' as source
from {{ ref('stg_countries') }}

union all 

select 
    *
,   'leagues'
from {{ ref('stg_leagues') }}