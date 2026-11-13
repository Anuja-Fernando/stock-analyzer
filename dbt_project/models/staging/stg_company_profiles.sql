with ranked as (

    select
        *,
        row_number() over (partition by ticker order by snapshot_date desc) as rn
    from {{ source('stocks_raw', 'company_profiles') }}

)

select
    ticker, short_name, sector, industry, currency, market_cap, beta, snapshot_date
from ranked
where rn = 1