-- Intraday price movements and statistics
-- Aggregated by hour for trend analysis

with hourly_stats as (
    select
        symbol,
        date_trunc('hour', recorded_at) as hour,
        
        -- OHLC for the hour
        first_value(price) over (
            partition by symbol, date_trunc('hour', recorded_at) 
            order by recorded_at 
            rows between unbounded preceding and unbounded following
        ) as hour_open,
        max(price) as hour_high,
        min(price) as hour_low,
        last_value(price) over (
            partition by symbol, date_trunc('hour', recorded_at) 
            order by recorded_at 
            rows between unbounded preceding and unbounded following
        ) as hour_close,
        
        -- Volume and change
        max(volume) as volume,
        avg(change_percent) as avg_change_percent,
        
        -- Count of price updates
        count(*) as update_count,
        
        -- Volatility
        max(volatility_pct) as max_volatility_pct
        
    from {{ ref('stg_stock_prices') }}
    where recorded_at >= now() - interval '7 days'
    group by symbol, date_trunc('hour', recorded_at), recorded_at
),

deduplicated as (
    select distinct
        symbol,
        hour,
        hour_open,
        hour_high,
        hour_low,
        hour_close,
        volume,
        round(avg_change_percent::numeric, 4) as avg_change_percent,
        update_count,
        round(max_volatility_pct::numeric, 4) as max_volatility_pct,
        
        -- Hour-over-hour change
        hour_close - hour_open as hour_change,
        round(((hour_close - hour_open) / nullif(hour_open, 0) * 100)::numeric, 4) as hour_change_percent
        
    from hourly_stats
)

select
    *,
    current_timestamp as calculated_at
from deduplicated
order by hour desc, symbol