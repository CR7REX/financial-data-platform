-- Data quality monitoring for stock prices
-- Tracks late arrivals, duplicates, and anomalies

with price_stats as (
    select
        symbol,
        date_trunc('day', recorded_at) as date,
        
        -- Basic counts
        count(*) as record_count,
        count(distinct recorded_at) as unique_timestamps,
        
        -- Time range
        min(recorded_at) as first_record,
        max(recorded_at) as last_record,
        
        -- Price validation
        min(price) as min_price,
        max(price) as max_price,
        avg(price) as avg_price,
        
        -- Detect gaps (> 5 minutes between records)
        max(
            extract(epoch from (recorded_at - lag(recorded_at) over (partition by symbol order by recorded_at)))
        ) / 60 as max_gap_minutes,
        
        -- Anomaly detection (price change > 10% in single tick)
        count(case when abs(change_percent) > 10 then 1 end) as large_moves
        
    from {{ ref('stg_stock_prices') }}
    where recorded_at >= now() - interval '7 days'
    group by symbol, date_trunc('day', recorded_at)
),

quality_flags as (
    select
        *,
        -- Data completeness check
        case 
            when record_count < 100 then 'LOW_VOLUME'
            when record_count >= 100 and record_count < 390 then 'PARTIAL'  -- 6.5 hours * 60 minutes
            else 'COMPLETE'
        end as completeness_status,
        
        -- Gap detection
        case 
            when max_gap_minutes > 5 then 'GAP_DETECTED'
            else 'CONTINUOUS'
        end as continuity_status,
        
        -- Anomaly flag
        case 
            when large_moves > 0 then 'ANOMALY_DETECTED'
            else 'NORMAL'
        end as anomaly_status,
        
        -- Lag detection (data delay > 1 hour)
        case 
            when extract(epoch from (current_timestamp - last_record)) / 3600 > 1 
            then 'STALE_DATA'
            else 'FRESH'
        end as freshness_status
        
    from price_stats
)

select
    symbol,
    date,
    record_count,
    unique_timestamps,
    first_record,
    last_record,
    round(min_price::numeric, 4) as min_price,
    round(max_price::numeric, 4) as max_price,
    round(avg_price::numeric, 4) as avg_price,
    round(max_gap_minutes::numeric, 2) as max_gap_minutes,
    large_moves,
    completeness_status,
    continuity_status,
    anomaly_status,
    freshness_status,
    current_timestamp as calculated_at
from quality_flags
order by date desc, symbol