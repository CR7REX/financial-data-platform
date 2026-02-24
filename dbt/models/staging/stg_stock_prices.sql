-- Staging model for stock prices
-- Clean and standardize raw data from Kafka

with source as (
    select * from {{ source('raw', 'stock_prices') }}
),

staged as (
    select
        -- Unique identifier
        {{ dbt_utils.generate_surrogate_key(['symbol', 'timestamp']) }} as price_id,
        
        -- Stock info
        symbol,
        
        -- Timestamps
        timestamp as recorded_at,
        processed_at as ingested_at,
        
        -- Price data
        round(price::numeric, 4) as price,
        round(open_price::numeric, 4) as open_price,
        round(high::numeric, 4) as high,
        round(low::numeric, 4) as low,
        round(prev_close::numeric, 4) as prev_close,
        
        -- Change metrics
        round(change::numeric, 4) as price_change,
        round(change_percent::numeric, 4) as change_percent,
        
        -- Volume
        volume::bigint as volume,
        
        -- Derived metrics
        round((high - low)::numeric, 4) as daily_range,
        round(((high - low) / nullif(price, 0) * 100)::numeric, 4) as volatility_pct
        
    from source
    where 
        -- Data quality filters
        price is not null 
        and price > 0
        and timestamp is not null
        and symbol is not null
)

select * from staged