-- Unpivot wide format into long format (one row per date + ticker)
WITH source AS (
    SELECT * FROM {{ source('ingest', 'raw_historical_prices_wide') }}
),

unpivoted AS (
    SELECT PRICE_DATE, 'SPY' AS ticker, SPY AS price FROM source
    UNION ALL
    SELECT PRICE_DATE, 'AGG' AS ticker, AGG AS price FROM source
    UNION ALL
    SELECT PRICE_DATE, 'GLD' AS ticker, GLD AS price FROM source
    UNION ALL
    SELECT PRICE_DATE, 'IWM' AS ticker, IWM AS price FROM source
    UNION ALL
    SELECT PRICE_DATE, 'QQQ' AS ticker, QQQ AS price FROM source
    UNION ALL
    SELECT PRICE_DATE, 'TLT' AS ticker, TLT AS price FROM source
    UNION ALL
    SELECT PRICE_DATE, 'VTI' AS ticker, VTI AS price FROM source
)

SELECT
    PRICE_DATE,
    ticker,
    price
FROM unpivoted
WHERE price IS NOT NULL
ORDER BY PRICE_DATE, ticker
