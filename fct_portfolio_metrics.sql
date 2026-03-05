WITH daily_returns AS (
    SELECT
        price_date,
        ticker,
        price,
        LAG(price) OVER (
            PARTITION BY ticker ORDER BY price_date
        ) AS prev_price,
        CASE
            WHEN LAG(price) OVER (
                PARTITION BY ticker ORDER BY price_date
            ) IS NULL THEN NULL
            ELSE (price - LAG(price) OVER (
                PARTITION BY ticker ORDER BY price_date
            )) / LAG(price) OVER (
                PARTITION BY ticker ORDER BY price_date
            )
        END AS daily_return
    FROM {{ ref('stg_prices') }}
),

metrics AS (
    SELECT
        ticker,
        AVG(daily_return) * 252          AS annualized_return,
        STDDEV(daily_return) * SQRT(252) AS annualized_volatility,
        COUNT(*)                         AS num_days
    FROM daily_returns
    WHERE daily_return IS NOT NULL
    GROUP BY ticker
),

cumulative AS (
    SELECT
        ticker,
        price_date,
        EXP(SUM(LN(1 + COALESCE(daily_return, 0)))
            OVER (PARTITION BY ticker ORDER BY price_date)
        ) AS cumulative_value
    FROM daily_returns
),

running_max AS (
    SELECT
        ticker,
        price_date,
        cumulative_value,
        MAX(cumulative_value) OVER (
            PARTITION BY ticker ORDER BY price_date
        ) AS peak_value
    FROM cumulative
),

drawdown AS (
    SELECT
        ticker,
        MIN((cumulative_value - peak_value) / NULLIF(peak_value, 0)) AS max_drawdown
    FROM running_max
    GROUP BY ticker
)

SELECT
    m.ticker,
    ROUND(m.annualized_return * 100, 2)                               AS return_pct,
    ROUND(m.annualized_volatility * 100, 2)                           AS volatility_pct,
    ROUND((m.annualized_return - 0.02) / m.annualized_volatility, 3) AS sharpe_ratio,
    ROUND(d.max_drawdown * 100, 2)                                    AS max_drawdown_pct,
    m.num_days
FROM metrics m
JOIN drawdown d ON m.ticker = d.ticker
ORDER BY sharpe_ratio DESC
