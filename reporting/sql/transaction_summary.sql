-- Transaction Summary Report
-- Provides executive-level KPIs: volume, value, fraud rate, top categories.

CREATE OR REPLACE VIEW vw_transaction_summary AS
WITH base AS (
    SELECT
        transaction_date,
        transaction_type,
        category,
        status,
        is_international,
        is_high_value,
        amount_usd,
        currency
    FROM fact_transactions
    WHERE transaction_date >= DATEADD(day, -90, CURRENT_DATE)
),
daily_kpis AS (
    SELECT
        transaction_date,
        COUNT(*)                                                AS total_count,
        SUM(amount_usd)                                         AS total_volume_usd,
        AVG(amount_usd)                                         AS avg_amount_usd,
        MAX(amount_usd)                                         AS max_amount_usd,
        COUNT(CASE WHEN status = 'FLAGGED'  THEN 1 END)         AS flagged_count,
        COUNT(CASE WHEN status = 'REVERSED' THEN 1 END)         AS reversed_count,
        COUNT(CASE WHEN is_international    THEN 1 END)         AS international_count,
        COUNT(CASE WHEN is_high_value       THEN 1 END)         AS high_value_count,
        COUNT(DISTINCT currency)                                AS currency_count,
        ROUND(
            COUNT(CASE WHEN status = 'FLAGGED' THEN 1 END)::FLOAT
            / NULLIF(COUNT(*), 0) * 100, 3
        )                                                       AS fraud_rate_pct
    FROM base
    GROUP BY transaction_date
),
category_rank AS (
    SELECT
        transaction_date,
        category,
        SUM(amount_usd) AS category_volume,
        RANK() OVER (PARTITION BY transaction_date ORDER BY SUM(amount_usd) DESC) AS rnk
    FROM base
    GROUP BY transaction_date, category
),
top_category AS (
    SELECT transaction_date, category AS top_category, category_volume AS top_category_volume
    FROM category_rank
    WHERE rnk = 1
)
SELECT
    k.transaction_date,
    k.total_count,
    ROUND(k.total_volume_usd, 2)     AS total_volume_usd,
    ROUND(k.avg_amount_usd, 2)       AS avg_amount_usd,
    ROUND(k.max_amount_usd, 2)       AS max_amount_usd,
    k.flagged_count,
    k.reversed_count,
    k.international_count,
    k.high_value_count,
    k.currency_count,
    k.fraud_rate_pct,
    t.top_category,
    ROUND(t.top_category_volume, 2)  AS top_category_volume,
    -- 7-day rolling averages
    ROUND(AVG(k.total_volume_usd) OVER (
        ORDER BY k.transaction_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2) AS rolling_7d_avg_volume,
    ROUND(AVG(k.fraud_rate_pct) OVER (
        ORDER BY k.transaction_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 3) AS rolling_7d_fraud_rate
FROM daily_kpis k
LEFT JOIN top_category t ON k.transaction_date = t.transaction_date
ORDER BY k.transaction_date DESC;


-- Convenience query: last 30-day executive summary (single-row)
CREATE OR REPLACE VIEW vw_executive_summary_30d AS
SELECT
    COUNT(*)                                                  AS total_transactions,
    ROUND(SUM(amount_usd), 2)                                 AS total_volume_usd,
    ROUND(AVG(amount_usd), 2)                                 AS avg_transaction_usd,
    COUNT(CASE WHEN status = 'FLAGGED'  THEN 1 END)           AS total_flagged,
    COUNT(CASE WHEN status = 'REVERSED' THEN 1 END)           AS total_reversed,
    ROUND(
        COUNT(CASE WHEN status = 'FLAGGED' THEN 1 END)::FLOAT
        / NULLIF(COUNT(*), 0) * 100, 3
    )                                                         AS fraud_rate_pct,
    COUNT(CASE WHEN is_international THEN 1 END)              AS international_transactions,
    COUNT(DISTINCT account_id)                                AS active_accounts
FROM fact_transactions
WHERE transaction_date >= DATEADD(day, -30, CURRENT_DATE)
  AND status != 'FAILED';
