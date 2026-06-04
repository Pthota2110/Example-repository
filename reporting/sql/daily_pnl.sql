-- Daily Profit & Loss Report
-- Shows net cash flow per account per day, broken out by transaction type.
-- Materialized as a Redshift view; QuickSight queries this directly.

CREATE OR REPLACE VIEW vw_daily_pnl AS
WITH daily_flows AS (
    SELECT
        account_id,
        transaction_date,
        category,
        SUM(CASE WHEN transaction_type = 'CREDIT'  THEN amount_usd ELSE 0 END) AS total_credits,
        SUM(CASE WHEN transaction_type = 'DEBIT'   THEN amount_usd ELSE 0 END) AS total_debits,
        SUM(CASE WHEN transaction_type = 'REFUND'  THEN amount_usd ELSE 0 END) AS total_refunds,
        SUM(CASE WHEN transaction_type = 'TRANSFER' THEN amount_usd ELSE 0 END) AS total_transfers,
        COUNT(*)                                                                  AS transaction_count,
        COUNT(CASE WHEN status = 'FLAGGED'  THEN 1 END)                          AS flagged_count,
        COUNT(CASE WHEN status = 'REVERSED' THEN 1 END)                          AS reversed_count
    FROM fact_transactions
    WHERE status NOT IN ('FAILED', 'PENDING')
    GROUP BY account_id, transaction_date, category
),
net_pnl AS (
    SELECT
        *,
        (total_credits - total_debits + total_refunds) AS net_flow_usd,
        ROUND(
            (total_debits / NULLIF(total_credits + total_debits, 0)) * 100, 2
        ) AS debit_ratio_pct
    FROM daily_flows
),
rolling_7d AS (
    SELECT
        account_id,
        transaction_date,
        SUM(net_flow_usd) OVER (
            PARTITION BY account_id
            ORDER BY transaction_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS rolling_7d_net_flow
    FROM net_pnl
    GROUP BY account_id, transaction_date, net_flow_usd
)
SELECT
    p.account_id,
    p.transaction_date,
    p.category,
    p.total_credits,
    p.total_debits,
    p.total_refunds,
    p.total_transfers,
    p.net_flow_usd,
    p.debit_ratio_pct,
    p.transaction_count,
    p.flagged_count,
    p.reversed_count,
    r.rolling_7d_net_flow,
    CASE
        WHEN p.net_flow_usd > 0 THEN 'NET_POSITIVE'
        WHEN p.net_flow_usd < 0 THEN 'NET_NEGATIVE'
        ELSE 'BREAKEVEN'
    END AS pnl_status
FROM net_pnl p
JOIN rolling_7d r
    ON p.account_id = r.account_id
    AND p.transaction_date = r.transaction_date;
