-- Monthly Balance Sheet Summary
-- Aggregates settled transactions into a simplified balance sheet per account.
-- Partition elimination via transaction_date ensures queries scan only needed data.

CREATE OR REPLACE VIEW vw_monthly_balance_sheet AS
WITH monthly_settled AS (
    SELECT
        account_id,
        DATE_TRUNC('month', transaction_date)::DATE AS report_month,
        SUM(CASE WHEN transaction_type = 'CREDIT'  THEN amount_usd ELSE 0 END) AS gross_income,
        SUM(CASE WHEN transaction_type = 'DEBIT'   THEN amount_usd ELSE 0 END) AS gross_expenses,
        SUM(CASE WHEN transaction_type = 'TRANSFER' AND amount_usd > 0 THEN amount_usd ELSE 0 END) AS transfers_in,
        SUM(CASE WHEN transaction_type = 'TRANSFER' AND amount_usd < 0 THEN ABS(amount_usd) ELSE 0 END) AS transfers_out,
        SUM(CASE WHEN transaction_type = 'REFUND'  THEN amount_usd ELSE 0 END) AS refunds_received,
        COUNT(DISTINCT transaction_id)  AS total_transactions,
        COUNT(DISTINCT merchant_id)     AS unique_merchants,
        MAX(amount_usd)                 AS largest_single_transaction,
        AVG(amount_usd)                 AS avg_transaction_amount
    FROM fact_transactions
    WHERE status = 'SETTLED'
    GROUP BY account_id, DATE_TRUNC('month', transaction_date)::DATE
),
with_net AS (
    SELECT
        *,
        (gross_income + transfers_in + refunds_received)
            - (gross_expenses + transfers_out) AS net_position,
        gross_income - gross_expenses AS operating_net
    FROM monthly_settled
),
with_mom AS (
    SELECT
        *,
        LAG(net_position) OVER (
            PARTITION BY account_id ORDER BY report_month
        ) AS prev_month_net_position,
        LAG(gross_expenses) OVER (
            PARTITION BY account_id ORDER BY report_month
        ) AS prev_month_expenses
    FROM with_net
)
SELECT
    account_id,
    report_month,
    gross_income,
    gross_expenses,
    transfers_in,
    transfers_out,
    refunds_received,
    net_position,
    operating_net,
    total_transactions,
    unique_merchants,
    largest_single_transaction,
    ROUND(avg_transaction_amount, 2) AS avg_transaction_amount,
    ROUND(
        ((net_position - COALESCE(prev_month_net_position, net_position))
         / NULLIF(ABS(COALESCE(prev_month_net_position, net_position)), 0)) * 100,
        2
    ) AS mom_net_position_change_pct,
    ROUND(
        ((gross_expenses - COALESCE(prev_month_expenses, gross_expenses))
         / NULLIF(COALESCE(prev_month_expenses, gross_expenses), 0)) * 100,
        2
    ) AS mom_expense_change_pct
FROM with_mom
ORDER BY account_id, report_month DESC;
