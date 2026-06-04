-- DDL: Core Redshift tables and schemas for the Financial Data Platform
-- Run once during environment bootstrap (Terraform calls this via a null_resource).

CREATE SCHEMA IF NOT EXISTS financial;

-- Core fact table (sort key on transaction_date for range queries)
CREATE TABLE IF NOT EXISTS financial.fact_transactions (
    transaction_id          VARCHAR(36)     NOT NULL,
    account_id              VARCHAR(64)     NOT NULL,  -- SHA-256 masked
    transaction_date        DATE            NOT NULL,
    transaction_time        VARCHAR(8),
    amount                  DECIMAL(18,2)   NOT NULL,
    amount_usd              DECIMAL(18,2),
    currency                CHAR(3)         NOT NULL,
    exchange_rate           DECIMAL(10,6),
    transaction_type        VARCHAR(20)     NOT NULL,
    merchant_id             VARCHAR(50),
    merchant_name           VARCHAR(200),
    category                VARCHAR(100),
    status                  VARCHAR(20)     NOT NULL,
    country_code            CHAR(2),
    device_id               VARCHAR(100),
    ip_address              VARCHAR(64),    -- SHA-256 masked
    day_of_week             SMALLINT,
    is_weekend              BOOLEAN,
    transaction_hour        SMALLINT,
    is_international        BOOLEAN,
    is_high_value           BOOLEAN,
    ingestion_timestamp     TIMESTAMP,
    source_file             VARCHAR(500),
    ingestion_date          DATE,
    created_at              TIMESTAMP       NOT NULL,
    updated_at              TIMESTAMP       NOT NULL,
    PRIMARY KEY (transaction_id)
)
DISTKEY (account_id)
SORTKEY (transaction_date);

-- Fraud alerts dimension
CREATE TABLE IF NOT EXISTS financial.dim_fraud_alerts (
    alert_id                VARCHAR(36)     NOT NULL DEFAULT gen_random_uuid()::VARCHAR,
    transaction_id          VARCHAR(36)     NOT NULL,
    account_id              VARCHAR(64),
    detected_at             TIMESTAMP       NOT NULL,
    action                  VARCHAR(10)     NOT NULL,
    risk_score              DECIMAL(5,4),
    triggered_rules         VARCHAR(1000),
    resolved_at             TIMESTAMP,
    resolved_by             VARCHAR(100),
    resolution_notes        VARCHAR(2000),
    PRIMARY KEY (alert_id)
)
DISTKEY (account_id)
SORTKEY (detected_at);

-- Merchant dimension
CREATE TABLE IF NOT EXISTS financial.dim_merchants (
    merchant_id             VARCHAR(50)     NOT NULL,
    merchant_name           VARCHAR(200),
    mcc_code                CHAR(4),
    category                VARCHAR(100),
    country_code            CHAR(2),
    is_high_risk            BOOLEAN         DEFAULT FALSE,
    created_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    updated_at              TIMESTAMP       DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (merchant_id)
)
DISTSTYLE ALL;

-- Date dimension (pre-populated for 5 years)
CREATE TABLE IF NOT EXISTS financial.dim_date (
    date_key                INTEGER         NOT NULL,  -- YYYYMMDD
    full_date               DATE            NOT NULL,
    year                    SMALLINT,
    quarter                 SMALLINT,
    month                   SMALLINT,
    month_name              VARCHAR(9),
    week_of_year            SMALLINT,
    day_of_week             SMALLINT,
    day_name                VARCHAR(9),
    is_weekend              BOOLEAN,
    is_us_holiday           BOOLEAN,
    fiscal_year             SMALLINT,
    fiscal_quarter          SMALLINT,
    PRIMARY KEY (date_key)
)
DISTSTYLE ALL;

GRANT SELECT ON ALL TABLES IN SCHEMA financial TO GROUP reporting_role;
GRANT SELECT ON ALL TABLES IN SCHEMA financial TO GROUP analyst_role;
