"""
AWS Glue job: Load Silver layer Parquet data into Amazon Redshift.

Uses COPY command via S3 staging for high-throughput bulk loads.
Performs upsert (merge) on transaction_id to handle late-arriving updates.
"""

import logging
import sys

import boto3
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STAGING_TABLE = "staging_transactions"
TARGET_TABLE = "fact_transactions"

MERGE_SQL = f"""
BEGIN;

-- Upsert: update existing rows, insert new ones
UPDATE {TARGET_TABLE}
SET
    amount           = s.amount,
    amount_usd       = s.amount_usd,
    status           = s.status,
    updated_at       = s.updated_at,
    exchange_rate    = s.exchange_rate,
    is_high_value    = s.is_high_value
FROM {STAGING_TABLE} s
WHERE {TARGET_TABLE}.transaction_id = s.transaction_id;

INSERT INTO {TARGET_TABLE}
SELECT s.*
FROM {STAGING_TABLE} s
LEFT JOIN {TARGET_TABLE} t ON s.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL;

DROP TABLE IF EXISTS {STAGING_TABLE};

COMMIT;
"""

CREATE_STAGING_SQL = f"""
CREATE TABLE IF NOT EXISTS {STAGING_TABLE} (LIKE {TARGET_TABLE});
TRUNCATE {STAGING_TABLE};
"""


def get_secret(secret_name: str, region: str = "us-east-1") -> dict:
    client = boto3.client("secretsmanager", region_name=region)
    response = client.get_secret_value(SecretId=secret_name)
    import json

    return json.loads(response["SecretString"])


def copy_to_staging(glue_ctx, silver_path: str, redshift_conn: str, iam_role: str):
    """Write Silver data to Redshift staging table via Glue's JDBC connector."""
    dynamic_frame = glue_ctx.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [silver_path], "recurse": True},
        format="parquet",
    )

    glue_ctx.write_dynamic_frame.from_jdbc_conf(
        frame=dynamic_frame,
        catalog_connection=redshift_conn,
        connection_options={
            "dbtable": STAGING_TABLE,
            "database": "financial_dw",
            "preactions": CREATE_STAGING_SQL,
        },
        redshift_tmp_dir="s3://tmp-bucket/redshift-tmp/",
    )
    logger.info("Copied %d records to staging table", dynamic_frame.count())


def run_merge(secret_name: str):
    """Execute the upsert SQL against Redshift via psycopg2."""
    import psycopg2

    creds = get_secret(secret_name)
    conn = psycopg2.connect(
        host=creds["host"],
        port=creds["port"],
        dbname=creds["dbname"],
        user=creds["username"],
        password=creds["password"],
        sslmode="require",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(MERGE_SQL)
        conn.commit()
        logger.info("Merge to %s complete", TARGET_TABLE)
    finally:
        conn.close()


def main():
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "silver_bucket",
            "redshift_connection",
            "redshift_secret",
            "iam_role",
        ],
    )

    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    silver_path = f"s3://{args['silver_bucket']}/silver/transactions/"
    copy_to_staging(glue_ctx, silver_path, args["redshift_connection"], args["iam_role"])
    run_merge(args["redshift_secret"])

    job.commit()


if __name__ == "__main__":
    main()
