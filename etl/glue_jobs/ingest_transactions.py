"""
AWS Glue job: Ingest raw financial transaction files from S3 into the Bronze layer.

Reads CSV/JSON from s3://bucket/raw/ and writes Parquet to s3://bucket/bronze/
with Glue job bookmarks enabled to process only new files on each run.
"""

import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DecimalType,
    DateType, TimestampType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",   StringType(),  False),
    StructField("account_id",       StringType(),  False),
    StructField("transaction_date", DateType(),    False),
    StructField("transaction_time", StringType(),  False),
    StructField("amount",           DecimalType(18, 2), False),
    StructField("currency",         StringType(),  False),
    StructField("transaction_type", StringType(),  False),
    StructField("merchant_id",      StringType(),  True),
    StructField("merchant_name",    StringType(),  True),
    StructField("category",         StringType(),  True),
    StructField("status",           StringType(),  False),
    StructField("country_code",     StringType(),  True),
    StructField("device_id",        StringType(),  True),
    StructField("ip_address",       StringType(),  True),
    StructField("created_at",       TimestampType(), False),
    StructField("updated_at",       TimestampType(), False),
])

VALID_TRANSACTION_TYPES = {"DEBIT", "CREDIT", "TRANSFER", "REFUND"}
VALID_STATUSES = {"PENDING", "SETTLED", "FLAGGED", "REVERSED", "FAILED"}


def mask_pii(df):
    """Hash account_id and ip_address using SHA-256 to remove PII."""
    return df.withColumn(
        "account_id", F.sha2(F.col("account_id"), 256)
    ).withColumn(
        "ip_address", F.when(
            F.col("ip_address").isNotNull(),
            F.sha2(F.col("ip_address"), 256)
        )
    )


def add_ingestion_metadata(df, source_path: str):
    return df.withColumn(
        "ingestion_timestamp", F.current_timestamp()
    ).withColumn(
        "source_file", F.lit(source_path)
    ).withColumn(
        "ingestion_date", F.current_date()
    )


def validate_required_columns(df, job_id: str):
    required = ["transaction_id", "account_id", "transaction_date", "amount", "currency"]
    null_counts = df.select([
        F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in required
    ]).collect()[0].asDict()

    for col_name, null_count in null_counts.items():
        if null_count > 0:
            logger.warning(
                "job_id=%s column=%s null_count=%d — rows will be quarantined",
                job_id, col_name, null_count
            )

    valid_df = df.filter(
        F.col("transaction_id").isNotNull() &
        F.col("account_id").isNotNull() &
        F.col("amount").isNotNull() &
        F.col("transaction_type").isin(VALID_TRANSACTION_TYPES) &
        F.col("status").isin(VALID_STATUSES)
    )
    quarantine_df = df.subtract(valid_df)
    return valid_df, quarantine_df


def main():
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME", "source_bucket", "bronze_bucket", "quarantine_bucket"
    ])

    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    source_path = f"s3://{args['source_bucket']}/raw/transactions/"
    bronze_path = f"s3://{args['bronze_bucket']}/bronze/transactions/"
    quarantine_path = f"s3://{args['quarantine_bucket']}/quarantine/transactions/"

    logger.info("Reading from %s", source_path)

    raw_df = spark.read.schema(TRANSACTION_SCHEMA).option(
        "header", "true"
    ).option(
        "mode", "PERMISSIVE"
    ).csv(source_path)

    logger.info("Ingested %d raw records", raw_df.count())

    masked_df = mask_pii(raw_df)
    enriched_df = add_ingestion_metadata(masked_df, source_path)
    valid_df, quarantine_df = validate_required_columns(enriched_df, args["JOB_NAME"])

    valid_df.write.mode("append").partitionBy(
        "transaction_date"
    ).parquet(bronze_path)

    if quarantine_df.count() > 0:
        quarantine_df.write.mode("append").partitionBy(
            "ingestion_date"
        ).parquet(quarantine_path)
        logger.warning("Quarantined %d records", quarantine_df.count())

    logger.info("Wrote %d valid records to %s", valid_df.count(), bronze_path)
    job.commit()


if __name__ == "__main__":
    main()
