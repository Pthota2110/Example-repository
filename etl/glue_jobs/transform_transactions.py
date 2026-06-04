"""
AWS Glue job: Transform Bronze → Silver layer.

Applies business rules, currency normalization, MCC category mapping,
deduplication, and enrichment. Writes cleaned Parquet to the Silver layer.
"""

import logging
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ISO 4217 approximate USD exchange rates (refreshed daily via parameter store in prod)
EXCHANGE_RATES = {
    "USD": 1.0,
    "EUR": 1.08,
    "GBP": 1.27,
    "JPY": 0.0067,
    "CAD": 0.74,
    "AUD": 0.65,
    "CHF": 1.12,
    "CNY": 0.14,
}

MCC_CATEGORY_MAP = {
    "5411": "Groceries",
    "5812": "Restaurants",
    "5541": "Gas Stations",
    "4111": "Transit",
    "7011": "Hotels",
    "4511": "Airlines",
    "5999": "Retail",
    "6011": "ATM",
    "5912": "Pharmacy",
    "7372": "Software",
    "5045": "Electronics",
    "8099": "Healthcare",
}


def normalize_currency(df):
    """Convert all transaction amounts to USD equivalent."""
    exchange_map = F.create_map(
        *[item for pair in [(F.lit(k), F.lit(v)) for k, v in EXCHANGE_RATES.items()] for item in pair]
    )
    return df.withColumn("amount_usd", F.round(F.col("amount") * exchange_map[F.col("currency")], 2)).withColumn(
        "exchange_rate", exchange_map[F.col("currency")]
    )


def enrich_category(df):
    """Map MCC codes in merchant_id prefix to human-readable categories."""
    mcc_map = F.create_map(
        *[item for pair in [(F.lit(k), F.lit(v)) for k, v in MCC_CATEGORY_MAP.items()] for item in pair]
    )
    mcc_code = F.substring(F.col("merchant_id"), 1, 4)
    return df.withColumn(
        "category",
        F.when(F.col("category").isNull(), mcc_map[mcc_code]).otherwise(F.col("category")),
    ).withColumn(
        "category",
        F.when(F.col("category").isNull(), F.lit("Uncategorized")).otherwise(F.col("category")),
    )


def deduplicate(df):
    """Keep the latest version of each transaction_id based on updated_at."""
    window = Window.partitionBy("transaction_id").orderBy(F.col("updated_at").desc())
    return df.withColumn("_rank", F.row_number().over(window)).filter(F.col("_rank") == 1).drop("_rank")


def add_derived_columns(df):
    return (
        df.withColumn("day_of_week", F.dayofweek("transaction_date"))
        .withColumn("is_weekend", F.col("day_of_week").isin([1, 7]))
        .withColumn(
            "transaction_hour",
            F.hour(F.to_timestamp(F.col("transaction_time"), "HH:mm:ss")),
        )
        .withColumn("is_international", F.col("country_code") != F.lit("US"))
        .withColumn("is_high_value", F.col("amount_usd") > F.lit(10000.0))
    )


def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "bronze_bucket", "silver_bucket"])

    sc = SparkContext()
    glue_ctx = GlueContext(sc)
    spark = glue_ctx.spark_session
    job = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    bronze_path = f"s3://{args['bronze_bucket']}/bronze/transactions/"
    silver_path = f"s3://{args['silver_bucket']}/silver/transactions/"

    df = spark.read.parquet(bronze_path)
    logger.info("Read %d records from Bronze layer", df.count())

    df = normalize_currency(df)
    df = enrich_category(df)
    df = deduplicate(df)
    df = add_derived_columns(df)

    df.write.mode("overwrite").partitionBy("transaction_date").parquet(silver_path)

    logger.info("Wrote %d records to Silver layer at %s", df.count(), silver_path)
    job.commit()


if __name__ == "__main__":
    main()
