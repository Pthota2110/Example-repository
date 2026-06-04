"""
AWS Lambda handler for real-time fraud detection.

Triggered by Kinesis Data Streams. Decodes base64 Kinesis records,
runs them through the fraud rule engine, and returns a batch item failure
list so Lambda retries only failed records (not the full shard).
"""

import base64
import json
import logging
import os

from kinesis_consumer import process_stream_batch

logger = logging.getLogger(__name__)
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO"))


def handler(event: dict, context) -> dict:
    """
    Lambda entry point.

    Returns batchItemFailures so Kinesis retries individual failed records
    rather than replaying the whole batch on partial failure.
    """
    records = event.get("Records", [])
    logger.info("Received batch of %d Kinesis records", len(records))

    decoded_records = []
    failures = []

    for record in records:
        sequence_number = record["kinesis"]["sequenceNumber"]
        try:
            raw = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
            decoded_records.append({"data": raw, "sequence_number": sequence_number})
        except Exception as e:
            logger.error("Failed to decode record %s: %s", sequence_number, e)
            failures.append({"itemIdentifier": sequence_number})

    try:
        results = process_stream_batch(decoded_records)
        blocked = sum(1 for r in results if r.get("action") == "BLOCK")
        flagged = sum(1 for r in results if r.get("action") == "FLAG")
        logger.info(
            "Batch complete: processed=%d blocked=%d flagged=%d",
            len(results),
            blocked,
            flagged,
        )
    except Exception as e:
        logger.error("Batch processing error: %s", e)
        for rec in decoded_records:
            failures.append({"itemIdentifier": rec["sequence_number"]})

    return {"batchItemFailures": failures}
