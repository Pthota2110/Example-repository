output "raw_bucket" {
  description = "S3 bucket for raw landing zone"
  value       = aws_s3_bucket.raw.bucket
}

output "bronze_bucket" {
  description = "S3 bucket for Bronze layer (cleaned, PII-masked)"
  value       = aws_s3_bucket.bronze.bucket
}

output "silver_bucket" {
  description = "S3 bucket for Silver layer (transformed, enriched)"
  value       = aws_s3_bucket.silver.bucket
}

output "gold_bucket" {
  description = "S3 bucket for Gold layer assets and scripts"
  value       = aws_s3_bucket.gold.bucket
}

output "kinesis_stream_arn" {
  description = "ARN of the Kinesis transaction stream"
  value       = aws_kinesis_stream.transactions.arn
}

output "fraud_lambda_arn" {
  description = "ARN of the fraud detection Lambda function"
  value       = aws_lambda_function.fraud_detection.arn
}

output "fraud_alerts_table" {
  description = "DynamoDB table name for fraud alerts"
  value       = aws_dynamodb_table.fraud_alerts.name
}

output "velocity_table" {
  description = "DynamoDB table name for velocity tracking"
  value       = aws_dynamodb_table.velocity.name
}

output "sns_topic_arn" {
  description = "SNS topic ARN for fraud alert notifications"
  value       = aws_sns_topic.fraud_alerts.arn
}

output "kms_key_arn" {
  description = "KMS key ARN used for platform encryption"
  value       = aws_kms_key.platform.arn
  sensitive   = true
}

output "glue_ingest_job" {
  description = "Name of the Glue ingest job"
  value       = aws_glue_job.ingest.name
}

output "glue_transform_job" {
  description = "Name of the Glue transform job"
  value       = aws_glue_job.transform.name
}
