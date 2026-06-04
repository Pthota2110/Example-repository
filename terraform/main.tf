terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket         = "financial-platform-tfstate"
    key            = "terraform.tfstate"
    region         = "us-east-1"
    encrypt        = true
    dynamodb_table = "financial-platform-tflock"
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = merge(var.tags, {
      Project     = var.project
      Environment = var.environment
      ManagedBy   = "Terraform"
    })
  }
}

locals {
  prefix = "${var.project}-${var.environment}"
}

# ─── S3 Buckets (Medallion Architecture) ─────────────────────────────────────

resource "aws_s3_bucket" "raw" {
  bucket = "${local.prefix}-raw"
}

resource "aws_s3_bucket" "bronze" {
  bucket = "${local.prefix}-bronze"
}

resource "aws_s3_bucket" "silver" {
  bucket = "${local.prefix}-silver"
}

resource "aws_s3_bucket" "gold" {
  bucket = "${local.prefix}-gold"
}

resource "aws_s3_bucket" "quarantine" {
  bucket = "${local.prefix}-quarantine"
}

resource "aws_s3_bucket" "dq_results" {
  bucket = "${local.prefix}-dq-results"
}

resource "aws_s3_bucket_versioning" "silver" {
  bucket = aws_s3_bucket.silver.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "all" {
  for_each = {
    raw        = aws_s3_bucket.raw.id
    bronze     = aws_s3_bucket.bronze.id
    silver     = aws_s3_bucket.silver.id
    gold       = aws_s3_bucket.gold.id
    quarantine = aws_s3_bucket.quarantine.id
  }
  bucket = each.value
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "aws:kms"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "all" {
  for_each = {
    raw        = aws_s3_bucket.raw.id
    bronze     = aws_s3_bucket.bronze.id
    silver     = aws_s3_bucket.silver.id
    gold       = aws_s3_bucket.gold.id
    quarantine = aws_s3_bucket.quarantine.id
  }
  bucket                  = each.value
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ─── Kinesis Data Stream ──────────────────────────────────────────────────────

resource "aws_kinesis_stream" "transactions" {
  name             = "${local.prefix}-transactions"
  shard_count      = var.kinesis_shard_count
  retention_period = 24

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }

  encryption_type = "KMS"
  kms_key_id      = aws_kms_key.platform.id
}

# ─── KMS Key ─────────────────────────────────────────────────────────────────

resource "aws_kms_key" "platform" {
  description             = "KMS key for ${local.prefix} platform encryption"
  deletion_window_in_days = 30
  enable_key_rotation     = true
}

resource "aws_kms_alias" "platform" {
  name          = "alias/${local.prefix}"
  target_key_id = aws_kms_key.platform.key_id
}

# ─── DynamoDB Tables ──────────────────────────────────────────────────────────

resource "aws_dynamodb_table" "fraud_alerts" {
  name         = "${local.prefix}-fraud-alerts"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "transaction_id"

  attribute {
    name = "transaction_id"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }

  point_in_time_recovery { enabled = true }
  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.platform.arn
  }
}

resource "aws_dynamodb_table" "velocity" {
  name         = "${local.prefix}-velocity"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "account_id"
  range_key    = "created_at"

  attribute {
    name = "account_id"
    type = "S"
  }
  attribute {
    name = "created_at"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
}

resource "aws_dynamodb_table" "lineage" {
  name         = "${local.prefix}-lineage"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "lineage_id"

  attribute {
    name = "lineage_id"
    type = "S"
  }

  ttl {
    attribute_name = "ttl"
    enabled        = true
  }
}

# ─── SNS Topic (Fraud Alerts) ─────────────────────────────────────────────────

resource "aws_sns_topic" "fraud_alerts" {
  name              = "${local.prefix}-fraud-alerts"
  kms_master_key_id = aws_kms_key.platform.id
}

resource "aws_sns_topic_subscription" "fraud_alerts_email" {
  topic_arn = aws_sns_topic.fraud_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ─── Lambda (Fraud Detection) ─────────────────────────────────────────────────

data "archive_file" "fraud_lambda" {
  type        = "zip"
  source_dir  = "${path.module}/../fraud_detection"
  output_path = "${path.module}/.build/fraud_detection.zip"
}

resource "aws_lambda_function" "fraud_detection" {
  function_name    = "${local.prefix}-fraud-detection"
  runtime          = "python3.12"
  handler          = "lambda_handler.handler"
  role             = aws_iam_role.lambda_exec.arn
  filename         = data.archive_file.fraud_lambda.output_path
  source_code_hash = data.archive_file.fraud_lambda.output_base64sha256
  memory_size      = var.lambda_memory_mb
  timeout          = var.lambda_timeout_seconds

  environment {
    variables = {
      VELOCITY_TABLE    = aws_dynamodb_table.velocity.name
      FRAUD_ALERT_TABLE = aws_dynamodb_table.fraud_alerts.name
      SNS_TOPIC_ARN     = aws_sns_topic.fraud_alerts.arn
      THRESHOLDS_PARAM  = "/${local.prefix}/fraud/thresholds"
      LOG_LEVEL         = "INFO"
    }
  }

  reserved_concurrent_executions = 100
}

resource "aws_lambda_event_source_mapping" "kinesis_to_fraud" {
  event_source_arn              = aws_kinesis_stream.transactions.arn
  function_name                 = aws_lambda_function.fraud_detection.arn
  starting_position             = "LATEST"
  batch_size                    = 100
  bisect_batch_on_function_error = true
  function_response_types       = ["ReportBatchItemFailures"]

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.fraud_dlq.arn
    }
  }
}

resource "aws_sqs_queue" "fraud_dlq" {
  name                      = "${local.prefix}-fraud-dlq"
  message_retention_seconds = 1209600  # 14 days
  kms_master_key_id         = aws_kms_key.platform.id
}

# ─── IAM ──────────────────────────────────────────────────────────────────────

resource "aws_iam_role" "lambda_exec" {
  name = "${local.prefix}-lambda-exec"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_basic" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "lambda_app" {
  name = "${local.prefix}-lambda-app-policy"
  role = aws_iam_role.lambda_exec.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = ["dynamodb:PutItem", "dynamodb:GetItem", "dynamodb:Query"]
        Resource = [
          aws_dynamodb_table.fraud_alerts.arn,
          aws_dynamodb_table.velocity.arn,
        ]
      },
      {
        Effect   = "Allow"
        Action   = ["sns:Publish"]
        Resource = [aws_sns_topic.fraud_alerts.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:DescribeStream", "kinesis:ListStreams"]
        Resource = [aws_kinesis_stream.transactions.arn]
      },
      {
        Effect   = "Allow"
        Action   = ["ssm:GetParameter"]
        Resource = ["arn:aws:ssm:${var.aws_region}:*:parameter/${local.prefix}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:GenerateDataKey"]
        Resource = [aws_kms_key.platform.arn]
      },
    ]
  })
}

resource "aws_iam_role" "glue_exec" {
  name = "${local.prefix}-glue-exec"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3" {
  name = "${local.prefix}-glue-s3-policy"
  role = aws_iam_role.glue_exec.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
      Resource = [
        "${aws_s3_bucket.raw.arn}/*",
        "${aws_s3_bucket.bronze.arn}/*",
        "${aws_s3_bucket.silver.arn}/*",
        "${aws_s3_bucket.gold.arn}/*",
        "${aws_s3_bucket.quarantine.arn}/*",
        aws_s3_bucket.raw.arn,
        aws_s3_bucket.bronze.arn,
        aws_s3_bucket.silver.arn,
        aws_s3_bucket.gold.arn,
        aws_s3_bucket.quarantine.arn,
      ]
    }]
  })
}

# ─── AWS Glue Jobs ────────────────────────────────────────────────────────────

resource "aws_glue_job" "ingest" {
  name         = "${local.prefix}-ingest"
  role_arn     = aws_iam_role.glue_exec.arn
  glue_version = "4.0"
  worker_type  = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.gold.bucket}/scripts/ingest_transactions.py"
    python_version  = "3"
  }

  default_arguments = {
    "--job-bookmark-option" = "job-bookmark-enable"
    "--source_bucket"       = aws_s3_bucket.raw.bucket
    "--bronze_bucket"       = aws_s3_bucket.bronze.bucket
    "--quarantine_bucket"   = aws_s3_bucket.quarantine.bucket
    "--enable-metrics"      = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }
}

resource "aws_glue_job" "transform" {
  name         = "${local.prefix}-transform"
  role_arn     = aws_iam_role.glue_exec.arn
  glue_version = "4.0"
  worker_type  = var.glue_worker_type
  number_of_workers = var.glue_number_of_workers

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.gold.bucket}/scripts/transform_transactions.py"
    python_version  = "3"
  }

  default_arguments = {
    "--bronze_bucket" = aws_s3_bucket.bronze.bucket
    "--silver_bucket" = aws_s3_bucket.silver.bucket
    "--enable-metrics" = "true"
  }
}

# ─── CloudWatch Alarms ────────────────────────────────────────────────────────

resource "aws_cloudwatch_metric_alarm" "high_fraud_rate" {
  alarm_name          = "${local.prefix}-high-fraud-rate"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "PassRatePct"
  namespace           = "FinancialPlatform/DataQuality"
  period              = 300
  statistic           = "Average"
  threshold           = 5.0
  alarm_description   = "Fraud rate exceeded 5% over 10 minutes"
  alarm_actions       = [aws_sns_topic.fraud_alerts.arn]

  dimensions = {
    Dataset = "transactions"
  }
}

resource "aws_cloudwatch_metric_alarm" "dq_failures" {
  alarm_name          = "${local.prefix}-dq-failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "FailedChecks"
  namespace           = "FinancialPlatform/DataQuality"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  alarm_description   = "One or more data quality checks failed"
  alarm_actions       = [aws_sns_topic.fraud_alerts.arn]
}
