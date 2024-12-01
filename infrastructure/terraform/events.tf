# ==========================================
# Ingestion Lambda Scheduler
# ==========================================

# ========
# CREATE
# ========

# Ingestion lambda scheduler
resource "aws_cloudwatch_event_rule" "ingestion_scheduler" {
  description         = "Trigger the ingestion lambda every 10 minutes"
  schedule_expression = "rate(10 minutes)"
}

# Ingestion lambda scheduler target
resource "aws_cloudwatch_event_target" "target_ingestion_lambda" {
  rule      = aws_cloudwatch_event_rule.ingestion_scheduler.name
  target_id = "IngestionLambda"
  arn       = aws_lambda_function.ingestion_lambda.arn
}

# Allow scheduler to invoke ingestion lambda
resource "aws_lambda_permission" "ingestion_allow_cloudwatch" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.ingestion_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.ingestion_scheduler.arn
}


# Event notification for transformation lambda
resource "aws_s3_bucket_notification" "bucket_notification" {
  bucket = aws_s3_bucket.ingestion_bucket.id
  lambda_function {
    lambda_function_arn = aws_lambda_function.transformation_lambda.arn
    events = ["s3:ObjectCreated:Put"]
    filter_prefix = "ingestion/"
    filter_suffix = ".json"
  } 
}

# Allow ingestion bucket to invoke transformation lambda
resource "aws_lambda_permission" "allow_ingestion_invoke" {
  statement_id = "AllowS3Invoke"
  action = "lambda:InvokeFunction"
  function_name = aws_lambda_function.transformation_lambda.function_name
  principal = "s3.amazonaws.com"
  source_arn = aws_s3_bucket.ingestion_bucket.arn
}
