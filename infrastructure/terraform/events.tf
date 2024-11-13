# ==========================================
# Ingestion Lambda Scheduler
# ==========================================

# ========
# CREATE
# ========

# Ingestion lambda scheduler
resource "aws_cloudwatch_event_rule" "ingestion_scheduler" {
  description         = "Trigger the ingestion lambda every 15 minutes"
  schedule_expression = "rate(15 minutes)"
}

# Ingestion lambda scheduler target
resource "aws_cloudwatch_event_target" "target_ingestion_lambda" {
  rule      = aws_cloudwatch_event_rule.ingestion_scheduler.name
  target_id = "IngestionLambda"
  arn       = aws_lambda_function.ingestion_lambda.arn
}
