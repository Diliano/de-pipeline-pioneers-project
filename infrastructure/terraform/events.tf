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




resource "aws_cloudwatch_event_target" "target_lambda" {
  rule      = aws_cloudwatch_event_rule.scheduler.name
  target_id = "QuoteHandlerLambda"
  arn       = aws_lambda_function.quote_handler.arn
}

resource "aws_cloudwatch_event_target" "target_lambda" {
  rule      = aws_cloudwatch_event_rule.scheduler.name
  target_id = "QuoteHandlerLambda"
  arn       = aws_lambda_function.quote_handler.arn
}
