# =======================================================
# Ingestion Lambda - CloudWatch Alarm Notification - SNS
# =======================================================

# ========
# DEFINE
# ========

# Ingestion lambda sns topic policy doc
data "aws_iam_policy_document" "ingestion_sns_publish_policy" {
  statement {
    effect    = "Allow"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.ingestion_alarm_topic.arn]
    principals {
      type        = "Service"
      identifiers = ["cloudwatch.amazonaws.com"]
    }
  }
}

# ========
# CREATE
# ========

# Ingestion lambda sns topic for cw alarm notifications
resource "aws_sns_topic" "ingestion_alarm_topic" {
  name = "ingestion-lambda-alarm-topic"
}

# Ingestion lambda alarm - email setup
resource "aws_sns_topic_subscription" "ingestion_alarm_email_subscription" {
  topic_arn = aws_sns_topic.ingestion_alarm_topic.arn
  protocol  = "email"
  endpoint  = "ppteamemail@protonmail.com"
}

# Ingestion lambda cw error filter
resource "aws_cloudwatch_log_metric_filter" "ingestion_lambda_error_filter" {
  name           = "IngestionLambdaErrorFilter"
  log_group_name = aws_cloudwatch_log_group.ingestion_log_group.name
  metric_transformation {
    name      = "IngestionLambdaErrorCount"
    namespace = "IngestionLambda"
    value     = "1"
  }
  pattern = "ERROR"
}

# Ingestion lambda error alarm
resource "aws_cloudwatch_metric_alarm" "ingestion_lambda_error_alarm" {
  alarm_name          = "IngestionLambdaErrorAlarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  metric_name         = aws_cloudwatch_log_metric_filter.ingestion_lambda_error_filter.metric_transformation[0].name
  namespace           = aws_cloudwatch_log_metric_filter.ingestion_lambda_error_filter.metric_transformation[0].namespace
  period              = 600
  statistic           = "Sum"
  threshold           = 1
  alarm_description   = "Alarm for ingestion lambda logged errors"
  alarm_actions       = [aws_sns_topic.ingestion_alarm_topic.arn]
}

# ========
# ATTACH
# ========

# Attach sns policy to cw alarm
resource "aws_sns_topic_policy" "ingestion_alarm_sns_policy" {
  arn    = aws_sns_topic.ingestion_alarm_topic.arn
  policy = data.aws_iam_policy_document.ingestion_sns_publish_policy.json
}
