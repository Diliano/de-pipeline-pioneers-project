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
  endpoint  = "" # Create an email for us all to use?
}

