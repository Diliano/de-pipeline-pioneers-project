# =======================================================
# Ingestion Lambda - CloudWatch Alarm Notification - SNS
# =======================================================

# ========
# CREATE
# ========

# Ingestion lambda sns topic for cw alarm notifications
resource "aws_sns_topic" "ingestion_alarm_topic" {
  name = "ingestion-lambda-alarm-topic"
}

