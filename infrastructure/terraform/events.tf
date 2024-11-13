# ==========================================
# Ingestion Lambda Scheduler
# ==========================================

# ========
# CREATE
# ========

resource "aws_cloudwatch_event_rule" "ingestion_scheduler" {
  description         = "Trigger the ingestion lambda every 15 minutes"
  schedule_expression = "rate(15 minutes)"
}
