# =======================================================
# Ingestion Lambda - CloudWatch Dashboard for Monitoring
# =======================================================

# ========
# CREATE
# ========

# Ingestion lambda cw dashboard with simple metric widgets
resource "aws_cloudwatch_dashboard" "ingestion_lambda_dashboard" {
  dashboard_name = "IngestionLambdaDashboard"

  dashboard_body = jsonencode({
    widgets = [
      # Invocation count widget
      {
        "type" : "metric",
        "x" : 0,
        "y" : 0,
        "width" : 6,
        "height" : 6,
        "properties" : {
          "metrics" : [
            ["AWS/Lambda", "Invocations", "FunctionName", aws_lambda_function.ingestion_lambda.function_name]
          ],
          "title" : "Invocation Count",
          "stat" : "Sum",
          "period" : 600,
          "region" : "eu-west-2"
        }
      },

      # Duration widget
      {
        "type" : "metric",
        "x" : 6,
        "y" : 0,
        "width" : 6,
        "height" : 6,
        "properties" : {
          "metrics" : [
            ["AWS/Lambda", "Duration", "FunctionName", aws_lambda_function.ingestion_lambda.function_name]
          ],
          "title" : "Duration (ms)",
          "stat" : "Average",
          "period" : 600,
          "region" : "eu-west-2"
        }
      },

      # Logged error count widget
      {
        "type" : "metric",
        "x" : 12,
        "y" : 0,
        "width" : 6,
        "height" : 6,
        "properties" : {
          "metrics" : [
            ["IngestionLambda", "IngestionLambdaErrorCount"]
          ],
          "title" : "Logged Error Count",
          "stat" : "Sum",
          "period" : 600,
          "region" : "eu-west-2"
        }
      }
    ]
  })
}
