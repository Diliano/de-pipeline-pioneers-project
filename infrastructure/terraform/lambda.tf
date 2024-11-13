# ==========================================
# Ingestion Lambda
# ==========================================

# ========
# DEFINE
# ========

# Ingestion lambda code
data "archive_file" "ingestion_lambda" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../../ingestion/lambda-function/ingestion_lambda.py"  # arbitrary path / filename used
  output_path      = "${path.module}/../../ingestion/lambda-function/ingestion_lambda.zip" # arbitrary path / filename used
}

# Ingestion layer dependencies
data "archive_file" "ingestion_layer" {
  type             = "zip"
  output_file_mode = "0666"
  source_dir       = "${path.module}/../../ingestion/lambda-function/layer"               # arbitrary path used
  output_path      = "${path.module}/../../ingestion/lambda-function/ingestion_layer.zip" # arbitrary filename used
}

# ========
# CREATE
# ========

# Ingestion layer dependencies
resource "aws_lambda_layer_version" "ingestion_layer" {
  layer_name          = "ingestion_layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket           = aws_s3_bucket.code_bucket.bucket
  s3_key              = aws_s3_object.ingestion_layer_code.key
}

resource "aws_lambda_function" "ingestion_lambda" {
  function_name = var.lambda_ingestion
  s3_bucket     = aws_s3_bucket.code_bucket.bucket
  s3_key        = aws_s3_object.ingestion_lambda_code.key
  role          = aws_iam_role.ingestion_lambda_role.arn
  handler       = "ingestion_lambda.lambda_handler" # arbitrary handler used
  runtime       = var.python_runtime
  layers        = [aws_lambda_layer_version.ingestion_layer.arn]

  environment {
    variables = {
      "S3_BUCKET_NAME" = aws_s3_bucket.ingestion_bucket.bucket
    }
  }
}

