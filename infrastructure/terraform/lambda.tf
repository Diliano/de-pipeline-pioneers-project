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
  source_file      = "${path.module}/../../src/ingestion.py"
  output_path      = "${path.module}/../../packages/ingestion/ingestion.zip"
}

# Ingestion layer dependencies
data "archive_file" "ingestion_layer" {
  type             = "zip"
  output_file_mode = "0666"
  source_dir       = "${path.module}/../../packages/layer"
  output_path      = "${path.module}/../../packages/ingestion/ingestion_layer.zip"
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
  handler       = "ingestion.lambder_handler"
  runtime       = var.python_runtime
  layers        = [aws_lambda_layer_version.ingestion_layer.arn]

  environment {
    variables = {
      "S3_BUCKET_NAME" = aws_s3_bucket.ingestion_bucket.bucket
    }
  }
}

