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
  s3_bucket           = aws_s3_bucket.ingestion_code_bucket.bucket
  s3_key              = aws_s3_object.ingestion_layer_code.key
  source_code_hash    = data.archive_file.ingestion_layer.output_base64sha256
}

# Ingestion lambda
resource "aws_lambda_function" "ingestion_lambda" {
  function_name    = var.lambda_ingestion
  s3_bucket        = aws_s3_bucket.ingestion_code_bucket.bucket
  s3_key           = aws_s3_object.ingestion_lambda_code.key
  role             = aws_iam_role.ingestion_lambda_role.arn
  handler          = "ingestion.lambda_handler"
  runtime          = var.python_runtime
  layers           = [aws_lambda_layer_version.ingestion_layer.arn]
  timeout          = 60
  source_code_hash = data.archive_file.ingestion_lambda.output_base64sha256

  environment {
    variables = {
      "S3_BUCKET_NAME" = aws_s3_bucket.ingestion_bucket.bucket
    }
  }
}

# ==========================================
# Transformation Lambda
# ==========================================

# ========
# DEFINE
# ========

# Transformation lambda code
data "archive_file" "transformation_lambda" {
  type             = "zip"
  output_file_mode = "0666"
  source_file      = "${path.module}/../../src/transformation.py"
  output_path      = "${path.module}/../../packages/transformation/transformation.zip"
}

# Transformation layer dependencies
data "archive_file" "transformation_layer" {
  type             = "zip"
  output_file_mode = "0666"
  source_dir       = "${path.module}/../../packages/layer"
  output_path      = "${path.module}/../../packages/transformation/transformation_layer.zip"
}

# ========
# CREATE
# ========

# Transformation layer dependencies
resource "aws_lambda_layer_version" "transformation_layer" {
  layer_name          = "transformation_layer"
  compatible_runtimes = [var.python_runtime]
  s3_bucket           = aws_s3_bucket.transformation_code_bucket.bucket
  s3_key              = aws_s3_object.transformation_layer_code.key
  source_code_hash    = data.archive_file.transformation_layer.output_base64sha256
}

# Transformation lambda
resource "aws_lambda_function" "transformation_lambda" {
  function_name    = var.lambda_transform
  s3_bucket        = aws_s3_bucket.transformation_code_bucket.bucket
  s3_key           = aws_s3_object.transformation_lambda_code.key
  role             = aws_iam_role.transformation_lambda_role.arn
  handler          = "transformation.lambda_handler"
  runtime          = var.python_runtime
  layers           = [aws_lambda_layer_version.transformation_layer.arn]
  source_code_hash = data.archive_file.transformation_lambda.output_base64sha256

  environment {
    variables = {
      "S3_PROCESSED_BUCKET" = aws_s3_bucket.processed_bucket.bucket,
      "S3_INGESTION_BUCKET" = aws_s3_bucket.ingestion_bucket.bucket
    }
  }
}
