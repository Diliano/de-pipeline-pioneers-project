# ==========================================
# Ingestion Lambda
# ==========================================

# ========
# DEFINE
# ========

# Ingestion lambda code
# data "archive_file" "ingestion_lambda" {
#   type             = "zip"
#   output_file_mode = "0666"
#   source_file      = "${path.module}/../../src/ingestion/ingestion.py"
#   output_path      = "${path.module}/../../packages/ingestion/ingestion.zip"
# }

# Ingestion layer dependencies
# data "archive_file" "ingestion_layer" {
#   type             = "zip"
#   output_file_mode = "0666"
#   source_dir       = "${path.module}/../../packages/layer"
#   output_path      = "${path.module}/../../packages/ingestion/ingestion_layer.zip"
# }

# ========
# CREATE
# ========

# Ingestion layer dependencies
# resource "aws_lambda_layer_version" "ingestion_layer" {
#   layer_name          = "ingestion_layer"
#   compatible_runtimes = [var.python_runtime]
#   s3_bucket           = aws_s3_bucket.ingestion_code_bucket.bucket
#   s3_key              = aws_s3_object.ingestion_layer_code.key
#   source_code_hash    = data.archive_file.ingestion_layer.output_base64sha256
# }

# Ingestion lambda
# resource "aws_lambda_function" "ingestion_lambda" {
#   function_name    = var.lambda_ingestion
#   s3_bucket        = aws_s3_bucket.ingestion_code_bucket.bucket
#   s3_key           = aws_s3_object.ingestion_lambda_code.key
#   role             = aws_iam_role.ingestion_lambda_role.arn
#   handler          = "ingestion.lambda_handler"
#   runtime          = var.python_runtime
#   layers           = [aws_lambda_layer_version.ingestion_layer.arn]
#   timeout          = 60
#   source_code_hash = data.archive_file.ingestion_lambda.output_base64sha256

  # might need depends_on
  # depends_on = [ 
  #   aws_s3_object.ingestion_lambda_code,
  #   aws_s3_object.ingestion_layer_code 
  # ]
#   environment {
#     variables = {
#       "S3_BUCKET_NAME" = aws_s3_bucket.ingestion_bucket.bucket
#     }
#   }
# }

# ==========================================
# Transformation Lambda
# ==========================================

# ========
# DEFINE
# ========

# Transformation lambda code
# data "archive_file" "transformation_lambda" {
#   type             = "zip"
#   output_file_mode = "0666"
#   source_file      = "${path.module}/../../src/transformation/transformation.py"
#   output_path      = "${path.module}/../../packages/transformation/transformation.zip"
# }

# # Transformation layer dependencies
# data "archive_file" "transformation_layer" {
#   type             = "zip"
#   output_file_mode = "0666"
#   source_dir       = "${path.module}/../../packages/layer"
#   output_path      = "${path.module}/../../packages/transformation/transformation_layer.zip"
# }

# ========
# CREATE
# ========

# # Transformation layer dependencies
# resource "aws_lambda_layer_version" "transformation_layer" {
#   layer_name          = "transformation_layer"
#   compatible_runtimes = [var.python_runtime]
#   s3_bucket           = aws_s3_bucket.transformation_code_bucket.bucket
#   s3_key              = aws_s3_object.transformation_layer_code.key
#   source_code_hash    = data.archive_file.transformation_layer.output_base64sha256
# }

# # Transformation lambda
# resource "aws_lambda_function" "transformation_lambda" {
#   function_name    = var.lambda_transform
#   s3_bucket        = aws_s3_bucket.transformation_code_bucket.bucket
#   s3_key           = aws_s3_object.transformation_lambda_code.key
#   role             = aws_iam_role.transformation_lambda_role.arn
#   handler          = "transformation.lambda_handler"
#   runtime          = var.python_runtime
#   layers           = [aws_lambda_layer_version.transformation_layer.arn]
#   source_code_hash = data.archive_file.transformation_lambda.output_base64sha256

  # might need depends_on
  # depends_on = [ 
  #   aws_s3_bucket.transformation_code_bucket,
  #   aws_s3_object.transformation_layer_code
  # ]

#   environment {
#     variables = {
#       "S3_PROCESSED_BUCKET" = aws_s3_bucket.processed_bucket.bucket,
#       "S3_INGESTION_BUCKET" = aws_s3_bucket.ingestion_bucket.bucket
#     }
#   }
# }



# Need to implement the loading lambda 

# Ingestion
resource "aws_s3_object" "ingestion_lambda_zip" {
  bucket = aws_s3_bucket.code_bucket.id
  key = "${var.lambda_ingestion}/ingestion.zip"
  source = "${path.module}/../../packages/ingestion/ingestion.zip"
}


resource "aws_lambda_function" "ingestion_lambda" {
  function_name = "ingestion-lambda"
  s3_bucket = aws_s3_bucket.code_bucket.id
  s3_key = aws_s3_object.ingestion_lambda_zip.key
  handler = "${var.lambda_ingestion}.lambda_handler"
  runtime = var.python_runtime
  role = aws_iam_role.ingestion_lambda_role.arn
  # memory_size = 256 # Most likely not needed
  timeout = 60
  depends_on = [ aws_s3_object.ingestion_lambda_zip ]

  environment {
    variables = {
      "S3_INGESTION_BUCKET" = aws_s3_bucket.ingestion_bucket.bucket
    }
  }
}


# Transformation

resource "aws_s3_object" "transformation_lambda_zip" {
  bucket = aws_s3_bucket.code_bucket.id
  key = "${var.lambda_transform}/transformation.zip"
  source = "${path.module}/../../packages/transformation/transformation.zip"
}

resource "aws_lambda_function" "transformation_lambda" {
  function_name = "transformation-lambda"
  s3_bucket = aws_s3_bucket.code_bucket.id
  s3_key = aws_s3_object.transformation_lambda_zip.key
  handler = "${var.lambda_transform}.lambda_handler"
  runtime = var.python_runtime
  role = aws_iam_role.transformation_lambda_role.arn
  # memory_size = 512 # its a chonky lambda :)
  timeout = 60

  depends_on = [ aws_s3_object.transformation_lambda_zip ]
  environment {
    variables = {
      "S3_INGESTION_BUCKET" = aws_s3_bucket.ingestion_bucket.bucket
      "S3_PROCESSED_BUCKET" = aws_s3_bucket.processed_bucket.bucket
    }
  }
}

# Loading

resource "aws_s3_object" "loading_lambda_zip" {
  bucket = aws_s3_bucket.code_bucket.id
  key = "${var.lambda_load}/loading.zip"
  source = "${path.module}/../../packages/loading/loading.zip"
}

resource "aws_lambda_function" "loading_lambda" {
  function_name = "loading-lambda"
  s3_bucket = aws_s3_bucket.code_bucket.id
  s3_key = aws_s3_object.loading_lambda_zip.key
  handler = "${var.lambda_load}.lambda_handler"
  runtime = var.python_runtime
  role = aws_iam_role.loading_lambda_role.arn
  # memory_size = 256 
  timeout = 60

  depends_on = [ aws_s3_object.loading_lambda_zip ]
  environment {
    variables = {
      "S3_PROCESSED_BUCKET" = aws_s3_bucket.processed_bucket.bucket
    }
  }
}