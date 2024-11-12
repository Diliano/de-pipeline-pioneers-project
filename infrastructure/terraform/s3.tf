resource "aws_s3_bucket" "data_bucket" {
  # S3 bucket for the data. 
  bucket_prefix = "nc-${var.data_bucket_prefix}"
  tags = {
    Name = "DataBucket"
  }
}

resource "aws_s3_bucket" "code_bucket" {
  # S3 bucket for the lambda code. 
  bucket_prefix = "nc-${var.code_bucket_prefix}"
  tags = {
    Name = "CodeBucket"
  }
}

# ==========================================
# Ingestion Lambda
# ==========================================

# ========
# CREATE
# ========

# Ingestion lambda code
resource "aws_s3_object" "ingestion_lambda_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "ingestion_lambda/ingestion_lambda.zip"
  source = data.archive_file.ingestion_lambda.output_path
}

# Ingestion lambda layer
resource "aws_s3_object" "ingestion_layer_code" {
  bucket = aws_s3_bucket.code_bucket.bucket
  key    = "ingestion_lambda/ingestion_layer.zip"
  source = data.archive_file.ingestion_layer.output_path
}
