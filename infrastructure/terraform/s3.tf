# ==========================================
# S3 BUCKETS
# ==========================================

# ========
# CREATE
# ========

resource "aws_s3_bucket" "ingestion_bucket" {
  # S3 bucket for the data. 
  bucket_prefix = "nc-${var.ingestion_bucket_prefix}"
  tags = {
    Name = "IngestionBucket"
  }
}

resource "aws_s3_bucket" "processed_bucket" {
  # S3 bucket for the transformed data. 
  bucket_prefix = "nc-${var.processed_bucket_prefix}"
  tags = {
    Name = "ProcessedBucket"
  }
}

resource "aws_s3_bucket" "ingestion_code_bucket" {
  # S3 bucket for the ingestion lambda code. 
  bucket_prefix = "nc-${var.ingestion_code_bucket_prefix}"
  tags = {
    Name = "IngestionCodeBucket"
  }
}

resource "aws_s3_bucket" "transformation_code_bucket" {
  # S3 bucket for the transformation lambda code. 
  bucket_prefix = "nc-${var.transformation_code_bucket_prefix}"
  tags = {
    Name = "TransformationCodeBucket"
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
  bucket = aws_s3_bucket.ingestion_code_bucket.bucket
  key    = "ingestion_lambda/ingestion_lambda.zip"
  source = data.archive_file.ingestion_lambda.output_path
  etag   = data.archive_file.ingestion_lambda.output_base64sha256
}

# Ingestion lambda layer
resource "aws_s3_object" "ingestion_layer_code" {
  bucket = aws_s3_bucket.ingestion_code_bucket.bucket
  key    = "ingestion_lambda/ingestion_layer.zip"
  source = data.archive_file.ingestion_layer.output_path
  etag   = data.archive_file.ingestion_layer.output_base64sha256
}

# ==========================================
# Transformation Lambda
# ==========================================

# ========
# CREATE
# ========

# Transformation lambda code
resource "aws_s3_object" "transformation_lambda_code" {
  bucket = aws_s3_bucket.transformation_code_bucket.bucket
  key    = "transformation_lambda/transformation_lambda.zip"
  source = data.archive_file.transformation_lambda.output_path
}

# Transformation lambda layer
resource "aws_s3_object" "transformation_layer_code" {
  bucket = aws_s3_bucket.transformation_code_bucket.bucket
  key    = "ingestion_lambda/transformation_layer.zip"
  source = data.archive_file.transformation_layer.output_path
}
