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

resource "aws_s3_bucket" "ingestion_bucket" {
    bucket_prefix = "nc-${var.ingestion_bucket_prefix}"

    tags = {
      Name        = "IngestionBucket"
      Environment = "Dev"
    }
}

resource "aws_s3_bucket_versioning" "versioning_ingestion_bucket" {
    bucket = aws_s3_bucket.ingestion_bucket.id
    versioning_configuration {
        status = "Enabled"
    }
}

