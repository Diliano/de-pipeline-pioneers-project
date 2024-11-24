output "S3_INGESTION_BUCKET"{
    value = aws_s3_bucket.ingestion_bucket.id
}

output "S3_PROCESSED_BUCKET" {
    value = aws_s3_bucket.processed_bucket.id
}