# ==========================================
# Ingestion Lambda
# ==========================================

# ========
# DEFINE
# ========

# Ingestion lambda trust policy doc
data "aws_iam_policy_document" "ingestion_trust_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

# Ingestion lambda - s3 write policy doc
data "aws_iam_policy_document" "s3_ingestion_policy_doc" {
  statement {
    effect = "Allow"

    actions = ["s3:PutObject"]

    resources = ["${aws_s3_bucket.ingestion_bucket.arn}/*"] # confirm - name taken from 'feature/task2.2-ingestion-s3-bucket' branch
  }
}
