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

# Ingestion lambda cloudwatch policy doc
data "aws_iam_policy_document" "ingestion_cw_document" {
  statement {
    effect = "Allow"

    actions = ["logs:CreateLogGroup"]

    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"]
  }

  statement {
    effect = "Allow"

    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]

    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.lambda_ingestion}:*"]
  }
}

# ========
# CREATE
# ========

# Ingestion lambda role
resource "aws_iam_role" "ingestion_lambda_role" {
  name_prefix        = "role-${var.lambda_ingestion}"
  assume_role_policy = data.aws_iam_policy_document.ingestion_trust_policy.json
}

# Ingestion lambda - s3 write policy
resource "aws_iam_policy" "ingestion_s3_write_policy" {
  name_prefix = "s3-policy-${var.lambda_ingestion}-write"
  policy      = data.aws_iam_policy_document.s3_ingestion_policy_doc.json
}

# Ingestion lambda cloudwatch policy
resource "aws_iam_policy" "ingestion_cw_policy" {
  name_prefix = "cw-policy-${var.lambda_ingestion}"
  policy      = data.aws_iam_policy_document.ingestion_cw_document.json
}

# ========
# ATTACH
# ========

# Attach ingestion s3 write policy to the ingestion role
resource "aws_iam_role_policy_attachment" "ingestion_s3_write_policy_attachment" {
  role       = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.ingestion_s3_write_policy.arn
}

