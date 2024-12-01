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

    resources = ["${aws_s3_bucket.ingestion_bucket.arn}/*"]
  }
}

# Ingestion lambda - secrets access policy

# data "aws_iam_policy_document" "ingestion_secrets_access_doc" {
#   statement {
#     effect = "Allow"
#     actions = "sts:AssumeRole"
#     resources = [  ]
#   }
# }
resource "aws_iam_policy" "ingestion_secrets_access_policy" {
  name = "SecretsManagerAccess"
  description = "Allow Ingestion Lambda to access specific secret"
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = "arn:aws:secretsmanager:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:secret:nc-totesys-db-credentials-*"
      }
    ]
  })
}

data "aws_iam_policy_document" "ingestion_s3_read_policy_doc" {
  statement {
    effect = "Allow"
    actions = [ "s3:GetObject" ]
    resources = [ "${aws_s3_bucket.ingestion_bucket.arn}/*" ]
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

resource "aws_iam_policy" "ingestion_s3_read_policy" {
  name_prefix = "s3-policy-${var.lambda_ingestion}-get"
  policy = data.aws_iam_policy_document.ingestion_s3_read_policy_doc.json
}
# Ingestion lambda cloudwatch policy
resource "aws_iam_policy" "ingestion_cw_policy" {
  name_prefix = "cw-policy-${var.lambda_ingestion}"
  policy      = data.aws_iam_policy_document.ingestion_cw_document.json
}

# Ingestion lambda cloudwatch group
resource "aws_cloudwatch_log_group" "ingestion_log_group" {
  name = "/aws/lambda/${var.lambda_ingestion}"
}

# ========
# ATTACH
# ========

# Attach ingestion s3 write policy to the ingestion role
resource "aws_iam_role_policy_attachment" "ingestion_s3_write_policy_attachment" {
  role       = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.ingestion_s3_write_policy.arn
}

resource "aws_iam_role_policy_attachment" "ingestion_s3_read_policy_attach" {
  role = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.ingestion_s3_read_policy.arn
}
# Ingestion lambda - secrets access policy
resource "aws_iam_role_policy_attachment" "name" {
  role = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.ingestion_secrets_access_policy.arn
}

# Attach ingestion cloudwatch policy to the ingestion role
resource "aws_iam_role_policy_attachment" "ingestion_cw_policy_attachment" {
  role       = aws_iam_role.ingestion_lambda_role.name
  policy_arn = aws_iam_policy.ingestion_cw_policy.arn
}

# ==========================================
# Transformation Lambda
# ==========================================

# ========
# DEFINE
# ========

# Transformation lambda trust policy doc
data "aws_iam_policy_document" "transformation_trust_policy" {
  statement {
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }

    actions = ["sts:AssumeRole"]
  }
}

# Transformation lambda - s3 write policy doc
data "aws_iam_policy_document" "s3_transformation_write_policy_doc" {
  statement {
    effect = "Allow"

    actions = ["s3:PutObject"]

    resources = ["${aws_s3_bucket.processed_bucket.arn}/*"]
  }
}

# Transformation lambda - s3 read policy doc
data "aws_iam_policy_document" "s3_transformation_read_policy_doc" {
  statement {
    effect = "Allow"

    actions = ["s3:GetObject"]

    resources = ["${aws_s3_bucket.ingestion_bucket.arn}/*"]
  }
}

# Transformation lambda cloudwatch policy doc
data "aws_iam_policy_document" "transformation_cw_document" {
  statement {
    effect = "Allow"

    actions = ["logs:CreateLogGroup"]

    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*"]
  }

  statement {
    effect = "Allow"

    actions = ["logs:CreateLogStream", "logs:PutLogEvents"]

    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.lambda_transform}:*"]
  }
}

# ========
# CREATE
# ========

# Transformation lambda role
resource "aws_iam_role" "transformation_lambda_role" {
  name_prefix        = "role-${var.lambda_transform}"
  assume_role_policy = data.aws_iam_policy_document.transformation_trust_policy.json
}

# Transformation lambda - s3 write policy
resource "aws_iam_policy" "transformation_s3_write_policy" {
  name_prefix = "s3-policy-${var.lambda_transform}-write"
  policy      = data.aws_iam_policy_document.s3_transformation_write_policy_doc.json
}

# Transformation lambda - s3 read policy
resource "aws_iam_policy" "transformation_s3_read_policy" {
  name_prefix = "s3-policy-${var.lambda_transform}-read"
  policy      = data.aws_iam_policy_document.s3_transformation_read_policy_doc.json
}

# Transformation lambda cloudwatch policy
resource "aws_iam_policy" "transformation_cw_policy" {
  name_prefix = "cw-policy-${var.lambda_transform}"
  policy      = data.aws_iam_policy_document.transformation_cw_document.json
}

# Transformation lambda cloudwatch group
resource "aws_cloudwatch_log_group" "transformation_log_group" {
  name = "/aws/lambda/${var.lambda_transform}"
}

# ========
# ATTACH
# ========

# Attach transformation s3 write policy to the transformation role
resource "aws_iam_role_policy_attachment" "transformation_s3_write_policy_attachment" {
  role       = aws_iam_role.transformation_lambda_role.name
  policy_arn = aws_iam_policy.transformation_s3_write_policy.arn
}

# Attach transformation s3 read policy to the transformation role
resource "aws_iam_role_policy_attachment" "transformation_s3_read_policy_attachment" {
  role       = aws_iam_role.transformation_lambda_role.name
  policy_arn = aws_iam_policy.transformation_s3_read_policy.arn
}

# Attach transformation cloudwatch policy to the transformation role
resource "aws_iam_role_policy_attachment" "transformation_cw_policy_attachment" {
  role       = aws_iam_role.transformation_lambda_role.name
  policy_arn = aws_iam_policy.transformation_cw_policy.arn
}


# ==========================================
# Loading Lambda
# ==========================================

# ========
# DEFINE
# ========

data "aws_iam_policy_document" "loading_trust_policy" {
  statement {
    effect = "Allow"

    principals {
      type = "Service"
      identifiers = [ "lambda.amazonaws.com" ]
    }
    actions = [ "sts:AssumeRole" ]
  }
}

data "aws_iam_policy_document" "s3_loading_policy_doc" {
  statement {
    effect = "Allow"
    actions = [ "s3:GetObject" ]
    resources = [ "${aws_s3_bucket.processed_bucket.arn}/*" ]
  }
}

data "aws_iam_policy_document" "loading_cw_document" {
  statement {
    effect = "Allow"
    actions = [ "logs:CreateLogGroup" ]
    resources = [ "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:*" ]
  }

  statement {
    effect = "Allow"
    actions = [ "logs:CreateLogStream", "log:PutLogEvents" ]
    resources = ["arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.lambda_load}:*"]
  }
}

resource "aws_iam_role" "loading_lambda_role" {
  name_prefix = "role-${var.lambda_load}"
  assume_role_policy = data.aws_iam_policy_document.loading_trust_policy.json
}

resource "aws_iam_policy" "loading_s3_get_policy" {
  name_prefix = "s3-policy-${var.lambda_load}-get"
  policy = data.aws_iam_policy_document.s3_loading_policy_doc.json
}

resource "aws_iam_policy" "loading_cw_policy" {
  name_prefix = "cw-policy-${var.lambda_load}"
  policy = data.aws_iam_policy_document.loading_cw_document.json
}

resource "aws_cloudwatch_log_group" "loading_log_group" {
  name = "/aws/lambda/${var.lambda_load}"
}

# ========
# ATTACH
# ========

resource "aws_iam_role_policy_attachment" "loading_s3_get_policy_attach" {
  role = aws_iam_role.loading_lambda_role.name
  policy_arn = aws_iam_policy.loading_s3_get_policy.arn
}

resource "aws_iam_role_policy_attachment" "loading_cw_policy_attach" {
  role = aws_iam_role.loading_lambda_role.name
  policy_arn = aws_iam_policy.loading_cw_policy.arn
}