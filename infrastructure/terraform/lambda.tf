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
  source_file      = "${path.module}/../../ingestion/lambda-function/ingestion_lambda.py"  # arbitrary path / filename used
  output_path      = "${path.module}/../../ingestion/lambda-function/ingestion_lambda.zip" # arbitrary path / filename used
}

# Ingestion layer dependencies
data "archive_file" "ingestion_layer" {
  type             = "zip"
  output_file_mode = "0666"
  source_dir       = "${path.module}/../../ingestion/lambda-function/layer"               # arbitrary path used
  output_path      = "${path.module}/../../ingestion/lambda-function/ingestion_layer.zip" # arbitrary filename used
}


