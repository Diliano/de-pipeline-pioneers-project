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
  source_file      = "${path.module}/../../ingestion/lambda-function/???" # confirm path and filename 
  output_path      = "${path.module}/../../ingestion/lambda-function/???" # confirm path and filename based on above filename
}

# Ingestion layer dependencies
data "archive_file" "ingestion_layer" {
  type             = "zip"
  output_file_mode = "0666"
  source_dir       = "${path.module}/../../ingestion/lambda-function/layer"               # suggested path
  output_path      = "${path.module}/../../ingestion/lambda-function/ingestion_layer.zip" # suggested filename
}


