# ==========================================
# VARIABLES
# ==========================================

# ========
# Buckets
# ========

variable "ingestion_bucket_prefix" {
  type    = string
  default = "pipeline-pioneers-ingestion"
}

variable "code_bucket_prefix" {
  type    = string
  default = "pipeline-pioneers-code"
}

variable "processed_bucket_prefix" {
  type    = string
  default = "pipeline-pioneers-processed"
}

# variable "transformation_code_bucket_prefix" {
#   type    = string
#   default = "pipeline-pioneers-transform-code"
# }

# ========
# Lambdas
# ========

variable "lambda_ingestion" {
  type    = string
  default = "ingestion"
}

variable "lambda_transform" {
  type    = string
  default = "transform"
}

variable "lambda_load" {
  type    = string
  default = "load"
}

variable "python_runtime" {
  type    = string
  default = "python3.12"
}


