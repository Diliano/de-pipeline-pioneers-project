variable "data_bucket_prefix" {
  type    = string
  default = "pipeline-pioneers-data"
}


variable "code_bucket_prefix" {
  type    = string
  default = "pipeline-pioneers-code"
}

variable "lambda_ingestion" {
  type    = string
  default = "ingestion_lambda"
}

# lambda_transformation
# lambda_loading

variable "python_runtime" {
  type    = string
  default = "python3.12"
}

variable "ingestion_bucket_prefix" {
  type    = string
  default = "pipeline-pioneers-ingestion"
}