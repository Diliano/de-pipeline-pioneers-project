variable "code_bucket_prefix" {
  type    = string
  default = "pipeline-pioneers-code"
}

variable "lambda_ingestion" {
  type    = string
  default = "ingestion_lambda"
}

variable "lambda_transform" {
  type    = string
  default = "transform_lambda"
}

variable "lambda_load" {
  type    = string
  default = "load_lambda"
}

variable "python_runtime" {
  type    = string
  default = "python3.12"
}

variable "ingestion_bucket_prefix" {
  type    = string
  default = "pipeline-pioneers-ingestion"
}

variable "processed_bucket_prefix" {
  type    = string
  default = "pipeline-pioneers-processed"
}

variable "transformation_code_bucket_prefix" {
  type    = string
  default = "pipeline-pioneers-transformation-code"
}
