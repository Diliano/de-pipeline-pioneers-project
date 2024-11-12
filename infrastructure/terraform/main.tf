terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
  backend "s3" {
    bucket = "nc-de-pipeline-pioneers-backend-tf-state"
    key    = "nc-de-pipeline-pioneers/terraform.tfstate"
    region = "eu-west-2"
  }
}

provider "aws" {
  region = "eu-west-2"
  default_tags {
    tags = {
      ProjectName  = "PP Data Pipeline"
      Team         = "Pipeline Pioneers"
      DeployedFrom = "Terraform"
      Repository   = "De-pipeline-pioneers-project"
      CostCentre   = "DE"
      Environment  = "dev"
    }
  }
}

data "aws_caller_identity" "current" {}

data "aws_region" "current" {}
