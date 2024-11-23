#!/bin/bash
set -e



# Variables
PROJECT_NAME="de-pipeline-pioneers"
TERRAFORM_DIR="./terraform"
S3_INGESTION_BUCKET="not-sure-yet"
S3_PROCESSED_BUCKET="not-sure-yet"


# 
echo "========================================="
echo "🚀 Starting Infrastructure Cleanup Process"
echo "========================================="


# Checking Terraform Dependency
if ! [ -x "$(command -v terraform )" ]; then
    echo "❌ Terraform not installed. Install it first." >&2
    exit 1
fi
echo "👉 Terraform available!"


# Navigating to Terraform Dir
echo "👉 Initialising Terraform!"
cd $TERRAFORM_DIR


# Removing objects from S3 Buckets
echo "👉 Cleaning up S3 Bucket: $S3_INGESTION_BUCKET..."
aws s3 rm s3://$S3_INGESTION_BUCKET --recursive
aws s3 rb s3://$S3_INGESTION_BUCKET 

echo "👉 Cleaning up S3 Bucket: $S3_PROCESSED_BUCKET..."
aws s3 rm s3://$S3_PROCESSED_BUCKET --recursive
aws s3 rb s3://$S3_PROCESSED_BUCKET

echo "✅ Bucket Cleanup Complete!"


# Destroy Terraform-managed Resources
echo "👉 Destroying Terraform-managed Resources..."
terraform destroy -auto-approve

echo "✅ Cleanup Complete! All Terraform Resources Have Been Destroyed!"

