#!/bin/bash
set -e



# Variables
PROJECT_NAME="de-pipeline-pioneers"
TERRAFORM_DIR="./terraform"


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

# Destroy Terraform-managed Resources
echo "👉 Destroying Terraform-managed Resources..."
terraform destroy -auto-approve

echo "✅ Cleanup Complete! All Terraform Resources Have Been Destroyed!"

