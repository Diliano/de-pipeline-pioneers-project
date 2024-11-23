#!/bin/bash
set -e


# Variables
PROJECT_NAME="de-pipeline-pioneers"
TERRAFORM_DIR="./terraform"


# Checking terraform dependency
if ! [ -x "$(command -v terraform )" ]; then
    echo "❌ Terraform not installed. Install it first." >&2
    exit 1
fi
echo "👉 Terraform available"

# Initialise Terraform
echo "👉 Initialising Terraform!"
cd $TERRAFORM_DIR
terraform init

echo "✅ Intialised Terraform!"


# Applying Terraform Configuration
echo "👉 Applying Terraform Configuration..."
terraform apply


echo "✅ Infrastructure Deployment Complete!"

