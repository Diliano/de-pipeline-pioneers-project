#!/bin/bash
set -e


# Variables
PROJECT_NAME="de-pipeline-pioneers"
TERRAFORM_DIR="./terraform"


echo "============================================="
echo "🚀 Starting Infrastructure Deployment Process"
echo "============================================="


# Checking Terraform Dependency
if ! [ -x "$(command -v terraform )" ]; then
    echo "❌ Terraform not installed. Install it first." >&2
    exit 1
fi
echo -e "👉 Terraform available!\n"

# Initialise Terraform
echo -e "👉 Initialising Terraform!\n"
cd $TERRAFORM_DIR
terraform init

echo -e "✅ Intialised Terraform!\n"


# Planning Terraform Configuration for TESTING
echo -e "👉 Planning Terraform Configuration..."
terraform plan 

# Applying Terraform Configuration
# terraform apply -auto-approve # needs to be uncommented after tests fully pass


echo "✅ Infrastructure Deployment Complete!"

