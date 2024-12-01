# Pipeline Pioneers DE Project 
Using ToteSys data

##

This project implements a data engineering pipeline to extract, transform, and load data from the ToteSys database into an AWS-hosted data lake and data warehouse. It showcases skills in Python, AWS, database modeling, and ETL processes, with a focus on automation, monitoring, and secure credential management.

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Project Setup](#project-setup)
- [Using the Makefile](#using-the-makefile)
- [Directory Structure](#directory-structure)
- [Components](#components)
- [Testing](#testing)
- [Deployment](#deployment)
- [Monitoring and Alerts](#monitoring-and-alerts)
- [Security and Secrets Management](#security-and-secrets-management)
- [Future Improvements](#future-improvements)
- [Contributing](#contributing)

## Overview

Key features:
- Automated ETL Pipeline from ToteSys to a data warehouse
- CloudWatch monitoring and alerting
- Modular Lambda functions for ingestion, transformation, and loading

## Architecture

Core components:
- Ingestion 
- Processed 
- Date warehouse 
- ?


## Prerequisites 
Tools used
- **Python 3.11.x**
- **Terraform**
- **AWS**
- **boto3 (Python AWS SDK)**

AWS Account

## Project Setup
To setup the project, follow these steps
```sh
# Clone the repo 
git clone https://github.com/Nimmo-san/De-pipeline-pioneers-project.git
cd De-pipeline-pioneers-project
```
## Using the makefile 

### Makefile Commands

- **Environment Setup**
  - `make create-environment`: Creates a virtual environment.
  - `make requirements`: Installs project dependencies from requirements.txt

- **Development Setup**
  - `make dev-setup`: Installs bandit, black, flake8, and coverage tools.
  
- **Testing & Code Quality**
  - `make run-bandit`: Run Bandit for security checks.
  - `make run-black`: Formats code with Black.
  - `make run-flake8`: Lints code with Flake8.
  - `make run-test`: Runs all tests using pytest.
  - `make check-coverage`: Runs tests with coverage reporting.
  - `make run-checks`: Executes all checks: formatting, linting, tests, and coverage.

- **Clean Up**
  - `make clean`: Removes the virtual environment, .coverage, .pytest_cache and Python bytecode (pycache)

## Directory Structure


## Components

## Testing 

## Deployment

## Monitoring and Alerts

## Security and Secrets Management 

## Future Improvements

## Contributing
