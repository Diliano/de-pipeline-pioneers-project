#################################################################################
#
# Makefile for Pipeline Pioneers DE Project
#
#################################################################################


# Variables
#################################################################################

PROJECT_NAME := de-pipeline-pioneers-project
PYTHON_INTERPRETER := python3
WD := $(shell pwd)
PYTHONPATH := $(WD)
SHELL := /bin/bash
PIP := $(PYTHON_INTERPRETER) -m pip


# Environment Setup
#################################################################################

# Define utility variable to activate virtual environment
ACTIVATE_ENV := source ./venv/bin/activate

# Execute commands within the virtual environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

## Create the Python virtual environment
create-environment:
	@echo ">>> Creating environment for $(PROJECT_NAME)..."
	@set -e; \
	if [ ! -d "venv" ]; then \
		$(PYTHON_INTERPRETER) --version; \
		$(PIP) install -q virtualenv; \
		virtualenv venv --python=$(PYTHON_INTERPRETER); \
	else \
		echo "Virtual environment already exists, skipping creation."; \
	fi

## Install project dependencies
requirements: create-environment
	$(call execute_in_env, $(PIP) install -r requirements.txt)


# Development Setup
#################################################################################

## Install Bandit for security analysis
bandit:
	$(call execute_in_env, $(PIP) install bandit)

## Install Black for code formatting
black:
	$(call execute_in_env, $(PIP) install black)

## Install Flake8 for linting
flake8:
	$(call execute_in_env, $(PIP) install flake8)

## Install Coverage for code coverage analysis
coverage:
	$(call execute_in_env, $(PIP) install coverage)

## Set up development tools (Black, Coverage)
dev-setup: bandit black flake8 coverage


# Code Quality Checks and Tests
#################################################################################

## Run Bandit for security checks
run-bandit:
	$(call execute_in_env, bandit -r -lll ./src ./tests)

## Format code with Black
run-black:
	$(call execute_in_env, black ./src/*.py ./tests/*.py)

## Run Flake8 to check code style
run-flake8:
	$(call execute_in_env, flake8 ./src ./tests)

## Run the tests
run-test:
	$(call execute_in_env, PYTHONPATH=$(PYTHONPATH) pytest -v)

## Run the coverage check
check-coverage:
	$(call execute_in_env, PYTHONPATH=$(PYTHONPATH) pytest --cov=src tests/)

## Run all checks (code formatting, unit tests, and coverage)
run-checks: run-bandit run-black run-flake8 run-test check-coverage


# Clean Up
#################################################################################

## Remove virtual environment, coverage, pycache, and other generated files
clean:
	rm -rf venv .coverage .pytest_cache */__pycache__
