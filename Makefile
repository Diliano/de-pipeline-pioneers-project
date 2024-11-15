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
	( \
		$(PYTHON_INTERPRETER) --version; \
		$(PIP) install -q virtualenv; \
		virtualenv venv --python=$(PYTHON_INTERPRETER); \
	)

## Install project dependencies
requirements: create-environment
	$(call execute_in_env, $(PIP) install -r requirements.txt)