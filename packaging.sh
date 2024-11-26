#!/bin/bash

set -e


# Dynamic directory
# SCRIPT_DIR=$(dirname "$(readlink -f "$0")")
# ROOT_DIR="${SCRIPT_DIR}/"
# SRC_DIR="${ROOT_DIR}/src"
# OUTPUT_DIR="${ROOT_DIR}/packages"

ROOT_DIR=$(pwd)
SRC_DIR="${ROOT_DIR}/src"
OUTPUT_DIR="${ROOT_DIR}/packages"

# Most likely not needed
# COMMON_DEPENDENCIES_LAYER="common_layer"


# mkdir -p ${SCRIPT_DIR}
mkdir -p ${OUTPUT_DIR}

for lambda_dir in ingestion loading transformation; do
    echo "Packaging ${lambda_dir} Lambda..."

    cd "${SRC_DIR}/${lambda_dir}"

    # Installing dependencies into a temp 'build' folder
    rm -rf build && mkdir build
    pip install --upgrade pip
    pip install -r requirements.txt -t ./build

    cp -r ./*.py ./build/

    mkdir -p "${OUTPUT_DIR}/${lambda_dir}"
    cd build
    # echo "current dir: $(pwd)"
    zip -r "${OUTPUT_DIR}/${lambda_dir}/${lambda_dir}.zip" .
    cd ..

    rm -rf build
    cd ../..
done

# Common layers, probably not necessary
# echo "Packaging common dependencies into a layer..."
# mkdir -p ${COMMON_DEPENDENCIES_LAYER}/python
# pip install -r common_requirements.txt -t ${COMMON_DEPENDENCIES_LAYER}/python
# cd ${COMMON_DEPENDENCIES_LAYER}
# zip -r ../${OUTPUT_DIR}/common_layer.zip python
# cd ..

# rm -rf ${COMMON_DEPENDENCIES_LAYER}/python

echo "Packaging complete. Archives stored in ${OUTPUT_DIR}"