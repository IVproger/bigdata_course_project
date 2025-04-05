#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Define the dataset source (Kaggle in this example)
DATASET="ravindrasinghrana/job-description-dataset"
DATA_DIR="./data"

echo "========================================"
echo "  Dataset Download Script"
echo "========================================"

# 1. Create the data directory if it doesn't already exist
echo "Creating data directory: $DATA_DIR (if not already present)"
mkdir -p "$DATA_DIR"

# 2. Download the dataset from Kaggle
echo "Downloading dataset from Kaggle: $DATASET"
kaggle datasets download -d "$DATASET" -p "$DATA_DIR" --unzip

# 3. Provide a confirmation message
echo "Dataset downloaded and unzipped in '$DATA_DIR'."
echo "========================================"
echo "Download complete!"
