#!/usr/bin/env bash

# Exit immediately if a command exits with a non-zero status.
set -e

# Dataset info
DATASET="ravindrasinghrana/job-description-dataset"
DATA_DIR="./data"

# Print header
echo "========================================"
echo "  Dataset Download Script"
echo "========================================"

# Step 1: Create data directory (if needed)
echo "Creating data directory: $DATA_DIR"
mkdir -p "$DATA_DIR"

# Step 2: Download dataset from Kaggle
echo "Downloading dataset from Kaggle: $DATASET"
kaggle datasets download -d "$DATASET" -p "$DATA_DIR" --unzip

# Step 3: Provide a confirmation message
echo "Dataset downloaded and extracted to '$DATA_DIR'."
echo "========================================"
echo "Download complete!"
