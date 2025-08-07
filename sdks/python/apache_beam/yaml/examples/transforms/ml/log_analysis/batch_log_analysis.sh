#!/bin/bash
set -e

# 1. Run iceberg_migration.yaml Beam YAML pipeline
echo "Running iceberg_migration.yaml Beam YAML pipeline..."
python -m apache_beam.yaml.main --yaml_pipeline_file iceberg_migration.yaml

# 2. Run ml_preprocessing.yaml Beam YAML pipeline
echo "Running ml_preprocessing.yaml Beam YAML pipeline..."
python -m apache_beam.yaml.main --yaml_pipeline_file ml_preprocessing.yaml

# 3. Run train.py to train a KNN model
echo "Training KNN model with train.py..."
python train.py
gcloud storage cp "./knn_model.pkl" \
  "gs://apache-beam-testing-charlesng/batch-log-analysis/knn_model.pkl"

# 4. Run anomaly_scoring.yaml Beam YAML pipeline
echo "Running anomaly_scoring.yaml Beam YAML pipeline..."
python -m apache_beam.yaml.main --yaml_pipeline_file anomaly_scoring.yaml

echo "All steps completed."