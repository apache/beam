#!/bin/bash

# Configuration
INSTANCE_ID=${1:-"aml-instance"}
DATABASE_ID=${2:-"aml-db"}
PROJECT_ID=$(gcloud config get-value project)

echo "Using Project: $PROJECT_ID"
echo "Using Instance: $INSTANCE_ID"
echo "Using Database: $DATABASE_ID"

echo "Reverting demo transactions..."

gcloud spanner databases execute-sql "$DATABASE_ID" --instance="$INSTANCE_ID" \
    --sql="DELETE FROM Transactions WHERE TransactionId IN ('tx_loop_03_TRIGGER', 'tx_fan_03_TRIGGER', 'tx_colocate_TRIGGER');"

echo "Demo reverted successfully. You can now re-trigger the scenarios."
