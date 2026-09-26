#!/bin/bash

# Configuration
INSTANCE_ID=${1:-"aml-instance"}
DATABASE_ID=${2:-"aml-db"}
PROJECT_ID=$(gcloud config get-value project)

echo "Using Project: $PROJECT_ID"
echo "Using Instance: $INSTANCE_ID"
echo "Using Database: $DATABASE_ID"

# 1. Create Instance (if not exists)
if ! gcloud spanner instances describe "$INSTANCE_ID" > /dev/null 2>&1; then
    echo "Creating Spanner instance $INSTANCE_ID..."
    gcloud spanner instances create "$INSTANCE_ID" \
        --config=regional-us-central1 \
        --description="AML Demo Instance" \
        --nodes=1 \
        --edition=ENTERPRISE
fi

# 2. Create Database (if not exists)
if ! gcloud spanner databases describe "$DATABASE_ID" --instance="$INSTANCE_ID" > /dev/null 2>&1; then
    echo "Creating Spanner database $DATABASE_ID..."
    gcloud spanner databases create "$DATABASE_ID" --instance="$INSTANCE_ID"
fi

# 3. Apply Schema
echo "Applying DDL schema..."
gcloud spanner databases ddl update "$DATABASE_ID" --instance="$INSTANCE_ID" \
    --ddl-file="examples/java/adk/spanner-schema.sql"

# 4. Seed Data
echo "Seeding initial data..."
gcloud spanner databases execute-sql "$DATABASE_ID" --instance="$INSTANCE_ID" \
    --sql="INSERT INTO Account (AccountId, AccountHolder, Status, CreatedAt) VALUES ('usr_alice', 'Alice Smith', 'CLEARED', CURRENT_TIMESTAMP()), ('usr_bob', 'Bob Jones', 'CLEARED', CURRENT_TIMESTAMP()), ('usr_charlie', 'Charlie Brown', 'CLEARED', CURRENT_TIMESTAMP()), ('usr_mule1', 'Mule One', 'CLEARED', CURRENT_TIMESTAMP()), ('usr_mule2', 'Mule Two', 'CLEARED', CURRENT_TIMESTAMP()), ('usr_collector', 'Collector Hub', 'CLEARED', CURRENT_TIMESTAMP()), ('usr_fraudA', 'Fraud User A', 'CLEARED', CURRENT_TIMESTAMP()), ('usr_fraudB', 'Fraud User B', 'CLEARED', CURRENT_TIMESTAMP());"

gcloud spanner databases execute-sql "$DATABASE_ID" --instance="$INSTANCE_ID" \
    --sql="INSERT INTO SharedDevice (DeviceId, DeviceModel, FirstSeen) VALUES ('dev_hardware_xyz99', 'iPhone 15 Pro', CURRENT_TIMESTAMP());"

gcloud spanner databases execute-sql "$DATABASE_ID" --instance="$INSTANCE_ID" \
    --sql="INSERT INTO Transactions (TransactionId, SenderId, ReceiverId, Amount, Status, Timestamp) VALUES ('tx_loop_01', 'usr_alice', 'usr_bob', 10000.00, 'CLEARED', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 HOUR)), ('tx_loop_02', 'usr_bob', 'usr_charlie', 9800.00, 'CLEARED', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 5 HOUR));"

gcloud spanner databases execute-sql "$DATABASE_ID" --instance="$INSTANCE_ID" \
    --sql="INSERT INTO Transactions (TransactionId, SenderId, ReceiverId, Amount, Status, Timestamp) VALUES ('tx_fan_01', 'usr_mule1', 'usr_collector', 9200.00, 'CLEARED', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 DAY)), ('tx_fan_02', 'usr_mule2', 'usr_collector', 9400.00, 'CLEARED', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY));"

gcloud spanner databases execute-sql "$DATABASE_ID" --instance="$INSTANCE_ID" \
    --sql="INSERT INTO AccountDevice (AccountId, DeviceId, LinkedAt) VALUES ('usr_fraudA', 'dev_hardware_xyz99', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)), ('usr_fraudB', 'dev_hardware_xyz99', TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY));"

echo "Spanner setup complete."
echo ""
echo "To trigger Scenario 1 (Circular Flow), run:"
echo "gcloud spanner databases execute-sql $DATABASE_ID --instance=$INSTANCE_ID --sql=\"INSERT INTO Transactions (TransactionId, SenderId, ReceiverId, Amount, Status, Timestamp) VALUES ('tx_loop_03_TRIGGER', 'usr_charlie', 'usr_alice', 9500.00, 'PENDING', CURRENT_TIMESTAMP())\""
echo ""
echo "To trigger Scenario 2 (Fan-In), run:"
echo "gcloud spanner databases execute-sql $DATABASE_ID --instance=$INSTANCE_ID --sql=\"INSERT INTO Transactions (TransactionId, SenderId, ReceiverId, Amount, Status, Timestamp) VALUES ('tx_fan_03_TRIGGER', 'usr_bob', 'usr_collector', 9100.00, 'PENDING', CURRENT_TIMESTAMP())\""
echo ""
echo "To trigger Scenario 3 (Co-location), run:"
echo "gcloud spanner databases execute-sql $DATABASE_ID --instance=$INSTANCE_ID --sql=\"INSERT INTO Transactions (TransactionId, SenderId, ReceiverId, Amount, Status, Timestamp) VALUES ('tx_colocate_TRIGGER', 'usr_fraudA', 'usr_fraudB', 4500.00, 'PENDING', CURRENT_TIMESTAMP())\""
