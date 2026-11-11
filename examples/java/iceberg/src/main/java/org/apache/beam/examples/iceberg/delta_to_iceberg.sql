-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- Beam SQL script for initial full-table migration from a Delta Lake table on
-- GCS to a Google Cloud Lakehouse (BigLake Iceberg REST catalog) table before
-- running DeltaLakeToLakehouseCdcExample for ongoing Change Data Capture (CDC).
--
-- Start the Beam SQL shell using:
--   DirectRunner: ./scripts/beam-sql.sh --io delta --io iceberg
--   Dataflow:     ./scripts/beam-sql.sh --runner dataflow --io delta --io iceberg
-- (Or pass -f delta_to_iceberg.sql to execute this script non-interactively.)

-- 1. Register the source Delta Lake table on GCS
--    (Matches --deltaTable=gs://my-delta-lake-bucket/delta_lake/employee_data/
--    in DeltaLakeToLakehouseCdcExample. To pin the initial snapshot to a
--    specific commit version before starting CDC from the next version, add
--    "version": 1 to TBLPROPERTIES.)
CREATE EXTERNAL TABLE delta_employee_data (
  employee_id BIGINT,
  employee_name VARCHAR,
  employee_role VARCHAR,
  employee_location VARCHAR
)
TYPE 'delta'
LOCATION 'gs://my-delta-lake-bucket/delta_lake/employee_data/'
TBLPROPERTIES '{
  "hadoop_config": {
    "fs.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
    "fs.AbstractFileSystem.gs.impl": "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
    "fs.gs.auth.type": "APPLICATION_DEFAULT",
    "fs.gs.project.id": "my-gcp-project"
  }
}';

-- 2. Register the Google Cloud Lakehouse (BigLake REST) Iceberg catalog
--    (Matches --lakehouseTable=my-gcp-project.my-lakehouse-warehouse-bucket.my_namespace.my_iceberg_table
--    in DeltaLakeToLakehouseCdcExample.)
CREATE CATALOG lakehouse_catalog
TYPE iceberg
PROPERTIES (
  'type' = 'rest',
  'uri' = 'https://biglake.googleapis.com/iceberg/v1/restcatalog',
  'warehouse' = 'gs://my-lakehouse-warehouse-bucket',
  'header.x-goog-user-project' = 'my-gcp-project',
  'rest.auth.type' = 'google',
  'io-impl' = 'org.apache.iceberg.gcp.gcs.GCSFileIO',
  'header.X-Iceberg-Access-Delegation' = 'vended-credentials'
);

-- 3. Ensure the target namespace and Iceberg table exist in the Lakehouse catalog
CREATE DATABASE IF NOT EXISTS lakehouse_catalog.my_namespace;

CREATE EXTERNAL TABLE IF NOT EXISTS lakehouse_catalog.my_namespace.my_iceberg_table (
  employee_id BIGINT,
  employee_name VARCHAR,
  employee_role VARCHAR,
  employee_location VARCHAR
)
TYPE 'iceberg';

-- 4. Configure pipeline options for Dataflow (omit or comment out when using DirectRunner)
SET runner='DataflowRunner';
SET project='my-gcp-project';
SET region='us-central1';
SET tempLocation='gs://my-temp-bucket/temp';

-- 5. Read the initial snapshot from Delta Lake and write to the Lakehouse Iceberg table
INSERT INTO lakehouse_catalog.my_namespace.my_iceberg_table
SELECT
  employee_id,
  employee_name,
  employee_role,
  employee_location
FROM delta_employee_data;
