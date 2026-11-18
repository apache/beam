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

-- Beam SQL script for Change Data Capture (CDC) migration from a Delta Lake
-- table on GCS to a Google Cloud Lakehouse (BigLake Iceberg REST catalog)
-- table, using CHANGES(...) and INSERT INTO ... WITH ('mode' = 'merge-on-read').
--
-- Before running it:
--   * Change Data Feed ('delta.enableChangeDataFeed' = 'true') must have been
--     enabled on the Delta Lake table for every version that CHANGES(...) reads.
--   * Migrate the initial snapshot with delta_to_iceberg.sql in this directory,
--     pinned to the version just before the first one read here (for example
--     "version": 1 in its TBLPROPERTIES, then CHANGES(t, 2, ...) here).
--   * Apply each range of versions exactly once. Re-applying a range inserts
--     its new rows again.
--
-- Start the Beam SQL shell using:
--   DirectRunner: ./scripts/beam-sql.sh --io delta --io iceberg
--   Dataflow:     ./scripts/beam-sql.sh --runner dataflow --io delta --io iceberg
-- (Or pass -f delta_to_iceberg_cdc.sql to execute this script non-interactively.)

-- 1. Register the source Delta Lake table on GCS
--    (employee_id is NOT NULL to match the target table below: the merge-on-read
--    write needs the rows to match the table's schema.)
CREATE EXTERNAL TABLE delta_employee_data (
  employee_id BIGINT NOT NULL,
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
--    (employee_id is NOT NULL because the merge-on-read write identifies rows by it.)
CREATE DATABASE IF NOT EXISTS lakehouse_catalog.my_namespace;

CREATE EXTERNAL TABLE IF NOT EXISTS lakehouse_catalog.my_namespace.my_iceberg_table (
  employee_id BIGINT NOT NULL,
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

-- 5. Read the changes of versions 2 through 4 from the Delta Lake Change Data
--    Feed (or of a timestamp range, such as
--    CHANGES(delta_employee_data, '2026-10-01T00:00:00Z', '2026-10-02T00:00:00Z'))
--    and apply their inserts, updates, and deletes to the Lakehouse Iceberg
--    table in merge-on-read mode, ordered by _commit_version. Naming the
--    metadata columns _change_type and _commit_version in the SELECT reads them.
--
--    Keep 'change_type_column', 'change_type_map' and '_change_type'. Each
--    change row also carries its kind as Beam ValueKind metadata, but runners
--    drop that metadata when they encode rows (for example between Dataflow
--    stages; only Dataflow's legacy streaming worker keeps it), and the write
--    applies a row without it as an INSERT, which would turn deletes into
--    inserts.
INSERT INTO lakehouse_catalog.my_namespace.my_iceberg_table
WITH (
  'mode' = 'merge-on-read',
  'change_type_column' = '_change_type',
  'change_type_map' = MAP[
    'insert', 'INSERT',
    'delete', 'DELETE',
    'update_preimage', 'UPDATE_BEFORE',
    'update_postimage', 'UPDATE_AFTER'
  ],
  'sequence_number_column' = '_commit_version',
  'equality_columns' = ARRAY['employee_id']
)
SELECT
  employee_id,
  employee_name,
  employee_role,
  employee_location,
  _change_type,
  _commit_version
FROM CHANGES(delta_employee_data, 2, 4);
