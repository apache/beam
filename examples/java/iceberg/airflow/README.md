<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Automating Delta Lake to GCP Lakehouse CDC with Dataflow Flex Templates and Apache Airflow

This directory provides everything needed to package Apache Beam's
[`DeltaLakeToLakehouseCdcExample`](../src/main/java/org/apache/beam/examples/iceberg/DeltaLakeToLakehouseCdcExample.java)
as a **Cloud Dataflow Flex Template** and schedule repeated Change Data Capture
(CDC) synchronizations with **Apache Airflow** (or **Cloud Composer**) using
`DataflowStartFlexTemplateOperator`.

## Contents

- [`flex_template_metadata.json`](flex_template_metadata.json): Dataflow Flex
  Template parameter metadata for `DeltaLakeToLakehouseCdcExample`.
- [`delta_lake_to_lakehouse_cdc_dag.py`](delta_lake_to_lakehouse_cdc_dag.py):
  Airflow DAG that tracks the Delta Lake commit watermark, resolves inclusive
  `[startVersion, endVersion]` boundaries, invokes
  `DataflowStartFlexTemplateOperator`, and advances the watermark atomically on
  success.

For the initial full-table snapshot migration before starting incremental CDC,
see [`examples/java/iceberg/sql/delta_to_iceberg.sql`](../sql/delta_to_iceberg.sql)
(and [`examples/java/iceberg/sql/delta_to_iceberg_cdc.sql`](../sql/delta_to_iceberg_cdc.sql)
for one-off Beam SQL CDC runs).

---

## Prerequisites

1. **Java 17+**: Both Delta Kernel and Apache Iceberg require Java 17 or later.
   If using SDKMAN:
   ```bash
   sdk use java 17.0.15-tem
   ```
2. **Source Delta Lake table on GCS** with Change Data Feed (CDF) enabled:
   ```sql
   ALTER TABLE delta.`gs://my-delta-lake-bucket/delta_lake/employee_data`
   SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true');
   ```
3. **Target GCP Lakehouse (Iceberg V2) table** initialized with the baseline
   snapshot pinned to a known commit version (for example, version `1`, right
   after enabling CDF; subsequent CDC runs then start at version `2`).
4. **GCP Resources & IAM**:
   - Enabled APIs: Dataflow, Cloud Build, Artifact Registry, Cloud Storage,
     BigLake.
   - An Artifact Registry Docker repository (e.g.,
     `us-central1-docker.pkg.dev/my-gcp-project/my-repo`).
   - Service account permissions for GCS buckets, Lakehouse catalog
     (`roles/biglake.admin`), Dataflow (`roles/dataflow.admin` /
     `roles/dataflow.worker`), and Artifact Registry.

---

## Step 1: Build the Flex Template Uber-JAR

From the root of the Apache Beam repository, run the dedicated Gradle task to
produce a shaded uber-JAR containing `DeltaLakeToLakehouseCdcExample`,
`Managed.DELTA_LAKE_CDC`, `Managed.ICEBERG`, `DataflowRunner`, and merged
`META-INF/services` entries:

```bash
./gradlew :examples:java:iceberg:deltaLakeToLakehouseCdcFlexTemplateJar
```

This creates:
```text
examples/java/iceberg/build/libs/delta-lake-to-lakehouse-cdc-flex-template-<beam-version>-bundled.jar
```

> **Note for standalone Maven projects:** If you build outside the Beam
> repository using Maven, configure `maven-shade-plugin` with
> `ServicesResourceTransformer` and align all `com.google.cloud.bigdataoss`
> artifacts (`gcs-connector`, `gcsio`, `util-hadoop`, `util`) to the same
> version in `<dependencyManagement>` to avoid runtime GCS Hadoop connector
> mismatches in the Flex Template launcher.

---

## Step 2: Build and Publish the Dataflow Flex Template

Use `gcloud dataflow flex-template build` to build the Java 17 Flex Template
container image, push it to Artifact Registry (or GCR), and upload the template
specification JSON to Cloud Storage:

```bash
export PROJECT_ID="my-gcp-project"
export REGION="us-central1"
export REPO_NAME="my-docker-repo"
export BUCKET="my-temp-bucket"
export JAR_PATH=$(ls examples/java/iceberg/build/libs/delta-lake-to-lakehouse-cdc-flex-template-*-bundled.jar | head -n 1)

gcloud dataflow flex-template build \
  "gs://${BUCKET}/templates/delta-lake-to-lakehouse-cdc.json" \
  --project="${PROJECT_ID}" \
  --image-gcr-path="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/delta-lake-to-lakehouse-cdc:latest" \
  --sdk-language="JAVA" \
  --flex-template-base-image="JAVA17" \
  --metadata-file="examples/java/iceberg/airflow/flex_template_metadata.json" \
  --jar="${JAR_PATH}" \
  --env="FLEX_TEMPLATE_JAVA_MAIN_CLASS=org.apache.beam.examples.iceberg.DeltaLakeToLakehouseCdcExample"
```

---

## Step 3: Deploy and Configure the Airflow DAG

### Option A: Cloud Composer (Recommended for Production)

1. Install the `deltalake` PyPI package on your Cloud Composer environment
   (`gcloud composer environments update ... --update-pypi-packages-from-file`
   or via the Cloud Console).
2. Update the default `params` in `delta_lake_to_lakehouse_cdc_dag.py` (or pass
   them via DAG run config / Airflow Variables) for your `project_id`,
   `location`, `template_gcs_path`, `delta_table`, `lakehouse_table`, and
   `equality_columns`.
3. Upload the DAG to Composer:
   ```bash
   gcloud composer environments storage dags import \
     --environment="my-composer-env" \
     --location="us-central1" \
     --source="examples/java/iceberg/airflow/delta_lake_to_lakehouse_cdc_dag.py"
   ```
4. **Initialize the commit watermark variable (required before the first run)**
   to the baseline commit version already covered by your initial snapshot (the
   DAG fails fast if neither this Variable nor `initial_synced_version` in run
   config is set):
   ```bash
   gcloud composer environments run my-composer-env \
     --location="us-central1" \
     variables -- set delta_to_lakehouse_cdc_last_synced_version 1
   ```

### Option B: Local Apache Airflow (Development & Testing)

```bash
python3.11 -m venv airflow_venv
AIRFLOW_VERSION=2.10.5
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.11.txt"

./airflow_venv/bin/pip install --upgrade pip
./airflow_venv/bin/pip install \
  "apache-airflow==${AIRFLOW_VERSION}" \
  "apache-airflow-providers-google" \
  --constraint "${CONSTRAINT_URL}"
# Airflow's Dataflow hook imports apache-beam provider helpers; --no-deps avoids
# needing local Python apache-beam when launching Java Flex Templates remotely.
./airflow_venv/bin/pip install "apache-airflow-providers-apache-beam==6.0.0" --no-deps
./airflow_venv/bin/pip install deltalake

export AIRFLOW_HOME="$(pwd)/airflow_local"
export AIRFLOW__CORE__DAGS_FOLDER="$(pwd)/examples/java/iceberg/airflow"
export AIRFLOW__CORE__LOAD_EXAMPLES=False

./airflow_venv/bin/airflow db migrate
./airflow_venv/bin/airflow connections add google_cloud_default \
  --conn-type google_cloud_platform \
  --conn-extra '{"project": "my-gcp-project", "key_path": "/path/to/service-account-key.json"}'

./airflow_venv/bin/airflow variables set delta_to_lakehouse_cdc_last_synced_version 1

# Run a single DAG execution with runtime parameter overrides:
./airflow_venv/bin/airflow dags test delta_lake_to_lakehouse_cdc_sync \
  --conf '{
    "project_id": "my-gcp-project",
    "location": "us-central1",
    "template_gcs_path": "gs://my-temp-bucket/templates/delta-lake-to-lakehouse-cdc.json",
    "temp_location": "gs://my-temp-bucket/temp",
    "staging_location": "gs://my-temp-bucket/staging",
    "delta_table": "gs://my-delta-lake-bucket/delta_lake/employee_data/",
    "lakehouse_table": "my-gcp-project.my-lakehouse-warehouse-bucket.my_namespace.my_iceberg_table",
    "equality_columns": "employee_id"
  }'
```

---

## How Delta Version ID Boundaries Are Handled

`delta_lake_to_lakehouse_cdc_dag.py` protects against duplicate commits, missed
commits, and schema-evolution mismatches with six safeguards:

1. **Explicit Baseline Watermark Required**:
   `initial_synced_version` defaults to `null`. On the first run, the DAG fails
   fast unless either the Airflow watermark Variable
   (`delta_to_lakehouse_cdc_last_synced_version`) has been initialized or
   `initial_synced_version` is explicitly passed in DAG run config—preventing
   an overlooked config from silently re-applying commits from the beginning.
2. **Inclusive Boundary Math (`start_version = last_synced_version + 1`)**:
   Both `startVersion` and `endVersion` in `Managed.DELTA_LAKE_CDC` are
   inclusive. Each run starts strictly at `last_synced_version + 1` so no commit
   is ever re-applied.
3. **Explicit Upper-Bound Pinning**: Before launching Dataflow,
   `resolve_version_boundary` queries the Delta `_delta_log` to pin `endVersion`
   explicitly. Commits written to Delta Lake while Dataflow is running are
   cleanly deferred to the next DAG run.
4. **Caught-Up Short-Circuiting**: When `start_version > latest_version`,
   `branch_on_new_commits` routes to `skip_no_new_commits` without submitting an
   invalid Dataflow job.
5. **CDF Validation & Schema-Evolution Gating**:
   - Verifies `delta.enableChangeDataFeed = true` holds across
     `[start_version, end_version]`.
   - When `stop_before_schema_change=True` (default) and a schema change occurs
     at commit `V > start_version`, caps `end_version = V - 1` so all commits
     preceding the schema change migrate cleanly first.
   - On the subsequent run when `start_version == V`, the DAG **fails fast**
     until you apply the corresponding `ALTER TABLE` schema update on the target
     Lakehouse Iceberg table and trigger one run with
     `{"acknowledged_schema_change_version": V}` (keeping schema-change gating
     active for any later schema changes).
   - Optionally limits commits per job via `max_versions_per_run`.
6. **End-to-End Idempotency (`sink_id` + Pending Range) & Atomic Watermark
   Advancement**:
   - `max_active_runs=1` prevents overlapping runs from racing on the watermark,
     and `retries=0` prevents blind automatic Dataflow re-submissions if a
     failure occurs after commit.
   - Before launching Dataflow, `resolve_version_boundary` computes a
     deterministic, range-scoped `--sinkId`
     (`<dag_id>-v{start_version}-to-v{end_version}`), passes it to the Iceberg
     CDC sink (`sink_id`), and persists `{start_version, end_version, sink_id}`
     in `<watermark_variable>_pending`.
   - If Airflow fails after the Dataflow job has already committed to Iceberg
     (before `advance_version_watermark` finishes), the next run detects the
     pending Variable and reuses the **exact same commit range and `sink_id`**.
     Beam's Iceberg CDC sink checks its `beam.cdc.committed-through-ms.<sink_id>`
     snapshot token, recognizes that the range already committed, and skips
     writing duplicate commits cleanly.
   - `DataflowStartFlexTemplateOperator` runs with `wait_until_finished=True`,
     and `advance_version_watermark` updates
     `delta_to_lakehouse_cdc_last_synced_version = end_version` before clearing
     the pending Variable.
