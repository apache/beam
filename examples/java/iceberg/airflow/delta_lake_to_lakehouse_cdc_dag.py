#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Apache Airflow DAG for scheduled Delta Lake to GCP Lakehouse CDC sync.

Uses ``DataflowStartFlexTemplateOperator`` to run Apache Beam's
``DeltaLakeToLakehouseCdcExample`` Dataflow Flex Template across advancing,
inclusive Delta Lake commit version boundaries (``[startVersion, endVersion]``).

Delta Commit Version Boundary Safeguards:
1. Inclusive Boundary Arithmetic: Both ``startVersion`` and ``endVersion`` in
   ``Managed.DELTA_LAKE_CDC`` are inclusive. Given ``last_synced_version``, the
   next run begins strictly at ``start_version = last_synced_version + 1``.
2. Explicit Upper-Bound Pinning: ``end_version`` is resolved from the source
   Delta Lake ``_delta_log`` before submitting the Dataflow job so concurrent
   writes during job execution cannot create gaps or duplicate commits.
3. No-Op Short-Circuiting: When ``start_version > latest_version``, the DAG
   branches cleanly to ``skip_no_new_commits`` without launching Dataflow.
4. CDF Validation & Schema-Evolution Splitting: Verifies that
   ``delta.enableChangeDataFeed = true`` holds across the range, and when
   ``stop_before_schema_change=True`` caps ``end_version`` at ``V - 1`` if a
   schema change occurs at commit ``V > start_version``.
5. Single Active Run & Atomic Watermark Advancement: ``max_active_runs=1``
   prevents concurrent runs from racing on the watermark, and
   ``advance_version_watermark`` updates the Airflow Variable only after
   ``DataflowStartFlexTemplateOperator(wait_until_finished=True)`` succeeds.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.models.param import Param
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartFlexTemplateOperator,
)

log = logging.getLogger(__name__)

GCP_CONN_ID = "google_cloud_default"
DEFAULT_WATERMARK_VAR = "delta_to_lakehouse_cdc_last_synced_version"


def compute_delta_version_boundary(
    last_synced_version: int,
    latest_version: int,
    get_version_info: Callable[[int], tuple[bool, str]],
    max_versions_per_run: int | None = None,
    stop_before_schema_change: bool = True,
    delta_table_uri: str = "",
    watermark_var: str = DEFAULT_WATERMARK_VAR,
) -> dict[str, Any]:
  """Computes the inclusive ``[start_version, end_version]`` CDC commit range.

  Args:
    last_synced_version: Highest Delta commit version already migrated to the
      target Lakehouse table (for example, the initial snapshot version or the
      commit that enabled Change Data Feed).
    latest_version: Current latest commit version of the source Delta table.
    get_version_info: Callback taking a Delta commit version ``v`` and
      returning ``(cdf_enabled, schema_json)`` at version ``v``.
    max_versions_per_run: Optional upper cap on the number of commits processed
      per Dataflow run.
    stop_before_schema_change: When ``True``, if a schema change occurs at
      commit ``V > start_version`` inside the candidate range, caps
      ``end_version`` at ``V - 1`` so pre-schema-change commits migrate first.
    delta_table_uri: Source Delta table URI (used in log/error messages).
    watermark_var: Airflow Variable name (used in log/error messages).

  Returns:
    A dictionary with ``has_new_commits``, ``last_synced_version``,
    ``latest_version``, ``start_version``, and ``end_version``.
  """
  # Both startVersion and endVersion in Managed.DELTA_LAKE_CDC are INCLUSIVE.
  start_version = last_synced_version + 1

  if start_version > latest_version:
    return {
        "has_new_commits": False,
        "last_synced_version": last_synced_version,
        "latest_version": latest_version,
        "start_version": None,
        "end_version": None,
    }

  candidate_end_version = latest_version
  if max_versions_per_run is not None and int(max_versions_per_run) > 0:
    candidate_end_version = min(
        latest_version, start_version + int(max_versions_per_run) - 1
    )

  start_cdf_enabled, start_schema_json = get_version_info(start_version)
  if not start_cdf_enabled:
    raise ValueError(
        f"Delta Lake table '{delta_table_uri}' does not have "
        f"'delta.enableChangeDataFeed = true' at start_version={start_version}. "
        f"Advance '{watermark_var}' to at least the commit where Change Data "
        f"Feed was enabled."
    )

  if last_synced_version >= 0:
    _, prev_schema_json = get_version_info(last_synced_version)
    if start_schema_json != prev_schema_json:
      log.warning(
          "Commit start_version=%d has a different Delta schema than "
          "last_synced_version=%d. Ensure the target Lakehouse Iceberg table "
          "schema has been altered accordingly before migrating commit %d.",
          start_version,
          last_synced_version,
          start_version,
      )

  end_version = candidate_end_version
  for ver in range(start_version + 1, candidate_end_version + 1):
    ver_cdf_enabled, ver_schema_json = get_version_info(ver)
    if not ver_cdf_enabled:
      raise ValueError(
          f"Change Data Feed was disabled at commit version {ver} on "
          f"'{delta_table_uri}'."
      )
    if stop_before_schema_change and ver_schema_json != start_schema_json:
      end_version = ver - 1
      log.warning(
          "Detected Delta schema change at commit version %d. Capping current "
          "CDC batch end_version=%d so pre-schema-change commits [%d, %d] "
          "synchronize cleanly first.",
          ver,
          end_version,
          start_version,
          end_version,
      )
      break

  return {
      "has_new_commits": True,
      "last_synced_version": last_synced_version,
      "latest_version": latest_version,
      "start_version": start_version,
      "end_version": end_version,
  }


def _build_delta_storage_options(gcp_conn_id: str) -> dict[str, str]:
  """Builds delta-rs GCS storage_options from Airflow's GCP connection or ADC."""
  try:
    conn = BaseHook.get_connection(gcp_conn_id)
    extra = conn.extra_dejson or {}
    key_path = (
        extra.get("key_path")
        or extra.get("extra__google_cloud_platform__key_path")
        or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    )
    if key_path and os.path.exists(key_path):
      return {"google_service_account_path": key_path}
  except Exception as exc:
    log.debug(
        "Could not read key_path from Airflow connection '%s': %s",
        gcp_conn_id,
        exc,
    )

  env_key = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
  if env_key and os.path.exists(env_key):
    return {"google_service_account_path": env_key}

  # Cloud Composer / Workload Identity / ADC fallback using OAuth2 token.
  gcs_hook = GCSHook(gcp_conn_id=gcp_conn_id)
  creds = gcs_hook.get_credentials()
  import google.auth.transport.requests

  creds.refresh(google.auth.transport.requests.Request())
  return {"google_storage_token": creds.token}


with DAG(
    dag_id="delta_lake_to_lakehouse_cdc_sync",
    description=(
        "Incrementally synchronizes Delta Lake Change Data Feed (CDC) commits "
        "to a GCP Lakehouse (Iceberg) table via a Dataflow Flex Template."
    ),
    schedule=timedelta(minutes=30),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs from sharing a watermark.
    default_args={
        "owner": "data-engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
    },
    params={
        "project_id": Param("my-gcp-project", type="string"),
        "location": Param("us-central1", type="string"),
        "template_gcs_path": Param(
            "gs://my-temp-bucket/templates/delta-lake-to-lakehouse-cdc.json",
            type="string",
        ),
        "temp_location": Param("gs://my-temp-bucket/temp", type="string"),
        "staging_location": Param("gs://my-temp-bucket/staging", type="string"),
        "delta_table": Param(
            "gs://my-delta-lake-bucket/delta_lake/employee_data/",
            type="string",
        ),
        "lakehouse_table": Param(
            "my-gcp-project.my-lakehouse-warehouse-bucket.my_namespace.my_iceberg_table",
            type="string",
        ),
        "equality_columns": Param("employee_id", type="string"),
        "use_portable_runner": Param("true", type="string"),
        "watermark_variable": Param(DEFAULT_WATERMARK_VAR, type="string"),
        "initial_synced_version": Param(
            1,
            type="integer",
            description=(
                "Baseline Delta commit version already covered by the initial "
                "snapshot / CDF enablement when the Airflow watermark Variable "
                "has not been set yet. The first CDC run starts at "
                "initial_synced_version + 1."
            ),
        ),
        "max_versions_per_run": Param(
            None,
            type=["null", "integer"],
            description=(
                "Optional cap on the number of Delta commits processed in a "
                "single Dataflow run. Leave null to sync up to the latest "
                "commit (or schema-change boundary)."
            ),
        ),
        "stop_before_schema_change": Param(
            True,
            type="boolean",
            description=(
                "If true and a Delta schema change occurs at commit "
                "V > start_version, caps end_version at V - 1 so "
                "pre-schema-change commits migrate cleanly first."
            ),
        ),
    },
    tags=["dataflow", "delta-lake", "lakehouse", "iceberg", "cdc"],
) as dag:

  @task(task_id="resolve_version_boundary")
  def resolve_version_boundary(**context: Any) -> dict[str, Any]:
    """Resolves the exact inclusive [start_version, end_version] boundary."""
    # Imported lazily inside the task so Airflow DAG parsing remains lightweight.
    from deltalake import DeltaTable

    params = context["params"]
    ti = context["ti"]
    delta_table_uri: str = params["delta_table"]
    watermark_var: str = params["watermark_variable"]
    initial_synced_version: int = int(params["initial_synced_version"])
    max_versions_per_run: int | None = params.get("max_versions_per_run")
    stop_before_schema_change: bool = bool(
        params.get("stop_before_schema_change", True)
    )

    raw_watermark = Variable.get(watermark_var, default_var=None)
    if raw_watermark is None:
      last_synced_version = initial_synced_version
      log.info(
          "Airflow Variable '%s' is not set yet; using "
          "initial_synced_version=%d.",
          watermark_var,
          last_synced_version,
      )
    else:
      last_synced_version = int(raw_watermark)
      log.info(
          "Loaded last_synced_version=%d from Airflow Variable '%s'.",
          last_synced_version,
          watermark_var,
      )

    storage_options = _build_delta_storage_options(GCP_CONN_ID)
    dt_latest = DeltaTable(delta_table_uri, storage_options=storage_options)
    latest_version = int(dt_latest.version())
    log.info(
        "Source Delta Lake table '%s' is currently at latest_version=%d.",
        delta_table_uri,
        latest_version,
    )

    def _get_version_info(version: int) -> tuple[bool, str]:
      dt_ver = DeltaTable(
          delta_table_uri, version=version, storage_options=storage_options
      )
      cdf_prop = dt_ver.metadata().configuration.get(
          "delta.enableChangeDataFeed", "false"
      )
      return str(cdf_prop).lower() == "true", dt_ver.schema().to_json()

    boundary = compute_delta_version_boundary(
        last_synced_version=last_synced_version,
        latest_version=latest_version,
        get_version_info=_get_version_info,
        max_versions_per_run=max_versions_per_run,
        stop_before_schema_change=stop_before_schema_change,
        delta_table_uri=delta_table_uri,
        watermark_var=watermark_var,
    )

    if not boundary["has_new_commits"]:
      log.info(
          "No new Delta commits to synchronize (last_synced_version=%d, "
          "latest_version=%d).",
          last_synced_version,
          latest_version,
      )
      ti.xcom_push(key="has_new_commits", value=False)
      return boundary

    start_version = boundary["start_version"]
    end_version = boundary["end_version"]
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dt%H%M%S")
    job_name = (
        f"delta-to-lakehouse-cdc-v{start_version}-to-v{end_version}-{run_ts}"
    )
    boundary["job_name"] = job_name

    log.info(
        "Resolved inclusive Delta CDC commit boundary: startVersion=%d, "
        "endVersion=%d (last_synced_version=%d, latest_version=%d).",
        start_version,
        end_version,
        last_synced_version,
        latest_version,
    )

    ti.xcom_push(key="has_new_commits", value=True)
    ti.xcom_push(key="start_version", value=str(start_version))
    ti.xcom_push(key="end_version", value=str(end_version))
    ti.xcom_push(key="job_name", value=job_name)
    return boundary

  @task.branch(task_id="branch_on_new_commits")
  def branch_on_new_commits(boundary: dict[str, Any]) -> str:
    """Runs the Flex Template task only when new Delta commits exist."""
    if boundary.get("has_new_commits"):
      return "run_cdc_flex_template"
    return "skip_no_new_commits"

  run_cdc_flex_template = DataflowStartFlexTemplateOperator(
      task_id="run_cdc_flex_template",
      project_id="{{ params.project_id }}",
      location="{{ params.location }}",
      gcp_conn_id=GCP_CONN_ID,
      wait_until_finished=True,
      body={
          "launchParameter": {
              "jobName": (
                  "{{ ti.xcom_pull(task_ids='resolve_version_boundary', "
                  "key='job_name') }}"
              ),
              "containerSpecGcsPath": "{{ params.template_gcs_path }}",
              "parameters": {
                  "deltaTable": "{{ params.delta_table }}",
                  "lakehouseTable": "{{ params.lakehouse_table }}",
                  "equalityColumns": "{{ params.equality_columns }}",
                  "startVersion": (
                      "{{ ti.xcom_pull(task_ids='resolve_version_boundary', "
                      "key='start_version') }}"
                  ),
                  "endVersion": (
                      "{{ ti.xcom_pull(task_ids='resolve_version_boundary', "
                      "key='end_version') }}"
                  ),
                  "usePortableRunner": "{{ params.use_portable_runner }}",
              },
              "environment": {
                  "tempLocation": "{{ params.temp_location }}",
                  "stagingLocation": "{{ params.staging_location }}",
              },
          }
      },
  )

  @task(task_id="advance_version_watermark")
  def advance_version_watermark(
      boundary: dict[str, Any], **context: Any
  ) -> int:
    """Advances the Airflow Variable watermark only after Dataflow succeeds."""
    watermark_var: str = context["params"]["watermark_variable"]
    new_watermark = int(boundary["end_version"])
    Variable.set(watermark_var, str(new_watermark))
    log.info(
        "Advanced Airflow Variable '%s' to Delta commit version %d.",
        watermark_var,
        new_watermark,
    )
    return new_watermark

  skip_no_new_commits = EmptyOperator(task_id="skip_no_new_commits")

  boundary_info = resolve_version_boundary()
  branch_decision = branch_on_new_commits(boundary_info)
  branch_decision >> run_cdc_flex_template >> advance_version_watermark(
      boundary_info
  )
  branch_decision >> skip_no_new_commits
