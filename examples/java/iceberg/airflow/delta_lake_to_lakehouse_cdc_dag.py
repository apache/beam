#
# Licensed to the Apache Software Foundation (ASF) under one
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
1. Explicit Baseline Watermark: Requires either the Airflow watermark Variable
   to be initialized or ``initial_synced_version`` to be passed explicitly so a
   misconfigured run never silently replays commits from the beginning.
2. Inclusive Boundary Arithmetic: Both ``startVersion`` and ``endVersion`` in
   ``Managed.DELTA_LAKE_CDC`` are inclusive. Given ``last_synced_version``, the
   next run begins strictly at ``start_version = last_synced_version + 1``.
3. Explicit Upper-Bound Pinning: ``end_version`` is resolved from the source
   Delta Lake ``_delta_log`` before submitting the Dataflow job so concurrent
   writes during job execution cannot create gaps or duplicate commits.
4. No-Op Short-Circuiting: When ``start_version > latest_version``, the DAG
   branches cleanly to ``skip_no_new_commits`` without launching Dataflow.
5. CDF Validation & Schema-Evolution Gating: Verifies that
   ``delta.enableChangeDataFeed = true`` holds across the range. When
   ``stop_before_schema_change=True``, caps ``end_version`` at ``V - 1`` if a
   schema change occurs at commit ``V > start_version``, and raises an error
   when ``start_version == V`` until the operator applies the matching
   ``ALTER TABLE`` to Lakehouse and passes
   ``acknowledged_schema_change_version=V``.
6. End-to-End Idempotency & Atomic Watermark Advancement: ``max_active_runs=1``
   and ``retries=0`` prevent concurrent or blind automatic Dataflow re-launches.
   Before launching Dataflow, the resolved range and a deterministic
   range-scoped ``sink_id`` (``<dag_id>-v{start}-to-v{end}``) are persisted in a
   pending Airflow Variable. If a run fails after Iceberg commits but before the
   watermark advances, the next run reuses the exact pending range and
   ``sink_id``, allowing Beam's Iceberg CDC sink to recognize the already-
   committed ``sink_id`` token and skip duplicate commits cleanly before
   ``advance_version_watermark`` updates the watermark and clears the pending
   marker.
"""

from __future__ import annotations

import json
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
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartFlexTemplateOperator,
)

log = logging.getLogger(__name__)

GCP_CONN_ID = "google_cloud_default"
DEFAULT_WATERMARK_VAR = "delta_to_lakehouse_cdc_last_synced_version"


def pending_watermark_variable(watermark_var: str) -> str:
  """Returns the Airflow Variable key storing an in-flight CDC commit range."""
  return f"{watermark_var}_pending"


def build_sink_id(dag_id: str, start_version: int, end_version: int) -> str:
  """Builds a deterministic range-scoped Iceberg CDC sink_id."""
  return f"{dag_id}-v{start_version}-to-v{end_version}"


def resolve_last_synced_version(
    raw_watermark: str | None,
    initial_synced_version: int | None,
    watermark_var: str = DEFAULT_WATERMARK_VAR,
) -> int:
  """Resolves the last synced Delta commit version, failing fast if unset."""
  if raw_watermark is not None:
    return int(raw_watermark)
  if initial_synced_version is not None:
    return int(initial_synced_version)
  raise ValueError(
      f"Airflow Variable '{watermark_var}' is not set and "
      f"'initial_synced_version' was not provided. Set '{watermark_var}' "
      f"(for example, `airflow variables set {watermark_var} "
      f"<baseline_version>`) or explicitly pass 'initial_synced_version' in "
      f"DAG run conf so CDC starts strictly after your initial Lakehouse "
      f"snapshot."
  )


def parse_pending_range(
    raw_pending: str | None,
    last_synced_version: int,
    dag_id: str,
    pending_var: str,
    watermark_var: str = DEFAULT_WATERMARK_VAR,
) -> tuple[dict[str, Any] | None, bool]:
  """Inspects an in-flight pending CDC range for crash/retry recovery.

  Returns:
    A tuple ``(pending_range, should_clear_stale)``. When ``pending_range`` is
    not ``None``, the caller must reuse its exact ``[start_version,
    end_version]`` and ``sink_id`` so the Iceberg CDC sink's idempotency token
    prevents duplicate commits. When ``should_clear_stale`` is ``True``, the
    watermark has already advanced past the pending marker and the caller
    should delete ``pending_var`` before computing the next range.
  """
  if not raw_pending:
    return None, False

  data = json.loads(raw_pending)
  pending_start = int(data["start_version"])
  pending_end = int(data["end_version"])
  if pending_start > pending_end:
    raise ValueError(
        f"Invalid pending range in Airflow Variable '{pending_var}': "
        f"start_version={pending_start} > end_version={pending_end}."
    )
  pending_sink_id = data.get("sink_id") or build_sink_id(
      dag_id, pending_start, pending_end
  )

  if last_synced_version >= pending_end:
    log.warning(
        "Clearing stale pending range [%d, %d] in '%s' because watermark "
        "'%s' is already at version %d.",
        pending_start,
        pending_end,
        pending_var,
        watermark_var,
        last_synced_version,
    )
    return None, True

  if pending_start == last_synced_version + 1:
    return (
        {
            "start_version": pending_start,
            "end_version": pending_end,
            "sink_id": pending_sink_id,
        },
        False,
    )

  raise ValueError(
      f"Inconsistent CDC state: Airflow Variable '{watermark_var}' is "
      f"{last_synced_version}, but pending range Variable '{pending_var}' "
      f"contains [{pending_start}, {pending_end}] (expected start_version="
      f"{last_synced_version + 1}). Verify the target Iceberg table state "
      f"and reconcile '{watermark_var}' / '{pending_var}' before retrying."
  )


def compute_delta_version_boundary(
    last_synced_version: int,
    latest_version: int,
    get_version_info: Callable[[int], tuple[bool, str]],
    max_versions_per_run: int | None = None,
    stop_before_schema_change: bool = True,
    acknowledged_schema_change_version: int | None = None,
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
    stop_before_schema_change: When ``True``, caps ``end_version`` at ``V - 1``
      if a schema change occurs at commit ``V > start_version``, and raises
      ``ValueError`` when ``start_version`` itself introduces a schema change
      unless ``acknowledged_schema_change_version == start_version``.
    acknowledged_schema_change_version: Optional Delta commit version for which
      the operator confirms the corresponding ``ALTER TABLE`` has been applied
      to the target Lakehouse Iceberg table.
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
      if (
          stop_before_schema_change
          and acknowledged_schema_change_version != start_version
      ):
        raise ValueError(
            f"Detected Delta schema change at start_version={start_version} "
            f"(compared to last_synced_version={last_synced_version}) on "
            f"'{delta_table_uri}'. Apply the matching ALTER TABLE schema "
            f"update to the target Lakehouse Iceberg table, then trigger a run "
            f"with 'acknowledged_schema_change_version={start_version}' (or "
            f"set 'stop_before_schema_change=false' to disable schema gating)."
        )
      if acknowledged_schema_change_version == start_version:
        log.info(
            "Proceeding across acknowledged Delta schema change at "
            "start_version=%d (last_synced_version=%d).",
            start_version,
            last_synced_version,
        )
      else:
        log.warning(
            "Commit start_version=%d has a different Delta schema than "
            "last_synced_version=%d (stop_before_schema_change=False). "
            "Ensure the target Lakehouse Iceberg table schema matches.",
            start_version,
            last_synced_version,
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
  """Builds delta-rs GCS storage_options from Airflow's GCP connection or ADC.

  Uses explicit service-account credentials from the Airflow GCP connection
  (``key_path`` or inline ``keyfile_dict``) or ``GOOGLE_APPLICATION_CREDENTIALS``
  when available. Otherwise returns ``{}`` so ``delta-rs`` natively uses
  Application Default Credentials / Cloud Composer Workload Identity / GCE
  Metadata Server authentication (including automatic token refresh).
  """
  try:
    conn = BaseHook.get_connection(gcp_conn_id)
    extra = conn.extra_dejson or {}
    key_path = (
        extra.get("key_path")
        or extra.get("extra__google_cloud_platform__key_path")
    )
    if key_path and os.path.exists(key_path):
      return {"google_service_account_path": key_path}

    keyfile_dict = (
        extra.get("keyfile_dict")
        or extra.get("extra__google_cloud_platform__keyfile_dict")
    )
    if keyfile_dict:
      keyfile_json = (
          json.dumps(keyfile_dict)
          if isinstance(keyfile_dict, dict)
          else str(keyfile_dict)
      )
      return {"google_service_account_key": keyfile_json}
  except Exception as exc:
    log.debug(
        "Could not read credentials from Airflow connection '%s': %s",
        gcp_conn_id,
        exc,
    )

  env_key = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
  if env_key and os.path.exists(env_key):
    return {"google_service_account_path": env_key}

  # Cloud Composer / Workload Identity / ADC fallback handled natively by
  # delta-rs's GCS object_store client.
  return {}


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
        # Disable automatic task retries to avoid blindly re-launching Dataflow
        # jobs if a failure occurs after commit; combined with the pending range
        # Variable + deterministic Iceberg sink_id, the next run recovers safely
        # without duplicating commits.
        "retries": 0,
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
            None,
            type=["null", "integer"],
            description=(
                "Baseline Delta commit version already covered by the initial "
                "snapshot / CDF enablement. Required on the first run when the "
                "Airflow watermark Variable has not been initialized yet (the "
                "DAG fails fast if neither is set, preventing accidental "
                "replay from the beginning)."
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
                "If true, caps end_version at V - 1 when a Delta schema change "
                "occurs at commit V > start_version, and raises an error when "
                "start_version == V until acknowledged via "
                "acknowledged_schema_change_version."
            ),
        ),
        "acknowledged_schema_change_version": Param(
            None,
            type=["null", "integer"],
            description=(
                "Set to commit version V for a single run after applying the "
                "corresponding ALTER TABLE to the target Lakehouse Iceberg "
                "table, allowing CDC to proceed past the schema change at V "
                "while keeping schema-change gating active for later commits."
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
    dag_id: str = context["dag"].dag_id
    delta_table_uri: str = params["delta_table"]
    watermark_var: str = params["watermark_variable"]
    pending_var: str = pending_watermark_variable(watermark_var)
    raw_initial = params.get("initial_synced_version")
    initial_synced_version: int | None = (
        int(raw_initial) if raw_initial is not None else None
    )
    max_versions_per_run: int | None = params.get("max_versions_per_run")
    stop_before_schema_change: bool = bool(
        params.get("stop_before_schema_change", True)
    )
    raw_ack = params.get("acknowledged_schema_change_version")
    acknowledged_schema_change_version: int | None = (
        int(raw_ack) if raw_ack is not None else None
    )

    raw_watermark = Variable.get(watermark_var, default_var=None)
    last_synced_version = resolve_last_synced_version(
        raw_watermark=raw_watermark,
        initial_synced_version=initial_synced_version,
        watermark_var=watermark_var,
    )
    if raw_watermark is None:
      log.info(
          "Airflow Variable '%s' is not set yet; using explicit "
          "initial_synced_version=%d.",
          watermark_var,
          last_synced_version,
      )
    else:
      log.info(
          "Loaded last_synced_version=%d from Airflow Variable '%s'.",
          last_synced_version,
          watermark_var,
      )

    # Check whether a previous run saved an in-flight pending commit range.
    # Reusing the exact pending range + deterministic sink_id allows the Iceberg
    # CDC sink to recognize and skip commits that already landed before a
    # failure in Airflow.
    raw_pending = Variable.get(pending_var, default_var=None)
    pending_range, should_clear_stale = parse_pending_range(
        raw_pending=raw_pending,
        last_synced_version=last_synced_version,
        dag_id=dag_id,
        pending_var=pending_var,
        watermark_var=watermark_var,
    )
    if should_clear_stale:
      Variable.delete(pending_var)

    if pending_range is not None:
      start_version = pending_range["start_version"]
      end_version = pending_range["end_version"]
      sink_id = pending_range["sink_id"]
      run_ts = datetime.now(timezone.utc).strftime("%Y%m%dt%H%M%S")
      job_name = (
          f"delta-to-lakehouse-cdc-v{start_version}-to-v{end_version}-{run_ts}"
      )
      boundary = {
          "has_new_commits": True,
          "last_synced_version": last_synced_version,
          "start_version": start_version,
          "end_version": end_version,
          "sink_id": sink_id,
          "job_name": job_name,
          "reused_pending": True,
      }
      log.warning(
          "Reusing pending Delta CDC commit range [%d, %d] with sink_id='%s' "
          "from Airflow Variable '%s' to preserve idempotency.",
          start_version,
          end_version,
          sink_id,
          pending_var,
      )
      ti.xcom_push(key="has_new_commits", value=True)
      ti.xcom_push(key="start_version", value=str(start_version))
      ti.xcom_push(key="end_version", value=str(end_version))
      ti.xcom_push(key="sink_id", value=sink_id)
      ti.xcom_push(key="job_name", value=job_name)
      return boundary

    storage_options = _build_delta_storage_options(GCP_CONN_ID)
    dt = DeltaTable(delta_table_uri, storage_options=storage_options)
    latest_version = int(dt.version())
    log.info(
        "Source Delta Lake table '%s' is currently at latest_version=%d.",
        delta_table_uri,
        latest_version,
    )

    def _extract_version_info(table: Any) -> tuple[bool, str]:
      cdf_prop = table.metadata().configuration.get(
          "delta.enableChangeDataFeed", "false"
      )
      return str(cdf_prop).lower() == "true", table.schema().to_json()

    # Cache the already-loaded latest version and reuse the same DeltaTable
    # instance via load_as_version() to avoid re-creating GCS clients per
    # commit version.
    version_info_cache: dict[int, tuple[bool, str]] = {
        latest_version: _extract_version_info(dt)
    }

    def _get_version_info(version: int) -> tuple[bool, str]:
      if version not in version_info_cache:
        dt.load_as_version(version)
        version_info_cache[version] = _extract_version_info(dt)
      return version_info_cache[version]

    boundary = compute_delta_version_boundary(
        last_synced_version=last_synced_version,
        latest_version=latest_version,
        get_version_info=_get_version_info,
        max_versions_per_run=max_versions_per_run,
        stop_before_schema_change=stop_before_schema_change,
        acknowledged_schema_change_version=acknowledged_schema_change_version,
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
    sink_id = build_sink_id(dag_id, start_version, end_version)
    run_ts = datetime.now(timezone.utc).strftime("%Y%m%dt%H%M%S")
    job_name = (
        f"delta-to-lakehouse-cdc-v{start_version}-to-v{end_version}-{run_ts}"
    )
    boundary["sink_id"] = sink_id
    boundary["job_name"] = job_name
    boundary["reused_pending"] = False

    # Persist the pending range + sink_id BEFORE launching Dataflow so any
    # retry/next run after an Airflow failure reuses the same range and sink_id.
    Variable.set(
        pending_var,
        json.dumps({
            "start_version": start_version,
            "end_version": end_version,
            "sink_id": sink_id,
        }),
    )

    log.info(
        "Resolved inclusive Delta CDC commit boundary: startVersion=%d, "
        "endVersion=%d, sinkId='%s' (last_synced_version=%d, "
        "latest_version=%d).",
        start_version,
        end_version,
        sink_id,
        last_synced_version,
        latest_version,
    )

    ti.xcom_push(key="has_new_commits", value=True)
    ti.xcom_push(key="start_version", value=str(start_version))
    ti.xcom_push(key="end_version", value=str(end_version))
    ti.xcom_push(key="sink_id", value=sink_id)
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
                  "sinkId": (
                      "{{ ti.xcom_pull(task_ids='resolve_version_boundary', "
                      "key='sink_id') }}"
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
    """Advances the watermark and clears the pending range after success."""
    watermark_var: str = context["params"]["watermark_variable"]
    pending_var: str = pending_watermark_variable(watermark_var)
    new_watermark = int(boundary["end_version"])
    # Advance the watermark before deleting the pending marker so a crash
    # between the two operations is always recoverable without duplicates.
    Variable.set(watermark_var, str(new_watermark))
    Variable.delete(pending_var)
    log.info(
        "Advanced Airflow Variable '%s' to Delta commit version %d and cleared "
        "pending marker '%s'.",
        watermark_var,
        new_watermark,
        pending_var,
    )
    return new_watermark

  skip_no_new_commits = EmptyOperator(task_id="skip_no_new_commits")

  boundary_info = resolve_version_boundary()
  branch_decision = branch_on_new_commits(boundary_info)
  branch_decision >> run_cdc_flex_template >> advance_version_watermark(
      boundary_info
  )
  branch_decision >> skip_no_new_commits
