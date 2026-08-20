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

"""A PTransform for integrating Vertex AI Model Monitoring v2 with Apache Beam RunInference.

Vertex AI Model Monitoring v2 provides drift and skew detection on arbitrary
models by evaluating input features, predictions, and attribution stats logged
to BigQuery against a training baseline.
"""

import logging
import time
from collections.abc import Callable
from typing import Any
from typing import Optional
from typing import Union

import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteResult
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.transforms.util import WaitOn

try:
  from google.api_core import exceptions
  from vertexai.resources.preview import ml_monitoring
except ImportError:
  exceptions = None
  ml_monitoring = None

__all__ = [
    'VertexModelMonitoringV2',
]


class _V2JobManager(beam.DoFn):
  """Base DoFn for managing Vertex AI Model Monitoring v2 lifecycle."""
  def __init__(
      self,
      project_id: str,
      location: str,
      display_name: str,
      model_name: str,
      model_version_id: str,
      model_monitoring_schema: Any,
      training_dataset: Any,
      tabular_objective_spec: Any,
      model_monitor_id: Optional[str] = None,
      explanation_spec: Optional[Any] = None,
      output_spec: Optional[Any] = None,
      notification_spec: Optional[Any] = None,
      credentials: Optional[Any] = None,
      **kwargs,
  ):
    self.project_id = project_id
    self.location = location
    self.display_name = display_name
    self.model_name = model_name
    self.model_version_id = model_version_id
    self.model_monitoring_schema = model_monitoring_schema
    self.training_dataset = training_dataset
    self.tabular_objective_spec = tabular_objective_spec
    self.model_monitor_id = model_monitor_id
    self.explanation_spec = explanation_spec
    self.output_spec = output_spec
    self.notification_spec = notification_spec
    self.credentials = credentials
    self.manager = None
    self.kwargs = kwargs

  def create_model_monitor(self):
    """Creates a ModelMonitor with a deterministic ID or retrieves existing one."""
    if ml_monitoring is None:
      raise ImportError(
          'Vertex AI Model Monitoring v2 dependencies are not installed.')

    try:
      return ml_monitoring.model_monitors.ModelMonitor.create(
          model_name=self.model_name,
          model_version_id=self.model_version_id,
          training_dataset=self.training_dataset,
          display_name=self.display_name,
          model_monitoring_schema=self.model_monitoring_schema,
          tabular_objective_spec=self.tabular_objective_spec,
          output_spec=self.output_spec,
          notification_spec=self.notification_spec,
          explanation_spec=self.explanation_spec,
          project=self.project_id,
          location=self.location,
          credentials=self.credentials,
          model_monitor_id=self.model_monitor_id,
          **self.kwargs,
      )
    except (exceptions.AlreadyExists, exceptions.Conflict) as e:
      if isinstance(e, exceptions.Conflict):
        time.sleep(15)
      logging.info(
          "Model monitor '%s' already exists; retrieving existing instance.",
          self.model_monitor_id or self.display_name,
      )
      if self.model_monitor_id:
        return ml_monitoring.model_monitors.ModelMonitor(
            model_monitor_name=self.model_monitor_id,
            project=self.project_id,
            location=self.location,
            credentials=self.credentials,
        )
      monitors = ml_monitoring.model_monitors.ModelMonitor.list(
          filter=f'display_name="{self.display_name}"',
          project=self.project_id,
          location=self.location,
          credentials=self.credentials,
      )
      if monitors:
        return monitors[0]
      raise


class _V2JobManagerBatch(_V2JobManager):
  """DoFn to manage batch / ad-hoc monitoring jobs."""
  def __init__(
      self,
      target_dataset: Any,
      monitoring_job_display_name: str,
      **kwargs,
  ):
    super().__init__(**kwargs)
    self.target_dataset = target_dataset
    self.monitoring_job_display_name = monitoring_job_display_name

  def setup(self):
    self.manager = self.create_model_monitor()

  def process(self, element):
    try:
      job = self.manager.run(
          target_dataset=self.target_dataset,
          display_name=self.monitoring_job_display_name,
      )
      # Ensure the background job creation RPC completes on Vertex AI before the DoFn finishes.
      for _ in range(60):
        if getattr(job, '_gca_resource', None) is not None:
          break
        time.sleep(0.5)
      else:
        if getattr(job, '_gca_resource', None) is None:
          logging.warning(
              "Model monitoring job '%s' submitted but confirmation timed out.",
              self.monitoring_job_display_name,
          )
    except (exceptions.AlreadyExists, exceptions.Conflict):
      logging.warning(
          "Monitoring job '%s' already submitted; skipping duplicate run.",
          self.monitoring_job_display_name,
      )


class _V2JobManagerStreaming(_V2JobManager):
  """DoFn to manage continuous scheduled monitoring jobs for streaming."""
  def __init__(
      self,
      target_dataset: Any,
      schedule_display_name: str,
      cron: Optional[str] = None,
      monitoring_job_display_name: Optional[str] = None,
      start_time: Optional[Any] = None,
      end_time: Optional[Any] = None,
      **kwargs,
  ):
    super().__init__(**kwargs)
    self.target_dataset = target_dataset
    self.cron = cron
    self.schedule_display_name = schedule_display_name
    self.monitoring_job_display_name = monitoring_job_display_name
    self.start_time = start_time
    self.end_time = end_time

  def setup(self):
    self.manager = self.create_model_monitor()

  def _schedule_already_exists(self) -> bool:
    """Checks if an identical schedule already exists on the model monitor."""
    try:
      existing_schedules = self.manager.list_schedules()
      if not existing_schedules:
        return False
      for schedule in existing_schedules:
        sched_display_name = getattr(schedule, 'display_name', None)
        sched_cron = getattr(schedule, 'cron', None)
        if isinstance(schedule, dict):
          sched_display_name = schedule.get('display_name', sched_display_name)
          sched_cron = schedule.get('cron', sched_cron)
        if (sched_display_name == self.schedule_display_name and
            (self.cron is None or sched_cron == self.cron)):
          return True
    except Exception as e:
      logging.warning(
          "Failed to list existing schedules: %s. Attempting creation.", e)
    return False

  def process(self, element):
    # Ignore schedule creation if a corresponding one already exists (e.g.
    # multiple streaming pipelines utilize the same model and write to the
    # same BigQuery table for monitoring.)
    if self._schedule_already_exists():
      logging.info(
          "Schedule '%s'%s already exists; skipping schedule creation.",
          self.schedule_display_name,
          f" with cron '{self.cron}'" if self.cron else "",
      )
      return
    # No cron provided, but no schedule exists either so no monitoring jobs
    # will be executed.
    elif not self.cron:
      raise ValueError(
          "No cron schedule provided for VertexModelMonitoringV2 in "
          "streaming pipeline and no pre-existing schedule was found. "
          "Provide a cron schedule or create a model monitor manually before "
          "pipeline execution.")

    try:
      self.manager.create_schedule(
          cron=self.cron,
          target_dataset=self.target_dataset,
          display_name=self.schedule_display_name,
          model_monitoring_job_display_name=self.monitoring_job_display_name,
          start_time=self.start_time,
          end_time=self.end_time,
          tabular_objective_spec=self.tabular_objective_spec,
          baseline_dataset=self.training_dataset,
          output_spec=self.output_spec,
          notification_spec=self.notification_spec,
          explanation_spec=self.explanation_spec,
      )
    # Catch race condition between two workers trying to create the schedule.
    except (exceptions.AlreadyExists, exceptions.Conflict):
      logging.info(
          "Schedule '%s' already exists; skipping schedule creation.",
          self.schedule_display_name,
      )


class VertexModelMonitoringV2(
    beam.PTransform[beam.PCollection[PredictionResult],
                    beam.PCollection[PredictionResult]]):
  """A composite PTransform that exports inference outputs to BigQuery and coordinates
  Vertex AI Model Monitoring v2 jobs.

  In batch pipelines, it blocks until inference records are committed to BigQuery
  before triggering an asynchronous ad-hoc monitoring job. In streaming pipelines,
  it provisions a recurring monitoring schedule at startup.
  """
  def __init__(
      self,
      project_id: str,
      location: str,
      display_name: str,
      model_name: str,
      model_version_id: str,
      model_monitoring_schema: Any,
      training_dataset: Any,
      tabular_objective_spec: Any,
      target_dataset: Any,
      unpack_fn: Callable[[PredictionResult], dict[str, Any]],
      bigquery_table: str,
      bigquery_schema: Optional[Union[str, dict[str, Any]]] = None,
      write_to_bigquery_kwargs: Optional[dict[str, Any]] = None,
      model_monitor_id: Optional[str] = None,
      cron: Optional[str] = None,
      schedule_display_name: Optional[str] = None,
      monitoring_job_display_name: Optional[str] = None,
      explanation_spec: Optional[Any] = None,
      output_spec: Optional[Any] = None,
      notification_spec: Optional[Any] = None,
      credentials: Optional[Any] = None,
      start_time: Optional[Any] = None,
      end_time: Optional[Any] = None,
      **kwargs,
  ):
    """
    Args:
      project_id: GCP project ID where the model monitor is created.
      location: GCP location/region (e.g. 'us-central1').
      display_name: User-visible display name for the model monitor.
      model_name: Resource name or ID of the monitored model.
      model_version_id: Version ID of the model.
      model_monitoring_schema: Schema specification describing input and output features.
      training_dataset: Baseline dataset specification (e.g. Training dataset).
      tabular_objective_spec: Drift and skew objective parameters.
      target_dataset: Target dataset specification pointing to production BigQuery logs.
      unpack_fn: Callable converting PredictionResult into a dictionary matching BigQuery table schema.
      bigquery_table: Destination BigQuery table spec in the format 'project:dataset.table' or 'dataset.table'.
      bigquery_schema: BigQuery schema definition for the destination table.
      write_to_bigquery_kwargs: Optional dictionary of keyword arguments passed to WriteToBigQuery.
      model_monitor_id: Optional deterministic resource ID for the model monitor.
        If omitted, Vertex AI generates an ID automatically.
      cron: Cron expression defining the recurring schedule for streaming pipelines (e.g. '@daily', '0 * * * *').
        Required for streaming pipelines.
      schedule_display_name: Display name for the streaming monitoring schedule.
      monitoring_job_display_name: Display name for the monitoring job.
      explanation_spec: Optional feature attribution monitoring specification.
      output_spec: Optional output specification for monitoring statistics.
      notification_spec: Optional alerting and notification configuration.
      credentials: Optional google.auth credentials.
      start_time: Optional start timestamp for streaming schedule.
      end_time: Optional end timestamp for streaming schedule.
    """
    self.project_id = project_id
    self.location = location
    self.display_name = display_name
    self.model_name = model_name
    self.model_version_id = model_version_id
    self.model_monitoring_schema = model_monitoring_schema
    self.training_dataset = training_dataset
    self.tabular_objective_spec = tabular_objective_spec
    self.target_dataset = target_dataset
    self.unpack_fn = unpack_fn
    self.bigquery_table = bigquery_table
    self.bigquery_schema = bigquery_schema
    self.write_to_bigquery_kwargs = write_to_bigquery_kwargs or {}
    self.model_monitor_id = model_monitor_id
    self.cron = cron
    self.schedule_display_name = schedule_display_name
    self.monitoring_job_display_name = monitoring_job_display_name
    self.explanation_spec = explanation_spec
    self.output_spec = output_spec
    self.notification_spec = notification_spec
    self.credentials = credentials
    self.start_time = start_time
    self.end_time = end_time
    self.kwargs = kwargs

  def annotations(self) -> dict[str, Any]:
    return {
        'model_identifier': '',
        **super().annotations(),
    }

  def expand(
      self, pcoll: beam.PCollection[PredictionResult]
  ) -> beam.PCollection[PredictionResult]:
    if ml_monitoring is None:
      raise ImportError(
          'Vertex AI Model Monitoring v2 dependencies are not installed.')

    pipeline = pcoll.pipeline
    is_streaming = pipeline.options.view_as(StandardOptions).streaming

    # 1. Unpack PredictionResult records for BigQuery
    bq_rows = pcoll | 'UnpackPredictionResult' >> beam.Map(self.unpack_fn)

    # 2. Write rows to BigQuery
    written = bq_rows | 'WriteToBigQuery' >> WriteToBigQuery(
        table=self.bigquery_table,
        schema=self.bigquery_schema,
        **self.write_to_bigquery_kwargs,
    )

    if is_streaming:
      if not self.cron:
        logging.warning(
            'A cron schedule was not provided, so a new monitoring job will '
            'not be created. Inferences will still be written to the BigQuery '
            f'table {self.bigquery_table}. This configuration will fail if '
            'a pre-existing model monitoring schedule does not already exist.')
      manager = _V2JobManagerStreaming(
          project_id=self.project_id,
          location=self.location,
          display_name=self.display_name,
          model_name=self.model_name,
          model_version_id=self.model_version_id,
          model_monitoring_schema=self.model_monitoring_schema,
          training_dataset=self.training_dataset,
          tabular_objective_spec=self.tabular_objective_spec,
          target_dataset=self.target_dataset,
          model_monitor_id=self.model_monitor_id,
          cron=self.cron,
          schedule_display_name=(
              self.schedule_display_name or f'{self.display_name}_schedule'),
          monitoring_job_display_name=self.monitoring_job_display_name,
          explanation_spec=self.explanation_spec,
          output_spec=self.output_spec,
          notification_spec=self.notification_spec,
          credentials=self.credentials,
          start_time=self.start_time,
          end_time=self.end_time,
          **self.kwargs,
      )
      _ = (
          pipeline
          | 'StreamingImpulse' >> beam.Impulse()
          | 'CreateMonitoringSchedule' >> beam.ParDo(manager))
    else:
      manager = _V2JobManagerBatch(
          project_id=self.project_id,
          location=self.location,
          display_name=self.display_name,
          model_name=self.model_name,
          model_version_id=self.model_version_id,
          model_monitoring_schema=self.model_monitoring_schema,
          training_dataset=self.training_dataset,
          tabular_objective_spec=self.tabular_objective_spec,
          target_dataset=self.target_dataset,
          model_monitor_id=self.model_monitor_id,
          monitoring_job_display_name=(
              self.monitoring_job_display_name or f'{self.display_name}_job'),
          explanation_spec=self.explanation_spec,
          output_spec=self.output_spec,
          notification_spec=self.notification_spec,
          credentials=self.credentials,
          **self.kwargs,
      )

      # Handle WriteResult from WriteToBigQuery to extract completion PCollection for WaitOn
      if isinstance(written, beam.pvalue.PCollection):
        wait_target = written
      elif isinstance(written, WriteResult):
        # Extract destination load job id pairs from batch file loads
        wait_target = None
        if hasattr(written, '_destination_load_jobid_pairs'
                   ) and written._destination_load_jobid_pairs is not None:
          try:
            wait_target = written.destination_load_jobid_pairs
          except AttributeError:
            wait_target = written._destination_load_jobid_pairs
        elif hasattr(written, '_destination_copy_jobid_pairs'
                     ) and written._destination_copy_jobid_pairs is not None:
          try:
            wait_target = written.destination_copy_jobid_pairs
          except AttributeError:
            wait_target = written._destination_copy_jobid_pairs
        if wait_target is None:
          wait_target = bq_rows
      else:
        wait_target = bq_rows

      _ = (
          pipeline
          | 'Impulse' >> beam.Impulse()
          | 'WaitOnBigQueryWrite' >> WaitOn(wait_target)
          | 'ManageModelMonitoring' >> beam.ParDo(manager))

    return pcoll
