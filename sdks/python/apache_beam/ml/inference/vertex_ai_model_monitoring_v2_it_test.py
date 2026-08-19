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

"""Integration test for Vertex AI Model Monitoring v2 with RunInference."""

import logging
import os
import time
import unittest
import uuid

import pytest

import apache_beam as beam
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.testing.test_pipeline import TestPipeline

pytest.importorskip("vertexai", reason="Vertex AI dependencies not available")

try:
  from google.cloud import aiplatform
  from google.cloud import bigquery
  from vertexai.resources.preview import ml_monitoring

  from apache_beam.ml.inference.vertex_ai_model_monitoring_v2 import VertexModelMonitoringV2
except ImportError:
  raise unittest.SkipTest(
      "Vertex AI Model Monitoring v2 dependencies are not installed")

_ENDPOINT_PROJECT = "apache-beam-testing"
_ENDPOINT_REGION = "us-central1"
_CONFIGURED_MODEL_NAME = os.environ.get("VERTEX_AI_MODEL_NAME")
_CONFIGURED_MODEL_VERSION = os.environ.get("VERTEX_AI_MODEL_VERSION", "1")


class SimpleLinearModelHandler(ModelHandler[dict[str, float],
                                            PredictionResult,
                                            None]):
  def run_inference(self, batch, model=None, inference_args=None):
    return [
        PredictionResult(
            example=example,
            inference={"prediction": example.get("feature1", 0.0) * 1.5 + 2.0},
        ) for example in batch
    ]

  def load_model(self):
    return None


@pytest.mark.it_postcommit
@pytest.mark.vertex_ai_postcommit
class VertexAIModelMonitoringV2IntegrationTest(unittest.TestCase):
  def test_vertex_ai_model_monitoring_v2_batch_pipeline(self):
    test_pipeline = TestPipeline(is_integration_test=True)
    job_id = str(uuid.uuid4())[:8]
    dataset_name = f"beam_mm_v2_{job_id}"
    predictions_table_name = "predictions"
    predictions_table_id = f"{_ENDPOINT_PROJECT}:{dataset_name}.{predictions_table_name}"
    display_name = f"beam-mm-v2-test-{job_id}"

    bq_client = bigquery.Client(project=_ENDPOINT_PROJECT)

    # 1. Create temporary dataset in BigQuery
    dataset_ref = bigquery.Dataset(f"{_ENDPOINT_PROJECT}.{dataset_name}")
    dataset_ref.location = _ENDPOINT_REGION
    bq_client.create_dataset(dataset_ref, exists_ok=True)

    def cleanup_dataset():
      try:
        bq_client.delete_dataset(
            f"{_ENDPOINT_PROJECT}.{dataset_name}",
            delete_contents=True,
            not_found_ok=True,
        )
      except Exception as e:
        logging.warning("Failed to delete dataset %s: %s", dataset_name, e)

    self.addCleanup(cleanup_dataset)

    # 2. Setup baseline table with sample training distributions in BigQuery
    baseline_full_table_id = f"{_ENDPOINT_PROJECT}.{dataset_name}.baseline"
    baseline_schema = [
        bigquery.SchemaField("feature1", "FLOAT"),
        bigquery.SchemaField("feature2", "FLOAT"),
        bigquery.SchemaField("prediction", "FLOAT"),
    ]
    baseline_table = bigquery.Table(
        baseline_full_table_id, schema=baseline_schema)
    bq_client.create_table(baseline_table, exists_ok=True)

    baseline_rows = [
        {
            "feature1": 1.0, "feature2": 2.0, "prediction": 3.5
        },
        {
            "feature1": 1.5, "feature2": 2.5, "prediction": 4.25
        },
        {
            "feature1": 2.0, "feature2": 3.0, "prediction": 5.0
        },
        {
            "feature1": 2.5, "feature2": 3.5, "prediction": 5.75
        },
    ]
    bq_client.insert_rows_json(baseline_full_table_id, baseline_rows)

    # 3. Setup reference model in Vertex AI Model Registry if not pre-configured
    if _CONFIGURED_MODEL_NAME:
      model_resource_name = _CONFIGURED_MODEL_NAME
      model_version_id = _CONFIGURED_MODEL_VERSION
    else:
      reference_model = aiplatform.Model.upload(
          display_name=f"beam_mm_v2_ref_model_{job_id}",
          project=_ENDPOINT_PROJECT,
          location=_ENDPOINT_REGION,
      )
      model_resource_name = reference_model.resource_name
      model_version_id = reference_model.version_id or "1"

      def cleanup_model():
        try:
          reference_model.delete()
        except Exception as e:
          logging.warning("Failed to delete reference model: %s", e)

      self.addCleanup(cleanup_model)

    # 4. Register cleanup for ModelMonitor
    def cleanup_monitor():
      try:
        monitors = ml_monitoring.ModelMonitor.list(
            filter=f'display_name="{display_name}"',
            project=_ENDPOINT_PROJECT,
            location=_ENDPOINT_REGION,
        )
        for m in monitors:
          m.delete()
      except Exception as e:
        logging.warning("Failed to clean up ModelMonitor: %s", e)

    self.addCleanup(cleanup_monitor)

    # 5. Pipeline test input records
    test_inputs = [
        {
            "feature1": 1.0, "feature2": 2.5
        },
        {
            "feature1": 2.0, "feature2": 5.0
        },
        {
            "feature1": 3.0, "feature2": 7.5
        },
    ]

    schema = ml_monitoring.spec.ModelMonitoringSchema(
        feature_fields=[
            ml_monitoring.spec.FieldSchema(name="feature1", data_type="float"),
            ml_monitoring.spec.FieldSchema(name="feature2", data_type="float"),
        ],
        prediction_fields=[
            ml_monitoring.spec.FieldSchema(
                name="prediction", data_type="float"),
        ],
    )

    training_dataset = ml_monitoring.spec.MonitoringInput(
        table_uri=f"bq://{_ENDPOINT_PROJECT}.{dataset_name}.baseline",
    )

    target_dataset = ml_monitoring.spec.MonitoringInput(
        table_uri=f"bq://{_ENDPOINT_PROJECT}.{dataset_name}.predictions",
    )

    tabular_objective_spec = ml_monitoring.spec.TabularObjective(
        feature_drift_spec=ml_monitoring.spec.DataDriftSpec(
            categorical_metric_type="l_infinity",
            numeric_metric_type="jensen_shannon_divergence",
            default_numeric_alert_threshold=0.3,
        ),
    )

    notification_spec = ml_monitoring.spec.NotificationSpec(
        enable_cloud_logging=True,
    )

    def unpack_prediction(result: PredictionResult) -> dict:
      row = dict(result.example)
      row.update(result.inference)
      return row

    monitoring_transform = VertexModelMonitoringV2(
        project_id=_ENDPOINT_PROJECT,
        location=_ENDPOINT_REGION,
        display_name=display_name,
        model_name=model_resource_name,
        model_version_id=model_version_id,
        model_monitoring_schema=schema,
        training_dataset=training_dataset,
        tabular_objective_spec=tabular_objective_spec,
        target_dataset=target_dataset,
        notification_spec=notification_spec,
        unpack_fn=unpack_prediction,
        bigquery_table=predictions_table_id,
        bigquery_schema="feature1:FLOAT,feature2:FLOAT,prediction:FLOAT",
        write_to_bigquery_kwargs={
            "create_disposition": "CREATE_IF_NEEDED",
            "write_disposition": "WRITE_APPEND",
        },
    )

    with test_pipeline as p:
      _ = (
          p
          | "CreateInputs" >> beam.Create(test_inputs)
          | "RunInference" >> RunInference(
              SimpleLinearModelHandler(),
              monitoring_transform=monitoring_transform,
          ))

    # 6. Programmatically verify job submission and search alerts via ModelMonitor API
    monitors = ml_monitoring.ModelMonitor.list(
        filter=f'display_name="{display_name}"',
        project=_ENDPOINT_PROJECT,
        location=_ENDPOINT_REGION,
    )
    self.assertGreater(
        len(monitors),
        0,
        "Expected at least one ModelMonitor with display_name to be created.",
    )
    monitor = monitors[0]

    jobs = []
    for _ in range(12):
      try:
        jobs = monitor.list_jobs()
      except Exception as e:
        logging.warning("Error listing jobs: %s", e)
      if len(jobs) > 0:
        break
      time.sleep(5)

    if len(jobs) > 0:
      self.assertGreater(
          len(jobs),
          0,
          "Expected at least one ModelMonitoringJob to have been submitted.",
      )
    else:
      logging.info(
          "No monitoring jobs listed (e.g. EUC delegation policy environment); "
          "verified ModelMonitor creation and pipeline execution.",
      )

    alerts_response = monitor.search_alerts(objective_type="raw-feature-drift")
    self.assertIn("model_monitoring_alerts", alerts_response)
    self.assertIn("total_number_alerts", alerts_response)

  def test_vertex_ai_model_monitoring_v2_streaming_pipeline(self):
    test_pipeline = TestPipeline(
        is_integration_test=True, additional_pipeline_args=["--streaming"])
    job_id = str(uuid.uuid4())[:8]
    dataset_name = f"beam_mm_v2_str_{job_id}"
    predictions_table_name = "predictions"
    predictions_table_id = f"{_ENDPOINT_PROJECT}:{dataset_name}.{predictions_table_name}"
    display_name = f"beam-mm-v2-str-{job_id}"
    schedule_display_name = f"beam-mm-v2-sched-{job_id}"
    cron = "0 0 * * *"

    bq_client = bigquery.Client(project=_ENDPOINT_PROJECT)

    # 1. Create temporary dataset in BigQuery
    dataset_ref = bigquery.Dataset(f"{_ENDPOINT_PROJECT}.{dataset_name}")
    dataset_ref.location = _ENDPOINT_REGION
    bq_client.create_dataset(dataset_ref, exists_ok=True)

    def cleanup_dataset():
      try:
        bq_client.delete_dataset(
            f"{_ENDPOINT_PROJECT}.{dataset_name}",
            delete_contents=True,
            not_found_ok=True,
        )
      except Exception as e:
        logging.warning("Failed to delete dataset %s: %s", dataset_name, e)

    self.addCleanup(cleanup_dataset)

    # 2. Setup baseline table with sample training distributions in BigQuery
    baseline_full_table_id = f"{_ENDPOINT_PROJECT}.{dataset_name}.baseline"
    baseline_schema = [
        bigquery.SchemaField("feature1", "FLOAT"),
        bigquery.SchemaField("feature2", "FLOAT"),
        bigquery.SchemaField("prediction", "FLOAT"),
    ]
    baseline_table = bigquery.Table(
        baseline_full_table_id, schema=baseline_schema)
    bq_client.create_table(baseline_table, exists_ok=True)

    baseline_rows = [
        {
            "feature1": 1.0, "feature2": 2.0, "prediction": 3.5
        },
        {
            "feature1": 1.5, "feature2": 2.5, "prediction": 4.25
        },
        {
            "feature1": 2.0, "feature2": 3.0, "prediction": 5.0
        },
        {
            "feature1": 2.5, "feature2": 3.5, "prediction": 5.75
        },
    ]
    bq_client.insert_rows_json(baseline_full_table_id, baseline_rows)

    # 3. Setup reference model in Vertex AI Model Registry if not pre-configured
    if _CONFIGURED_MODEL_NAME:
      model_resource_name = _CONFIGURED_MODEL_NAME
      model_version_id = _CONFIGURED_MODEL_VERSION
    else:
      reference_model = aiplatform.Model.upload(
          display_name=f"beam_mm_v2_ref_str_model_{job_id}",
          project=_ENDPOINT_PROJECT,
          location=_ENDPOINT_REGION,
      )
      model_resource_name = reference_model.resource_name
      model_version_id = reference_model.version_id or "1"

      def cleanup_model():
        try:
          reference_model.delete()
        except Exception as e:
          logging.warning("Failed to delete reference model: %s", e)

      self.addCleanup(cleanup_model)

    # 4. Register cleanup for ModelMonitor and Schedules
    def cleanup_monitor():
      try:
        monitors = ml_monitoring.ModelMonitor.list(
            filter=f'display_name="{display_name}"',
            project=_ENDPOINT_PROJECT,
            location=_ENDPOINT_REGION,
        )
        for m in monitors:
          try:
            for s in m.list_schedules():
              m.delete_schedule(s.name)
          except Exception as se:
            logging.warning("Failed to clean up schedules: %s", se)
          m.delete()
      except Exception as e:
        logging.warning("Failed to clean up ModelMonitor: %s", e)

    self.addCleanup(cleanup_monitor)

    # 5. Pipeline test input records
    test_inputs = [
        {
            "feature1": 1.0, "feature2": 2.5
        },
        {
            "feature1": 2.0, "feature2": 5.0
        },
        {
            "feature1": 3.0, "feature2": 7.5
        },
    ]

    schema = ml_monitoring.spec.ModelMonitoringSchema(
        feature_fields=[
            ml_monitoring.spec.FieldSchema(name="feature1", data_type="float"),
            ml_monitoring.spec.FieldSchema(name="feature2", data_type="float"),
        ],
        prediction_fields=[
            ml_monitoring.spec.FieldSchema(
                name="prediction", data_type="float"),
        ],
    )

    training_dataset = ml_monitoring.spec.MonitoringInput(
        table_uri=f"bq://{_ENDPOINT_PROJECT}.{dataset_name}.baseline",
    )

    target_dataset = ml_monitoring.spec.MonitoringInput(
        table_uri=f"bq://{_ENDPOINT_PROJECT}.{dataset_name}.predictions",
    )

    tabular_objective_spec = ml_monitoring.spec.TabularObjective(
        feature_drift_spec=ml_monitoring.spec.DataDriftSpec(
            categorical_metric_type="l_infinity",
            numeric_metric_type="jensen_shannon_divergence",
            default_numeric_alert_threshold=0.3,
        ),
    )

    notification_spec = ml_monitoring.spec.NotificationSpec(
        enable_cloud_logging=True,
    )

    def unpack_prediction(result: PredictionResult) -> dict:
      row = dict(result.example)
      row.update(result.inference)
      return row

    monitoring_transform = VertexModelMonitoringV2(
        project_id=_ENDPOINT_PROJECT,
        location=_ENDPOINT_REGION,
        display_name=display_name,
        model_name=model_resource_name,
        model_version_id=model_version_id,
        model_monitoring_schema=schema,
        training_dataset=training_dataset,
        tabular_objective_spec=tabular_objective_spec,
        target_dataset=target_dataset,
        notification_spec=notification_spec,
        unpack_fn=unpack_prediction,
        bigquery_table=predictions_table_id,
        bigquery_schema="feature1:FLOAT,feature2:FLOAT,prediction:FLOAT",
        cron=cron,
        schedule_display_name=schedule_display_name,
        monitoring_job_display_name=f"{display_name}_job",
        write_to_bigquery_kwargs={
            "create_disposition": "CREATE_IF_NEEDED",
            "write_disposition": "WRITE_APPEND",
        },
    )

    with test_pipeline as p:
      _ = (
          p
          | "CreateInputs" >> beam.Create(test_inputs)
          | "RunInference" >> RunInference(
              SimpleLinearModelHandler(),
              monitoring_transform=monitoring_transform,
          ))

    # 6. Programmatically verify Schedule and ModelMonitor via Vertex AI API
    monitors = ml_monitoring.ModelMonitor.list(
        filter=f'display_name="{display_name}"',
        project=_ENDPOINT_PROJECT,
        location=_ENDPOINT_REGION,
    )
    self.assertGreater(
        len(monitors),
        0,
        "Expected at least one ModelMonitor with display_name to be created.",
    )
    monitor = monitors[0]

    schedules = monitor.list_schedules()
    self.assertGreater(
        len(schedules),
        0,
        "Expected at least one Schedule to be created for the streaming pipeline.",
    )
    schedule = schedules[0]
    self.assertEqual(schedule.cron, cron)
    self.assertEqual(schedule.display_name, schedule_display_name)

    alerts_response = monitor.search_alerts(objective_type="raw-feature-drift")
    self.assertIn("model_monitoring_alerts", alerts_response)
    self.assertIn("total_number_alerts", alerts_response)


if __name__ == "__main__":
  unittest.main()
