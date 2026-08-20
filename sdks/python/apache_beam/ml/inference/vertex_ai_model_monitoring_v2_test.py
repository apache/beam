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

import dataclasses
import unittest
from unittest import mock

import pytest

import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteResult
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

# Test target imports
try:
  from google.api_core import exceptions
  from vertexai.resources.preview import ml_monitoring

  from apache_beam.ml.inference.vertex_ai_model_monitoring_v2 import VertexModelMonitoringV2
  from apache_beam.ml.inference.vertex_ai_model_monitoring_v2 import _V2JobManager
  from apache_beam.ml.inference.vertex_ai_model_monitoring_v2 import _V2JobManagerBatch
  from apache_beam.ml.inference.vertex_ai_model_monitoring_v2 import _V2JobManagerStreaming
except ImportError:
  VertexModelMonitoringV2 = None
  _V2JobManager = None
  _V2JobManagerBatch = None
  _V2JobManagerStreaming = None


@dataclasses.dataclass
class DummySpec:
  name: str = "dummy"

  def _as_proto(self):
    return mock.MagicMock()


class FakeModelHandler(ModelHandler[int, PredictionResult, None]):
  def run_inference(self, batch, model, inference_args=None):
    return [PredictionResult(x, x * 2) for x in batch]

  def load_model(self):
    return None

  def get_postprocess_fns(self):
    return [
        lambda result: PredictionResult(result.example, result.inference + 1)
    ]


class RunInferenceMonitoringOutletTest(unittest.TestCase):
  def test_monitoring_transform_receives_raw_prediction_results(self):
    """Verifies that the monitoring transform receives raw PredictionResults before post-processing."""
    expected_raw = [
        PredictionResult(1, 2),
        PredictionResult(2, 4),
        PredictionResult(3, 6),
    ]

    class VerifyMonitoringTransform(beam.PTransform):
      def expand(self, pcoll):
        assert_that(pcoll, equal_to(expected_raw), label="VerifyRawMonitoring")
        return pcoll

    model_handler = FakeModelHandler()
    with TestPipeline() as p:
      elements = [1, 2, 3]
      main_output = (
          p
          | beam.Create(elements)
          | RunInference(
              model_handler,
              monitoring_transform=VerifyMonitoringTransform(),
          ))

      # Postprocessing adds 1 to inference result (e.g. 1*2 + 1 = 3)
      expected_postprocessed = [
          PredictionResult(1, 3),
          PredictionResult(2, 5),
          PredictionResult(3, 7),
      ]
      assert_that(
          main_output,
          equal_to(expected_postprocessed),
          label="VerifyPostprocessed")

  def test_with_monitoring_transform_chaining(self):
    """Verifies with_monitoring_transform method chaining syntax."""
    class VerifyChainedMonitoringTransform(beam.PTransform):
      def expand(self, pcoll):
        assert_that(
            pcoll,
            equal_to([PredictionResult(10, 20)]),
            label="VerifyChainedMonitoring",
        )
        return pcoll

    model_handler = FakeModelHandler()
    with TestPipeline() as p:
      _ = (
          p
          | beam.Create([10])
          | RunInference(model_handler).with_monitoring_transform(
              VerifyChainedMonitoringTransform()))


@pytest.mark.skipif(
    VertexModelMonitoringV2 is None,
    reason="VertexModelMonitoringV2 not yet implemented or dependencies missing"
)
class VertexAIModelMonitoringV2JobManagerTest(unittest.TestCase):
  def setUp(self):
    self.project_id = "test-project"
    self.location = "us-central1"
    self.display_name = "test-monitor"
    self.model_name = "projects/123/locations/us-central1/models/test-model"
    self.model_version_id = "1"
    self.schema = DummySpec("schema")
    self.training_dataset = DummySpec("training_dataset")
    self.tabular_objective = DummySpec("tabular_objective")
    self.target_dataset = DummySpec("target_dataset")

  @mock.patch(
      "vertexai.resources.preview.ml_monitoring.model_monitors.ModelMonitor.create"
  )
  def test_create_model_monitor_success(self, mock_create):
    mock_monitor = mock.MagicMock()
    mock_create.return_value = mock_monitor

    manager = _V2JobManager(
        project_id=self.project_id,
        location=self.location,
        display_name=self.display_name,
        model_name=self.model_name,
        model_version_id=self.model_version_id,
        model_monitoring_schema=self.schema,
        training_dataset=self.training_dataset,
        tabular_objective_spec=self.tabular_objective,
    )

    monitor = manager.create_model_monitor()
    self.assertEqual(monitor, mock_monitor)
    mock_create.assert_called_once_with(
        model_name=self.model_name,
        model_version_id=self.model_version_id,
        training_dataset=self.training_dataset,
        display_name=self.display_name,
        model_monitoring_schema=self.schema,
        tabular_objective_spec=self.tabular_objective,
        output_spec=None,
        notification_spec=None,
        explanation_spec=None,
        project=self.project_id,
        location=self.location,
        credentials=None,
        model_monitor_id=None,
    )

  @mock.patch(
      "vertexai.resources.preview.ml_monitoring.model_monitors.ModelMonitor.__init__",
      return_value=None)
  @mock.patch(
      "vertexai.resources.preview.ml_monitoring.model_monitors.ModelMonitor.create"
  )
  def test_create_model_monitor_already_exists_fallback(
      self, mock_create, mock_init):
    mock_create.side_effect = exceptions.AlreadyExists("Monitor already exists")

    manager = _V2JobManager(
        project_id=self.project_id,
        location=self.location,
        display_name=self.display_name,
        model_name=self.model_name,
        model_version_id=self.model_version_id,
        model_monitoring_schema=self.schema,
        training_dataset=self.training_dataset,
        tabular_objective_spec=self.tabular_objective,
        model_monitor_id="custom-monitor-id",
    )

    monitor = manager.create_model_monitor()
    self.assertIsInstance(monitor, ml_monitoring.ModelMonitor)
    mock_init.assert_called_once_with(
        model_monitor_name="custom-monitor-id",
        project=self.project_id,
        location=self.location,
        credentials=None,
    )

  @mock.patch("time.sleep")
  @mock.patch(
      "vertexai.resources.preview.ml_monitoring.model_monitors.ModelMonitor.__init__",
      return_value=None)
  @mock.patch(
      "vertexai.resources.preview.ml_monitoring.model_monitors.ModelMonitor.create"
  )
  def test_create_model_monitor_conflict_fallback(
      self, mock_create, mock_init, mock_sleep):
    mock_create.side_effect = exceptions.Conflict("Monitor conflict")

    manager = _V2JobManager(
        project_id=self.project_id,
        location=self.location,
        display_name=self.display_name,
        model_name=self.model_name,
        model_version_id=self.model_version_id,
        model_monitoring_schema=self.schema,
        training_dataset=self.training_dataset,
        tabular_objective_spec=self.tabular_objective,
        model_monitor_id="custom-monitor-id",
    )

    monitor = manager.create_model_monitor()
    self.assertIsInstance(monitor, ml_monitoring.ModelMonitor)
    mock_sleep.assert_called_once_with(15)
    mock_init.assert_called_once_with(
        model_monitor_name="custom-monitor-id",
        project=self.project_id,
        location=self.location,
        credentials=None,
    )

  @mock.patch(
      "vertexai.resources.preview.ml_monitoring.model_monitors.ModelMonitor.list"
  )
  @mock.patch(
      "vertexai.resources.preview.ml_monitoring.model_monitors.ModelMonitor.create"
  )
  def test_create_model_monitor_already_exists_fallback_without_id(
      self, mock_create, mock_list):
    mock_create.side_effect = exceptions.AlreadyExists("Monitor already exists")
    mock_existing_monitor = mock.MagicMock()
    mock_list.return_value = [mock_existing_monitor]

    manager = _V2JobManager(
        project_id=self.project_id,
        location=self.location,
        display_name=self.display_name,
        model_name=self.model_name,
        model_version_id=self.model_version_id,
        model_monitoring_schema=self.schema,
        training_dataset=self.training_dataset,
        tabular_objective_spec=self.tabular_objective,
        model_monitor_id=None,
    )

    monitor = manager.create_model_monitor()
    self.assertEqual(monitor, mock_existing_monitor)
    mock_list.assert_called_once_with(
        filter=f'display_name="{self.display_name}"',
        project=self.project_id,
        location=self.location,
        credentials=None,
    )

  @mock.patch(
      "vertexai.resources.preview.ml_monitoring.model_monitors.ModelMonitor.create"
  )
  def test_batch_job_manager_process_runs_job(self, mock_create):
    mock_monitor = mock.MagicMock()
    mock_create.return_value = mock_monitor

    batch_manager = _V2JobManagerBatch(
        project_id=self.project_id,
        location=self.location,
        display_name=self.display_name,
        model_name=self.model_name,
        model_version_id=self.model_version_id,
        model_monitoring_schema=self.schema,
        training_dataset=self.training_dataset,
        tabular_objective_spec=self.tabular_objective,
        target_dataset=self.target_dataset,
        monitoring_job_display_name="test-batch-job",
    )

    batch_manager.setup()
    batch_manager.process(None)

    mock_monitor.run.assert_called_once_with(
        target_dataset=self.target_dataset,
        display_name="test-batch-job",
    )

  @mock.patch(
      "vertexai.resources.preview.ml_monitoring.model_monitors.ModelMonitor.create"
  )
  def test_batch_job_manager_process_handles_already_exists(self, mock_create):
    mock_monitor = mock.MagicMock()
    mock_monitor.run.side_effect = exceptions.AlreadyExists(
        "Job already exists")
    mock_create.return_value = mock_monitor

    batch_manager = _V2JobManagerBatch(
        project_id=self.project_id,
        location=self.location,
        display_name=self.display_name,
        model_name=self.model_name,
        model_version_id=self.model_version_id,
        model_monitoring_schema=self.schema,
        training_dataset=self.training_dataset,
        tabular_objective_spec=self.tabular_objective,
        target_dataset=self.target_dataset,
        monitoring_job_display_name="test-batch-job",
    )

    batch_manager.setup()
    # Should not raise exception
    batch_manager.process(None)
    mock_monitor.run.assert_called_once()

  @mock.patch(
      "vertexai.resources.preview.ml_monitoring.model_monitors.ModelMonitor.create"
  )
  def test_streaming_job_manager_setup_and_process(self, mock_create):
    mock_monitor = mock.MagicMock()
    mock_monitor.list_schedules.return_value = []
    mock_create.return_value = mock_monitor

    streaming_manager = _V2JobManagerStreaming(
        project_id=self.project_id,
        location=self.location,
        display_name=self.display_name,
        model_name=self.model_name,
        model_version_id=self.model_version_id,
        model_monitoring_schema=self.schema,
        training_dataset=self.training_dataset,
        tabular_objective_spec=self.tabular_objective,
        target_dataset=self.target_dataset,
        cron="@hourly",
        schedule_display_name="test-schedule",
        monitoring_job_display_name="test-sched-job",
    )

    streaming_manager.setup()
    streaming_manager.process(None)
    mock_monitor.list_schedules.assert_called_once()
    mock_monitor.create_schedule.assert_called_once_with(
        cron="@hourly",
        target_dataset=self.target_dataset,
        display_name="test-schedule",
        model_monitoring_job_display_name="test-sched-job",
        start_time=None,
        end_time=None,
        tabular_objective_spec=self.tabular_objective,
        baseline_dataset=self.training_dataset,
        output_spec=None,
        notification_spec=None,
        explanation_spec=None,
    )

  @mock.patch(
      "vertexai.resources.preview.ml_monitoring.model_monitors.ModelMonitor.create"
  )
  def test_streaming_job_manager_skips_creation_when_identical_schedule_exists(
      self, mock_create):
    mock_monitor = mock.MagicMock()
    mock_existing_schedule = mock.MagicMock(
        display_name="test-schedule", cron="@hourly")
    mock_monitor.list_schedules.return_value = [mock_existing_schedule]
    mock_create.return_value = mock_monitor

    streaming_manager = _V2JobManagerStreaming(
        project_id=self.project_id,
        location=self.location,
        display_name=self.display_name,
        model_name=self.model_name,
        model_version_id=self.model_version_id,
        model_monitoring_schema=self.schema,
        training_dataset=self.training_dataset,
        tabular_objective_spec=self.tabular_objective,
        target_dataset=self.target_dataset,
        cron="@hourly",
        schedule_display_name="test-schedule",
        monitoring_job_display_name="test-sched-job",
    )

    streaming_manager.setup()
    streaming_manager.process(None)
    mock_monitor.list_schedules.assert_called_once()
    mock_monitor.create_schedule.assert_not_called()

  @mock.patch(
      "vertexai.resources.preview.ml_monitoring.model_monitors.ModelMonitor.create"
  )
  def test_streaming_job_manager_raises_value_error_when_no_cron_and_no_schedule(
      self, mock_create):
    mock_monitor = mock.MagicMock()
    mock_monitor.list_schedules.return_value = []
    mock_create.return_value = mock_monitor

    streaming_manager = _V2JobManagerStreaming(
        project_id=self.project_id,
        location=self.location,
        display_name=self.display_name,
        model_name=self.model_name,
        model_version_id=self.model_version_id,
        model_monitoring_schema=self.schema,
        training_dataset=self.training_dataset,
        tabular_objective_spec=self.tabular_objective,
        target_dataset=self.target_dataset,
        cron=None,
        schedule_display_name="test-schedule",
        monitoring_job_display_name="test-sched-job",
    )

    streaming_manager.setup()
    with self.assertRaises(ValueError):
      streaming_manager.process(None)

  @mock.patch(
      "vertexai.resources.preview.ml_monitoring.model_monitors.ModelMonitor.create"
  )
  def test_streaming_job_manager_allows_no_cron_when_schedule_exists(
      self, mock_create):
    mock_monitor = mock.MagicMock()
    mock_existing_schedule = mock.MagicMock(
        display_name="test-schedule", cron="@daily")
    mock_monitor.list_schedules.return_value = [mock_existing_schedule]
    mock_create.return_value = mock_monitor

    streaming_manager = _V2JobManagerStreaming(
        project_id=self.project_id,
        location=self.location,
        display_name=self.display_name,
        model_name=self.model_name,
        model_version_id=self.model_version_id,
        model_monitoring_schema=self.schema,
        training_dataset=self.training_dataset,
        tabular_objective_spec=self.tabular_objective,
        target_dataset=self.target_dataset,
        cron=None,
        schedule_display_name="test-schedule",
        monitoring_job_display_name="test-sched-job",
    )

    streaming_manager.setup()
    streaming_manager.process(None)
    mock_monitor.list_schedules.assert_called_once()
    mock_monitor.create_schedule.assert_not_called()


@pytest.mark.skipif(
    VertexModelMonitoringV2 is None,
    reason="VertexModelMonitoringV2 not yet implemented or dependencies missing"
)
class VertexModelMonitoringV2TransformTest(unittest.TestCase):
  def setUp(self):
    self.project_id = "test-project"
    self.location = "us-central1"
    self.display_name = "test-monitor"
    self.model_name = "projects/123/locations/us-central1/models/test-model"
    self.model_version_id = "1"
    self.schema = DummySpec("schema")
    self.training_dataset = DummySpec("training_dataset")
    self.tabular_objective = DummySpec("tabular_objective")
    self.target_dataset = DummySpec("target_dataset")
    self.unpack_fn = lambda pr: {"feat": pr.example, "pred": pr.inference}
    self.bq_table = "test-project:dataset.table"

  @mock.patch(
      "apache_beam.ml.inference.vertex_ai_model_monitoring_v2.WriteToBigQuery")
  @mock.patch(
      "vertexai.resources.preview.ml_monitoring.model_monitors.ModelMonitor.create"
  )
  def test_batch_pipeline_expansion_with_write_result(
      self, mock_create, mock_write_to_bq):
    class FakeWriteTransform(beam.PTransform):
      def expand(self, pcoll):
        load_pcoll = pcoll | "FakeLoads" >> beam.Map(
            lambda x: ("dest", "job_1"))
        return WriteResult(
            method=WriteToBigQuery.Method.FILE_LOADS,
            destination_load_jobid_pairs=load_pcoll,
        )

    mock_write_to_bq.return_value = FakeWriteTransform()
    mock_monitor = mock.MagicMock()
    mock_create.return_value = mock_monitor

    transform = VertexModelMonitoringV2(
        project_id=self.project_id,
        location=self.location,
        display_name=self.display_name,
        model_name=self.model_name,
        model_version_id=self.model_version_id,
        model_monitoring_schema=self.schema,
        training_dataset=self.training_dataset,
        tabular_objective_spec=self.tabular_objective,
        target_dataset=self.target_dataset,
        unpack_fn=self.unpack_fn,
        bigquery_table=self.bq_table,
        bigquery_schema="feat:INTEGER,pred:INTEGER",
        write_to_bigquery_kwargs={"create_disposition": "CREATE_IF_NEEDED"},
    )

    with TestPipeline() as p:
      pcoll = p | beam.Create([PredictionResult(1, 2), PredictionResult(2, 4)])
      output = pcoll | transform
      assert_that(
          output, equal_to([PredictionResult(1, 2), PredictionResult(2, 4)]))

    mock_write_to_bq.assert_called_once_with(
        table=self.bq_table,
        schema="feat:INTEGER,pred:INTEGER",
        create_disposition="CREATE_IF_NEEDED",
    )
    mock_monitor.run.assert_called_once()

  def test_annotations_shadow_model_identifier(self):
    transform = VertexModelMonitoringV2(
        project_id=self.project_id,
        location=self.location,
        display_name=self.display_name,
        model_name=self.model_name,
        model_version_id=self.model_version_id,
        model_monitoring_schema=self.schema,
        training_dataset=self.training_dataset,
        tabular_objective_spec=self.tabular_objective,
        target_dataset=self.target_dataset,
        unpack_fn=self.unpack_fn,
        bigquery_table=self.bq_table,
    )
    annotations = transform.annotations()
    self.assertIn("model_identifier", annotations)
    self.assertEqual(annotations["model_identifier"], "")

  def test_run_inference_monitoring_outlet_shadows_model_identifier(self):
    class DummyMonitoring(beam.PTransform):
      def expand(self, pcoll):
        return pcoll | "Map" >> beam.Map(lambda x: x)

    class DummyModelHandler(ModelHandler[int, PredictionResult, None]):
      def run_inference(self, batch, model=None, inference_args=None):
        return [PredictionResult(example=x, inference=x * 2) for x in batch]

      def load_model(self):
        return None

    p = beam.Pipeline()
    ri = RunInference(
        DummyModelHandler(),
        monitoring_transform=DummyMonitoring(),
        model_identifier="test-model-identifier",
    )
    _ = p | beam.Create([1, 2, 3]) | ri
    proto = p.to_runner_api()

    outlet_transforms = [
        t for t in proto.components.transforms.values()
        if "BeamML_RunInference_MonitoringOutlet" in t.unique_name
    ]
    self.assertTrue(len(outlet_transforms) > 0)
    for t in outlet_transforms:
      self.assertEqual(t.annotations.get("model_identifier"), b"")

  @mock.patch(
      "apache_beam.ml.inference.vertex_ai_model_monitoring_v2.WriteToBigQuery")
  @mock.patch(
      "vertexai.resources.preview.ml_monitoring.model_monitors.ModelMonitor.create"
  )
  def test_streaming_pipeline_expansion_with_cron(
      self, mock_create, mock_write_to_bq):
    class FakeWriteTransform(beam.PTransform):
      def expand(self, pcoll):
        return pcoll

    mock_write_to_bq.return_value = FakeWriteTransform()
    mock_monitor = mock.MagicMock()
    mock_monitor.list_schedules.return_value = []
    mock_create.return_value = mock_monitor

    transform = VertexModelMonitoringV2(
        project_id=self.project_id,
        location=self.location,
        display_name=self.display_name,
        model_name=self.model_name,
        model_version_id=self.model_version_id,
        model_monitoring_schema=self.schema,
        training_dataset=self.training_dataset,
        tabular_objective_spec=self.tabular_objective,
        target_dataset=self.target_dataset,
        unpack_fn=self.unpack_fn,
        bigquery_table=self.bq_table,
        cron="0 0 * * *",
    )

    with TestPipeline(additional_pipeline_args=["--streaming"]) as p:
      pcoll = p | beam.Create([PredictionResult(1, 2), PredictionResult(2, 4)])
      output = pcoll | transform
      assert_that(
          output, equal_to([PredictionResult(1, 2), PredictionResult(2, 4)]))

  @mock.patch(
      "apache_beam.ml.inference.vertex_ai_model_monitoring_v2.WriteToBigQuery")
  @mock.patch(
      "vertexai.resources.preview.ml_monitoring.model_monitors.ModelMonitor.create"
  )
  def test_streaming_pipeline_expansion_without_cron(
      self, mock_create, mock_write_to_bq):
    class FakeWriteTransform(beam.PTransform):
      def expand(self, pcoll):
        return pcoll

    mock_write_to_bq.return_value = FakeWriteTransform()
    mock_monitor = mock.MagicMock()
    mock_monitor.list_schedules.return_value = [
        mock.MagicMock(display_name="test-monitor_schedule", cron="0 0 * * *")
    ]
    mock_create.return_value = mock_monitor

    transform = VertexModelMonitoringV2(
        project_id=self.project_id,
        location=self.location,
        display_name=self.display_name,
        model_name=self.model_name,
        model_version_id=self.model_version_id,
        model_monitoring_schema=self.schema,
        training_dataset=self.training_dataset,
        tabular_objective_spec=self.tabular_objective,
        target_dataset=self.target_dataset,
        unpack_fn=self.unpack_fn,
        bigquery_table=self.bq_table,
        cron=None,
    )

    with TestPipeline(additional_pipeline_args=["--streaming"]) as p:
      pcoll = p | beam.Create([PredictionResult(1, 2), PredictionResult(2, 4)])
      output = pcoll | transform
      assert_that(
          output, equal_to([PredictionResult(1, 2), PredictionResult(2, 4)]))


if __name__ == "__main__":
  unittest.main()
