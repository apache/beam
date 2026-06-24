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

"""Tests for apache_beam.runners.interactive.interactive_beam."""
# pytype: skip-file

import dataclasses
import importlib
import sys
import time
import unittest
from concurrent.futures import TimeoutError
from typing import NamedTuple
from unittest.mock import ANY
from unittest.mock import MagicMock
from unittest.mock import call
from unittest.mock import patch

import apache_beam as beam
from apache_beam import dataframe as frames
from apache_beam.dataframe.frame_base import DeferredBase
from apache_beam.options.pipeline_options import FlinkRunnerOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import interactive_runner as ir
from apache_beam.runners.interactive.dataproc.dataproc_cluster_manager import DataprocClusterManager
from apache_beam.runners.interactive.dataproc.types import ClusterMetadata
from apache_beam.runners.interactive.options.capture_limiters import Limiter
from apache_beam.runners.interactive.recording_manager import AsyncComputationResult
from apache_beam.runners.interactive.testing.mock_env import isolated_env
from apache_beam.runners.runner import PipelineState
from apache_beam.testing.test_stream import TestStream


@dataclasses.dataclass
class MockClusterMetadata:
  master_url = 'mock_url'


class Record(NamedTuple):
  order_id: int
  product_id: int
  quantity: int


# The module name is also a variable in module.
_module_name = 'apache_beam.runners.interactive.interactive_beam_test'


def _get_watched_pcollections_with_variable_names():
  watched_pcollections = {}
  for watching in ie.current_env().watching():
    for key, val in watching:
      if hasattr(val, '__class__') and isinstance(val, beam.pvalue.PCollection):
        watched_pcollections[val] = key
  return watched_pcollections


@unittest.skipIf(
    not ie.current_env().is_interactive_ready,
    '[interactive] dependency is not installed.')
@isolated_env
class InteractiveBeamTest(unittest.TestCase):
  def setUp(self):
    self._var_in_class_instance = 'a var in class instance, not directly used'

  def tearDown(self):
    ib.options.capture_control.set_limiters_for_test([])

  def test_watch_main_by_default(self):
    test_env = ie.InteractiveEnvironment()
    # Current Interactive Beam env fetched and the test env are 2 instances.
    self.assertNotEqual(id(ie.current_env()), id(test_env))
    self.assertEqual(ie.current_env().watching(), test_env.watching())

  def test_watch_a_module_by_name(self):
    test_env = ie.InteractiveEnvironment()
    ib.watch(_module_name)
    test_env.watch(_module_name)
    self.assertEqual(ie.current_env().watching(), test_env.watching())

  def test_watch_a_module_by_module_object(self):
    test_env = ie.InteractiveEnvironment()
    module = importlib.import_module(_module_name)
    ib.watch(module)
    test_env.watch(module)
    self.assertEqual(ie.current_env().watching(), test_env.watching())

  def test_watch_locals(self):
    # test_env serves as local var too.
    test_env = ie.InteractiveEnvironment()
    ib.watch(locals())
    test_env.watch(locals())
    self.assertEqual(ie.current_env().watching(), test_env.watching())

  def test_watch_class_instance(self):
    test_env = ie.InteractiveEnvironment()
    ib.watch(self)
    test_env.watch(self)
    self.assertEqual(ie.current_env().watching(), test_env.watching())

  @unittest.skipIf(sys.platform == "win32", "[BEAM-10627]")
  def test_show_always_watch_given_pcolls(self):
    p = beam.Pipeline(ir.InteractiveRunner())
    # pylint: disable=bad-option-value
    pcoll = p | 'Create' >> beam.Create(range(10))
    # The pcoll is not watched since watch(locals()) is not explicitly called.
    self.assertFalse(pcoll in _get_watched_pcollections_with_variable_names())
    # The call of show watches pcoll.
    ib.watch({'p': p})
    ie.current_env().track_user_pipelines()
    ib.show(pcoll)
    self.assertTrue(pcoll in _get_watched_pcollections_with_variable_names())

  @unittest.skipIf(sys.platform == "win32", "[BEAM-10627]")
  def test_show_mark_pcolls_computed_when_done(self):
    p = beam.Pipeline(ir.InteractiveRunner())
    # pylint: disable=bad-option-value
    pcoll = p | 'Create' >> beam.Create(range(10))
    self.assertFalse(pcoll in ie.current_env().computed_pcollections)
    # The call of show marks pcoll computed.
    ib.watch(locals())
    ie.current_env().track_user_pipelines()
    ib.show(pcoll)
    self.assertTrue(pcoll in ie.current_env().computed_pcollections)

  @patch((
      'apache_beam.runners.interactive.interactive_beam.'
      'visualize_computed_pcoll'))
  def test_show_handles_dict_of_pcolls(self, mocked_visualize):
    p = beam.Pipeline(ir.InteractiveRunner())
    # pylint: disable=bad-option-value
    pcoll = p | 'Create' >> beam.Create(range(10))
    ib.watch(locals())
    ie.current_env().track_user_pipelines()
    ie.current_env().mark_pcollection_computed([pcoll])
    ie.current_env()._is_in_ipython = True
    ie.current_env()._is_in_notebook = True
    ib.show({'pcoll': pcoll})
    mocked_visualize.assert_called_once()

  @patch((
      'apache_beam.runners.interactive.interactive_beam.'
      'visualize_computed_pcoll'))
  def test_show_handles_iterable_of_pcolls(self, mocked_visualize):
    p = beam.Pipeline(ir.InteractiveRunner())
    # pylint: disable=bad-option-value
    pcoll = p | 'Create' >> beam.Create(range(10))
    ib.watch(locals())
    ie.current_env().track_user_pipelines()
    ie.current_env().mark_pcollection_computed([pcoll])
    ie.current_env()._is_in_ipython = True
    ie.current_env()._is_in_notebook = True
    ib.show([pcoll])
    mocked_visualize.assert_called_once()

  @patch('apache_beam.runners.interactive.interactive_beam.visualize')
  def test_show_handles_deferred_dataframes(self, mocked_visualize):
    p = beam.Pipeline(ir.InteractiveRunner())

    deferred = frames.convert.to_dataframe(p | beam.Create([Record(0, 0, 0)]))

    ib.watch(locals())
    ie.current_env().track_user_pipelines()
    ie.current_env()._is_in_ipython = True
    ie.current_env()._is_in_notebook = True
    ib.show(deferred)
    mocked_visualize.assert_called_once()

  @patch((
      'apache_beam.runners.interactive.interactive_beam.'
      'visualize_computed_pcoll'))
  def test_show_noop_when_pcoll_container_is_invalid(self, mocked_visualize):
    class SomeRandomClass:
      def __init__(self, pcoll):
        self._pcoll = pcoll

    p = beam.Pipeline(ir.InteractiveRunner())
    # pylint: disable=bad-option-value
    pcoll = p | 'Create' >> beam.Create(range(10))
    ie.current_env().mark_pcollection_computed([pcoll])
    ie.current_env()._is_in_ipython = True
    ie.current_env()._is_in_notebook = True
    self.assertRaises(ValueError, ib.show, SomeRandomClass(pcoll))
    mocked_visualize.assert_not_called()

  def test_recordings_describe(self):
    """Tests that getting the description works."""

    # Create the pipelines to test.
    p1 = beam.Pipeline(ir.InteractiveRunner())
    p2 = beam.Pipeline(ir.InteractiveRunner())

    ib.watch(locals())

    # Get the descriptions. This test is simple as there isn't much logic in the
    # method.
    self.assertEqual(ib.recordings.describe(p1)['size'], 0)
    self.assertEqual(ib.recordings.describe(p2)['size'], 0)

    all_descriptions = ib.recordings.describe()
    self.assertEqual(all_descriptions[p1]['size'], 0)
    self.assertEqual(all_descriptions[p2]['size'], 0)

    # Ensure that the variable name for the pipeline is set correctly.
    self.assertEqual(all_descriptions[p1]['pipeline_var'], 'p1')
    self.assertEqual(all_descriptions[p2]['pipeline_var'], 'p2')

  def test_recordings_clear(self):
    """Tests that clearing the pipeline is correctly forwarded."""

    # Create a basic pipeline to store something in the cache.
    p = beam.Pipeline(ir.InteractiveRunner())
    elem = p | beam.Create([1])
    ib.watch(locals())
    ie.current_env().track_user_pipelines()

    # This records the pipeline so that the cache size is > 0.
    ib.collect(elem)
    self.assertGreater(ib.recordings.describe(p)['size'], 0)

    # After clearing, the cache should be empty.
    ib.recordings.clear(p)
    self.assertEqual(ib.recordings.describe(p)['size'], 0)

  def test_recordings_record(self):
    """Tests that recording pipeline succeeds."""

    # Add the TestStream so that it can be cached.
    ib.options.recordable_sources.add(TestStream)

    # Create a pipeline with an arbitrary amonunt of elements.
    p = beam.Pipeline(
        ir.InteractiveRunner(), options=PipelineOptions(streaming=True))
    # pylint: disable=unused-variable
    _ = (p
         | TestStream()
             .advance_watermark_to(0)
             .advance_processing_time(1)
             .add_elements(list(range(10)))
             .advance_processing_time(1))  # yapf: disable
    ib.watch(locals())
    ie.current_env().track_user_pipelines()

    # Assert that the pipeline starts in a good state.
    self.assertEqual(ib.recordings.describe(p)['state'], PipelineState.STOPPED)
    self.assertEqual(ib.recordings.describe(p)['size'], 0)

    # Create a lmiter that stops the background caching job when something is
    # written to cache. This is used to make ensure that the pipeline is
    # functioning properly and that there are no data races with the test.
    class SizeLimiter(Limiter):
      def __init__(self, pipeline):
        self.pipeline = pipeline
        self.should_trigger = False

      def is_triggered(self):
        return (
            ib.recordings.describe(self.pipeline)['size'] > 0 and
            self.should_trigger)

    limiter = SizeLimiter(p)
    ib.options.capture_control.set_limiters_for_test([limiter])

    # Assert that a recording can be started only once.
    self.assertTrue(ib.recordings.record(p))
    self.assertFalse(ib.recordings.record(p))
    self.assertEqual(ib.recordings.describe(p)['state'], PipelineState.RUNNING)

    # Wait for the pipeline to start and write something to cache.
    limiter.should_trigger = True
    for _ in range(60):
      if limiter.is_triggered():
        break
      time.sleep(1)
    self.assertTrue(
        limiter.is_triggered(),
        'Test timed out waiting for limiter to be triggered. This indicates '
        'that the BackgroundCachingJob did not cache anything.')

    # Assert that a recording can be stopped and can't be started again until
    # after the cache is cleared.
    ib.recordings.stop(p)
    self.assertEqual(ib.recordings.describe(p)['state'], PipelineState.STOPPED)
    self.assertFalse(ib.recordings.record(p))
    ib.recordings.clear(p)
    self.assertTrue(ib.recordings.record(p))
    ib.recordings.stop(p)

  def test_collect_raw_records_true(self):
    p = beam.Pipeline(ir.InteractiveRunner())
    data = list(range(5))
    pcoll = p | 'Create' >> beam.Create(data)
    ib.watch(locals())
    ie.current_env().track_user_pipelines()

    result = ib.collect(pcoll, raw_records=True)
    self.assertIsInstance(result, list)
    self.assertEqual(result, data)

    result_n = ib.collect(pcoll, n=3, raw_records=True)
    self.assertIsInstance(result_n, list)
    self.assertEqual(result_n, data[:3])

  def test_collect_raw_records_false(self):
    p = beam.Pipeline(ir.InteractiveRunner())
    data = list(range(5))
    pcoll = p | 'Create' >> beam.Create(data)
    ib.watch(locals())
    ie.current_env().track_user_pipelines()

    result = ib.collect(pcoll)
    self.assertNotIsInstance(result, list)
    self.assertTrue(
        hasattr(result, 'columns'), "Result should have 'columns' attribute")
    self.assertTrue(
        hasattr(result, 'values'), "Result should have 'values' attribute")

    result_n = ib.collect(pcoll, n=3)
    self.assertNotIsInstance(result_n, list)
    self.assertTrue(
        hasattr(result_n, 'columns'),
        "Result (n=3) should have 'columns' attribute")
    self.assertTrue(
        hasattr(result_n, 'values'),
        "Result (n=3) should have 'values' attribute")

  def test_collect_raw_records_true_multiple_pcolls(self):
    p = beam.Pipeline(ir.InteractiveRunner())
    data1 = list(range(3))
    data2 = [x * x for x in range(3)]
    pcoll1 = p | 'Create1' >> beam.Create(data1)
    pcoll2 = p | 'Create2' >> beam.Create(data2)
    ib.watch(locals())
    ie.current_env().track_user_pipelines()

    result = ib.collect(pcoll1, pcoll2, raw_records=True)
    self.assertIsInstance(result, tuple)
    self.assertEqual(len(result), 2)
    self.assertIsInstance(result[0], list)
    self.assertEqual(result[0], data1)
    self.assertIsInstance(result[1], list)
    self.assertEqual(result[1], data2)

  def test_collect_raw_records_false_multiple_pcolls(self):
    p = beam.Pipeline(ir.InteractiveRunner())
    data1 = list(range(3))
    data2 = [x * x for x in range(3)]
    pcoll1 = p | 'Create1' >> beam.Create(data1)
    pcoll2 = p | 'Create2' >> beam.Create(data2)
    ib.watch(locals())
    ie.current_env().track_user_pipelines()

    result = ib.collect(pcoll1, pcoll2)
    self.assertIsInstance(result, tuple)
    self.assertEqual(len(result), 2)
    self.assertNotIsInstance(result[0], list)
    self.assertTrue(hasattr(result[0], 'columns'))
    self.assertNotIsInstance(result[1], list)
    self.assertTrue(hasattr(result[1], 'columns'))

  def test_collect_raw_records_true_force_tuple(self):
    p = beam.Pipeline(ir.InteractiveRunner())
    data = list(range(5))
    pcoll = p | 'Create' >> beam.Create(data)
    ib.watch(locals())
    ie.current_env().track_user_pipelines()

    result = ib.collect(pcoll, raw_records=True, force_tuple=True)
    self.assertIsInstance(result, tuple)
    self.assertEqual(len(result), 1)
    self.assertIsInstance(result[0], list)
    self.assertEqual(result[0], data)


@unittest.skipIf(
    not ie.current_env().is_interactive_ready,
    '[interactive] dependency is not installed.')
@isolated_env
class InteractiveBeamClustersTest(unittest.TestCase):
  def setUp(self):
    self.current_env.options.cache_root = 'gs://fake'
    self.clusters = self.current_env.clusters

  def tearDown(self):
    self.current_env.options.cache_root = None

  def test_cluster_metadata_pass_through_metadata(self):
    cid = ClusterMetadata(project_id='test-project')
    meta = self.clusters.cluster_metadata(cid)
    self.assertIs(meta, cid)

  def test_cluster_metadata_identifies_pipeline(self):
    cid = beam.Pipeline()
    known_meta = ClusterMetadata(project_id='test-project')
    dcm = DataprocClusterManager(known_meta)
    self.clusters.pipelines[cid] = dcm

    meta = self.clusters.cluster_metadata(cid)
    self.assertIs(meta, known_meta)

  def test_cluster_metadata_identifies_master_url(self):
    cid = 'test-url'
    known_meta = ClusterMetadata(project_id='test-project')
    _ = DataprocClusterManager(known_meta)
    self.clusters.master_urls[cid] = known_meta

    meta = self.clusters.cluster_metadata(cid)
    self.assertIs(meta, known_meta)

  def test_cluster_metadata_default_value(self):
    cid_none = None
    cid_unknown_p = beam.Pipeline()
    cid_unknown_master_url = 'test-url'
    default_meta = ClusterMetadata(project_id='test-project')
    self.clusters.set_default_cluster(default_meta)

    self.assertIs(default_meta, self.clusters.cluster_metadata(cid_none))
    self.assertIs(default_meta, self.clusters.cluster_metadata(cid_unknown_p))
    self.assertIs(
        default_meta, self.clusters.cluster_metadata(cid_unknown_master_url))

  def test_create_a_new_cluster(self):
    meta = ClusterMetadata(project_id='test-project')
    _ = self.clusters.create(meta)

    # Derived fields are populated.
    self.assertTrue(meta.master_url.startswith('test-url'))
    self.assertEqual(meta.dashboard, 'test-dashboard')
    # The cluster is known.
    self.assertIn(meta, self.clusters.dataproc_cluster_managers)
    self.assertIn(meta.master_url, self.clusters.master_urls)
    # The default cluster is updated to the created cluster.
    self.assertIs(meta, self.clusters.default_cluster_metadata)

  def test_create_but_reuse_a_known_cluster(self):
    known_meta = ClusterMetadata(
        project_id='test-project', region='test-region')
    known_dcm = DataprocClusterManager(known_meta)
    known_meta.master_url = 'test-url'
    self.clusters.set_default_cluster(known_meta)
    self.clusters.dataproc_cluster_managers[known_meta] = known_dcm
    self.clusters.master_urls[known_meta.master_url] = known_meta

    # Use an equivalent meta as the identifier to create a cluster.
    cid_meta = ClusterMetadata(
        project_id=known_meta.project_id,
        region=known_meta.region,
        cluster_name=known_meta.cluster_name)
    dcm = self.clusters.create(cid_meta)
    # The known cluster manager is returned.
    self.assertIs(dcm, known_dcm)

    # Then use an equivalent master_url as the identifier.
    cid_master_url = known_meta.master_url
    dcm = self.clusters.create(cid_master_url)
    self.assertIs(dcm, known_dcm)

  def test_cleanup_by_a_pipeline(self):
    meta = ClusterMetadata(project_id='test-project')
    dcm = self.clusters.create(meta)

    # Set up the association between a pipeline and a cluster.
    # In real code, it's set by the runner the 1st time a pipeline is executed.
    options = PipelineOptions()
    options.view_as(FlinkRunnerOptions).flink_master = meta.master_url
    p = beam.Pipeline(options=options)
    self.clusters.pipelines[p] = dcm
    dcm.pipelines.add(p)

    self.clusters.cleanup(p)
    # Delete the cluster.
    self.m_delete_cluster.assert_called_once()
    # Pipeline association is cleaned up.
    self.assertNotIn(p, self.clusters.pipelines)
    self.assertNotIn(p, dcm.pipelines)
    # The internal option in the pipeline is overwritten.
    self.assertEqual(
        p.options.view_as(FlinkRunnerOptions).flink_master, '[auto]')
    # The original option is unchanged.
    self.assertEqual(
        options.view_as(FlinkRunnerOptions).flink_master, meta.master_url)
    # The cluster is unknown now.
    self.assertNotIn(meta, self.clusters.dataproc_cluster_managers)
    self.assertNotIn(meta.master_url, self.clusters.master_urls)
    # The cleaned up cluster is also the default cluster. Clean the default.
    self.assertIsNone(self.clusters.default_cluster_metadata)

  def test_not_cleanup_if_multiple_pipelines_share_a_manager(self):
    meta = ClusterMetadata(project_id='test-project')
    dcm = self.clusters.create(meta)

    options = PipelineOptions()
    options.view_as(FlinkRunnerOptions).flink_master = meta.master_url
    options2 = PipelineOptions()
    options2.view_as(FlinkRunnerOptions).flink_master = meta.master_url
    p = beam.Pipeline(options=options)
    p2 = beam.Pipeline(options=options2)
    self.clusters.pipelines[p] = dcm
    self.clusters.pipelines[p2] = dcm
    dcm.pipelines.add(p)
    dcm.pipelines.add(p2)

    self.clusters.cleanup(p)
    # No cluster deleted.
    self.m_delete_cluster.assert_not_called()
    # Pipeline association of p is cleaned up.
    self.assertNotIn(p, self.clusters.pipelines)
    self.assertNotIn(p, dcm.pipelines)
    # The internal option in the pipeline is overwritten.
    self.assertEqual(
        p.options.view_as(FlinkRunnerOptions).flink_master, '[auto]')
    # The original option is unchanged.
    self.assertEqual(
        options.view_as(FlinkRunnerOptions).flink_master, meta.master_url)
    # Pipeline association of p2 still presents.
    self.assertIn(p2, self.clusters.pipelines)
    self.assertIn(p2, dcm.pipelines)
    self.assertEqual(
        p2.options.view_as(FlinkRunnerOptions).flink_master, meta.master_url)
    self.assertEqual(
        options2.view_as(FlinkRunnerOptions).flink_master, meta.master_url)
    # The cluster is still known.
    self.assertIn(meta, self.clusters.dataproc_cluster_managers)
    self.assertIn(meta.master_url, self.clusters.master_urls)
    # The default cluster still presents.
    self.assertIs(meta, self.clusters.default_cluster_metadata)

  def test_cleanup_by_a_master_url(self):
    meta = ClusterMetadata(project_id='test-project')
    _ = self.clusters.create(meta)

    self.clusters.cleanup(meta.master_url)
    self.m_delete_cluster.assert_called_once()
    self.assertNotIn(meta, self.clusters.dataproc_cluster_managers)
    self.assertNotIn(meta.master_url, self.clusters.master_urls)
    self.assertIsNone(self.clusters.default_cluster_metadata)

  def test_cleanup_by_meta(self):
    known_meta = ClusterMetadata(
        project_id='test-project', region='test-region')
    _ = self.clusters.create(known_meta)

    meta = ClusterMetadata(
        project_id=known_meta.project_id,
        region=known_meta.region,
        cluster_name=known_meta.cluster_name)
    self.clusters.cleanup(meta)
    self.m_delete_cluster.assert_called_once()
    self.assertNotIn(known_meta, self.clusters.dataproc_cluster_managers)
    self.assertNotIn(known_meta.master_url, self.clusters.master_urls)
    self.assertIsNone(self.clusters.default_cluster_metadata)

  def test_force_cleanup_everything(self):
    meta = ClusterMetadata(project_id='test-project')
    meta2 = ClusterMetadata(project_id='test-project-2')
    _ = self.clusters.create(meta)
    _ = self.clusters.create(meta2)

    self.clusters.cleanup(force=True)
    self.assertEqual(self.m_delete_cluster.call_count, 2)
    self.assertNotIn(meta, self.clusters.dataproc_cluster_managers)
    self.assertNotIn(meta2, self.clusters.dataproc_cluster_managers)
    self.assertIsNone(self.clusters.default_cluster_metadata)

  def test_cleanup_noop_for_no_cluster_identifier(self):
    meta = ClusterMetadata(project_id='test-project')
    _ = self.clusters.create(meta)

    self.clusters.cleanup()
    self.m_delete_cluster.assert_not_called()

  def test_cleanup_noop_unknown_cluster(self):
    meta = ClusterMetadata(project_id='test-project')
    dcm = self.clusters.create(meta)
    p = beam.Pipeline()
    self.clusters.pipelines[p] = dcm
    dcm.pipelines.add(p)

    cid_pipeline = beam.Pipeline()
    self.clusters.cleanup(cid_pipeline)
    self.m_delete_cluster.assert_not_called()

    cid_master_url = 'some-random-url'
    self.clusters.cleanup(cid_master_url)
    self.m_delete_cluster.assert_not_called()

    cid_meta = ClusterMetadata(project_id='random-project')
    self.clusters.cleanup(cid_meta)
    self.m_delete_cluster.assert_not_called()

    self.assertIn(meta, self.clusters.dataproc_cluster_managers)
    self.assertIn(meta.master_url, self.clusters.master_urls)
    self.assertIs(meta, self.clusters.default_cluster_metadata)
    self.assertIn(p, self.clusters.pipelines)
    self.assertIn(p, dcm.pipelines)

  def test_describe_everything(self):
    meta = ClusterMetadata(project_id='test-project')
    meta2 = ClusterMetadata(
        project_id='test-project', region='some-other-region')
    _ = self.clusters.create(meta)
    _ = self.clusters.create(meta2)

    meta_list = self.clusters.describe()
    self.assertEqual([meta, meta2], meta_list)

  def test_describe_by_cluster_identifier(self):
    known_meta = ClusterMetadata(project_id='test-project')
    known_meta2 = ClusterMetadata(
        project_id='test-project', region='some-other-region')
    dcm = self.clusters.create(known_meta)
    dcm2 = self.clusters.create(known_meta2)
    p = beam.Pipeline()
    p2 = beam.Pipeline()
    self.clusters.pipelines[p] = dcm
    dcm.pipelines.add(p)
    self.clusters.pipelines[p2] = dcm2
    dcm.pipelines.add(p2)

    cid_pipeline = p
    meta = self.clusters.describe(cid_pipeline)
    self.assertIs(meta, known_meta)

    cid_master_url = known_meta.master_url
    meta = self.clusters.describe(cid_master_url)
    self.assertIs(meta, known_meta)

    cid_meta = ClusterMetadata(
        project_id=known_meta.project_id,
        region=known_meta.region,
        cluster_name=known_meta.cluster_name)
    meta = self.clusters.describe(cid_meta)
    self.assertIs(meta, known_meta)

  def test_describe_everything_when_cluster_identifer_unknown(self):
    known_meta = ClusterMetadata(project_id='test-project')
    known_meta2 = ClusterMetadata(
        project_id='test-project', region='some-other-region')
    dcm = self.clusters.create(known_meta)
    dcm2 = self.clusters.create(known_meta2)
    p = beam.Pipeline()
    p2 = beam.Pipeline()
    self.clusters.pipelines[p] = dcm
    dcm.pipelines.add(p)
    self.clusters.pipelines[p2] = dcm2
    dcm.pipelines.add(p2)

    cid_pipeline = beam.Pipeline()
    meta_list = self.clusters.describe(cid_pipeline)
    self.assertEqual([known_meta, known_meta2], meta_list)

    cid_master_url = 'some-random-url'
    meta_list = self.clusters.describe(cid_master_url)
    self.assertEqual([known_meta, known_meta2], meta_list)

    cid_meta = ClusterMetadata(project_id='some-random-project')
    meta_list = self.clusters.describe(cid_meta)
    self.assertEqual([known_meta, known_meta2], meta_list)

  def test_default_value_for_invalid_worker_number(self):
    meta = ClusterMetadata(project_id='test-project', num_workers=1)
    self.clusters.create(meta)

    self.assertEqual(meta.num_workers, 2)


@unittest.skipIf(
    not ie.current_env().is_interactive_ready,
    '[interactive] dependency is not installed.')
@isolated_env
class InteractiveBeamComputeTest(unittest.TestCase):
  def setUp(self):
    self.env = ie.current_env()
    self.env._is_in_ipython = False  # Default to non-IPython

  def test_compute_blocking(self):
    p = beam.Pipeline(ir.InteractiveRunner())
    data = list(range(10))
    pcoll = p | 'Create' >> beam.Create(data)
    ib.watch(locals())
    self.env.track_user_pipelines()

    result = ib.compute(pcoll, blocking=True)
    self.assertIsNone(result)  # Blocking returns None
    self.assertTrue(pcoll in self.env.computed_pcollections)
    collected = ib.collect(pcoll, raw_records=True)
    self.assertEqual(collected, data)

  def test_compute_non_blocking(self):
    p = beam.Pipeline(ir.InteractiveRunner())
    data = list(range(5))
    pcoll = p | 'Create' >> beam.Create(data)
    ib.watch(locals())
    self.env.track_user_pipelines()

    async_result = ib.compute(pcoll, blocking=False)
    self.assertIsInstance(async_result, AsyncComputationResult)

    pipeline_result = async_result.result(timeout=60)
    self.assertTrue(async_result.done())
    self.assertIsNone(async_result.exception())
    self.assertEqual(pipeline_result.state, PipelineState.DONE)
    self.assertTrue(pcoll in self.env.computed_pcollections)
    collected = ib.collect(pcoll, raw_records=True)
    self.assertEqual(collected, data)

  def test_compute_with_list_input(self):
    p = beam.Pipeline(ir.InteractiveRunner())
    pcoll1 = p | 'Create1' >> beam.Create([1, 2, 3])
    pcoll2 = p | 'Create2' >> beam.Create([4, 5, 6])
    ib.watch(locals())
    self.env.track_user_pipelines()

    ib.compute([pcoll1, pcoll2], blocking=True)
    self.assertTrue(pcoll1 in self.env.computed_pcollections)
    self.assertTrue(pcoll2 in self.env.computed_pcollections)

  def test_compute_with_dict_input(self):
    p = beam.Pipeline(ir.InteractiveRunner())
    pcoll1 = p | 'Create1' >> beam.Create([1, 2, 3])
    pcoll2 = p | 'Create2' >> beam.Create([4, 5, 6])
    ib.watch(locals())
    self.env.track_user_pipelines()

    ib.compute({'a': pcoll1, 'b': pcoll2}, blocking=True)
    self.assertTrue(pcoll1 in self.env.computed_pcollections)
    self.assertTrue(pcoll2 in self.env.computed_pcollections)

  def test_compute_empty_input(self):
    result = ib.compute([], blocking=True)
    self.assertIsNone(result)
    result_async = ib.compute([], blocking=False)
    self.assertIsNone(result_async)

  def test_compute_force_recompute(self):
    p = beam.Pipeline(ir.InteractiveRunner())
    pcoll = p | 'Create' >> beam.Create([1, 2, 3])
    ib.watch(locals())
    self.env.track_user_pipelines()

    ib.compute(pcoll, blocking=True)
    self.assertTrue(pcoll in self.env.computed_pcollections)

    # Mock evict_computed_pcollections to check if it's called
    with patch.object(self.env, 'evict_computed_pcollections') as mock_evict:
      ib.compute(pcoll, blocking=True, force_compute=True)
      mock_evict.assert_called_once_with(p)
    self.assertTrue(pcoll in self.env.computed_pcollections)

  def test_compute_non_blocking_exception(self):
    p = beam.Pipeline(ir.InteractiveRunner())

    def raise_error(elem):
      raise ValueError('Test Error')

    pcoll = p | 'Create' >> beam.Create([1]) | 'Error' >> beam.Map(raise_error)
    ib.watch(locals())
    self.env.track_user_pipelines()

    async_result = ib.compute(pcoll, blocking=False)
    self.assertIsInstance(async_result, AsyncComputationResult)

    with self.assertRaises(ValueError):
      async_result.result(timeout=60)

    self.assertTrue(async_result.done())
    self.assertIsInstance(async_result.exception(), ValueError)
    self.assertFalse(pcoll in self.env.computed_pcollections)

  @patch('apache_beam.runners.interactive.recording_manager.IS_IPYTHON', True)
  @patch('apache_beam.runners.interactive.recording_manager.display')
  @patch('apache_beam.runners.interactive.recording_manager.clear_output')
  @patch('apache_beam.runners.interactive.recording_manager.HTML')
  @patch('ipywidgets.Button')
  @patch('ipywidgets.FloatProgress')
  @patch('ipywidgets.Output')
  @patch('ipywidgets.HBox')
  @patch('ipywidgets.VBox')
  def test_compute_non_blocking_ipython_widgets(
      self,
      mock_vbox,
      mock_hbox,
      mock_output,
      mock_progress,
      mock_button,
      mock_html,
      mock_clear_output,
      mock_display,
  ):
    self.env._is_in_ipython = True
    p = beam.Pipeline(ir.InteractiveRunner())
    pcoll = p | 'Create' >> beam.Create(range(3))
    ib.watch(locals())
    self.env.track_user_pipelines()

    mock_controls = mock_vbox.return_value
    mock_html_instance = mock_html.return_value

    async_result = ib.compute(pcoll, blocking=False)
    self.assertIsNotNone(async_result)
    mock_button.assert_called_once_with(description='Cancel')
    mock_progress.assert_called_once()
    mock_output.assert_called_once()
    mock_hbox.assert_called_once()
    mock_vbox.assert_called_once()
    mock_html.assert_called_once_with('<p>Initializing...</p>')

    self.assertEqual(mock_display.call_count, 2)
    mock_display.assert_has_calls([
        call(mock_controls, display_id=async_result._display_id),
        call(mock_html_instance)
    ])

    mock_clear_output.assert_called_once()
    async_result.result(timeout=60)  # Let it finish

  def test_compute_dependency_wait_true(self):
    p = beam.Pipeline(ir.InteractiveRunner())
    pcoll1 = p | 'Create1' >> beam.Create([1, 2, 3])
    pcoll2 = pcoll1 | 'Map' >> beam.Map(lambda x: x * 2)
    ib.watch(locals())
    self.env.track_user_pipelines()

    rm = self.env.get_recording_manager(p)

    # Start pcoll1 computation
    async_res1 = ib.compute(pcoll1, blocking=False)
    self.assertTrue(self.env.is_pcollection_computing(pcoll1))

    # Spy on _wait_for_dependencies
    with patch.object(rm,
                      '_wait_for_dependencies',
                      wraps=rm._wait_for_dependencies) as spy_wait:
      async_res2 = ib.compute(pcoll2, blocking=False, wait_for_inputs=True)

      # Check that wait_for_dependencies was called for pcoll2
      spy_wait.assert_called_with({pcoll2}, async_res2)

      # Let pcoll1 finish
      async_res1.result(timeout=60)
      self.assertTrue(pcoll1 in self.env.computed_pcollections)
      self.assertFalse(self.env.is_pcollection_computing(pcoll1))

      # pcoll2 should now run and complete
      async_res2.result(timeout=60)
      self.assertTrue(pcoll2 in self.env.computed_pcollections)

  @patch.object(ie.InteractiveEnvironment, 'is_pcollection_computing')
  def test_compute_dependency_wait_false(self, mock_is_computing):
    p = beam.Pipeline(ir.InteractiveRunner())
    pcoll1 = p | 'Create1' >> beam.Create([1, 2, 3])
    pcoll2 = pcoll1 | 'Map' >> beam.Map(lambda x: x * 2)
    ib.watch(locals())
    self.env.track_user_pipelines()

    rm = self.env.get_recording_manager(p)

    # Pretend pcoll1 is computing
    mock_is_computing.side_effect = lambda pcoll: pcoll is pcoll1

    with patch.object(rm,
                      '_execute_pipeline_fragment',
                      wraps=rm._execute_pipeline_fragment) as spy_execute:
      async_res2 = ib.compute(pcoll2, blocking=False, wait_for_inputs=False)
      async_res2.result(timeout=60)

      # Assert that execute was called for pcoll2 without waiting
      spy_execute.assert_called_with({pcoll2}, async_res2, ANY, ANY)
      self.assertTrue(pcoll2 in self.env.computed_pcollections)

  def test_async_computation_result_cancel(self):
    p = beam.Pipeline(ir.InteractiveRunner())
    # A stream that never finishes to test cancellation
    pcoll = p | beam.Create([1]) | beam.Map(lambda x: time.sleep(100))
    ib.watch(locals())
    self.env.track_user_pipelines()

    async_result = ib.compute(pcoll, blocking=False)
    self.assertIsInstance(async_result, AsyncComputationResult)

    # Give it a moment to start
    time.sleep(0.1)

    # Mock the pipeline result's cancel method
    mock_pipeline_result = MagicMock()
    mock_pipeline_result.state = PipelineState.RUNNING
    async_result.set_pipeline_result(mock_pipeline_result)

    self.assertTrue(async_result.cancel())
    mock_pipeline_result.cancel.assert_called_once()

    # The future should be cancelled eventually by the runner
    # This part is hard to test without deeper runner integration
    with self.assertRaises(TimeoutError):
      async_result.result(timeout=1)  # It should not complete successfully

  @patch(
      'apache_beam.runners.interactive.recording_manager.RecordingManager.'
      '_execute_pipeline_fragment')
  def test_compute_multiple_async(self, mock_execute_fragment):
    p = beam.Pipeline(ir.InteractiveRunner())
    pcoll1 = p | 'Create1' >> beam.Create([1, 2, 3])
    pcoll2 = p | 'Create2' >> beam.Create([4, 5, 6])
    pcoll3 = pcoll1 | 'Map1' >> beam.Map(lambda x: x * 2)
    ib.watch(locals())
    self.env.track_user_pipelines()

    mock_pipeline_result = MagicMock()
    mock_pipeline_result.state = PipelineState.DONE
    mock_execute_fragment.return_value = mock_pipeline_result

    res1 = ib.compute(pcoll1, blocking=False)
    res2 = ib.compute(pcoll2, blocking=False)
    res3 = ib.compute(pcoll3, blocking=False)  # Depends on pcoll1

    self.assertIsNotNone(res1)
    self.assertIsNotNone(res2)
    self.assertIsNotNone(res3)

    res1.result(timeout=60)
    res2.result(timeout=60)
    res3.result(timeout=60)

    time.sleep(0.1)

    self.assertTrue(
        pcoll1 in self.env.computed_pcollections, "pcoll1 not marked computed")
    self.assertTrue(
        pcoll2 in self.env.computed_pcollections, "pcoll2 not marked computed")
    self.assertTrue(
        pcoll3 in self.env.computed_pcollections, "pcoll3 not marked computed")

    self.assertEqual(mock_execute_fragment.call_count, 3)

  @patch(
      'apache_beam.runners.interactive.interactive_beam.'
      'deferred_df_to_pcollection')
  def test_compute_input_flattening(self, mock_deferred_to_pcoll):
    p = beam.Pipeline(ir.InteractiveRunner())
    pcoll1 = p | 'C1' >> beam.Create([1])
    pcoll2 = p | 'C2' >> beam.Create([2])
    pcoll3 = p | 'C3' >> beam.Create([3])
    pcoll4 = p | 'C4' >> beam.Create([4])

    class MockDeferred(DeferredBase):
      def __init__(self, pcoll):
        mock_expr = MagicMock()
        super().__init__(mock_expr)
        self._pcoll = pcoll

      def _get_underlying_pcollection(self):
        return self._pcoll

    deferred_pcoll = MockDeferred(pcoll4)

    mock_deferred_to_pcoll.return_value = (pcoll4, p)

    ib.watch(locals())
    self.env.track_user_pipelines()

    with patch.object(self.env, 'get_recording_manager') as mock_get_rm:
      mock_rm = MagicMock()
      mock_get_rm.return_value = mock_rm
      ib.compute(pcoll1, [pcoll2], {'a': pcoll3}, deferred_pcoll)

      expected_pcolls = {pcoll1, pcoll2, pcoll3, pcoll4}
      mock_rm.compute_async.assert_called_once_with(
          expected_pcolls,
          wait_for_inputs=True,
          blocking=False,
          runner=None,
          options=None,
          force_compute=False)

  def test_compute_invalid_input_type(self):
    with self.assertRaisesRegex(ValueError,
                                "not a dict, an iterable or a PCollection"):
      ib.compute(123)

  def test_compute_mixed_pipelines(self):
    p1 = beam.Pipeline(ir.InteractiveRunner())
    pcoll1 = p1 | 'C1' >> beam.Create([1])
    p2 = beam.Pipeline(ir.InteractiveRunner())
    pcoll2 = p2 | 'C2' >> beam.Create([2])
    ib.watch(locals())
    self.env.track_user_pipelines()

    with self.assertRaisesRegex(
        ValueError, "All PCollections must belong to the same pipeline"):
      ib.compute(pcoll1, pcoll2)

  @patch(
      'apache_beam.runners.interactive.interactive_beam.'
      'deferred_df_to_pcollection')
  @patch.object(ib, 'watch')
  def test_compute_with_deferred_base(self, mock_watch, mock_deferred_to_pcoll):
    p = beam.Pipeline(ir.InteractiveRunner())
    pcoll = p | 'C1' >> beam.Create([1])

    class MockDeferred(DeferredBase):
      def __init__(self, pcoll):
        # Provide a dummy expression to satisfy DeferredBase.__init__
        mock_expr = MagicMock()
        super().__init__(mock_expr)
        self._pcoll = pcoll

      def _get_underlying_pcollection(self):
        return self._pcoll

    deferred = MockDeferred(pcoll)

    mock_deferred_to_pcoll.return_value = (pcoll, p)

    with patch.object(self.env, 'get_recording_manager') as mock_get_rm:
      mock_rm = MagicMock()
      mock_get_rm.return_value = mock_rm
      ib.compute(deferred)

      mock_deferred_to_pcoll.assert_called_once_with(deferred)
      self.assertEqual(mock_watch.call_count, 2)
      mock_watch.assert_has_calls([
          call({f'anonymous_pcollection_{id(pcoll)}': pcoll}),
          call({f'anonymous_pipeline_{id(p)}': p})
      ],
                                  any_order=False)
      mock_rm.compute_async.assert_called_once_with({pcoll},
                                                    wait_for_inputs=True,
                                                    blocking=False,
                                                    runner=None,
                                                    options=None,
                                                    force_compute=False)

  def test_compute_new_pipeline(self):
    p = beam.Pipeline(ir.InteractiveRunner())
    pcoll = p | 'Create' >> beam.Create([1])
    # NOT calling ib.watch() or track_user_pipelines()

    with patch.object(self.env, 'get_recording_manager') as mock_get_rm, \
        patch.object(ib, 'watch') as mock_watch:
      mock_rm = MagicMock()
      mock_get_rm.return_value = mock_rm
      ib.compute(pcoll)

      mock_watch.assert_called_with({f'anonymous_pipeline_{id(p)}': p})
      mock_get_rm.assert_called_once_with(p, create_if_absent=True)
      mock_rm.compute_async.assert_called_once()


if __name__ == '__main__':
  unittest.main()
