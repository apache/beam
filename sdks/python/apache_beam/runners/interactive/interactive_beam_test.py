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
from typing import NamedTuple
from unittest.mock import patch

import apache_beam as beam
from apache_beam import dataframe as frames
from apache_beam.options.pipeline_options import FlinkRunnerOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import interactive_runner as ir
from apache_beam.runners.interactive.dataproc.dataproc_cluster_manager import DataprocClusterManager
from apache_beam.runners.interactive.dataproc.types import ClusterMetadata
from apache_beam.runners.interactive.options.capture_limiters import Limiter
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


if __name__ == '__main__':
  unittest.main()
