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

"""Tests for interactive_environment_inspector."""
# pytype: skip-file

import json
import sys
import unittest

import apache_beam as beam
import apache_beam.runners.interactive.messaging.interactive_environment_inspector as inspector
from apache_beam.runners.interactive import interactive_beam as ib
from apache_beam.runners.interactive import interactive_environment as ie
from apache_beam.runners.interactive import interactive_runner as ir
from apache_beam.runners.interactive.dataproc.types import ClusterMetadata
from apache_beam.runners.interactive.testing.mock_env import isolated_env
from apache_beam.runners.interactive.utils import obfuscate


@unittest.skipIf(
    not ie.current_env().is_interactive_ready,
    '[interactive] dependency is not installed.')
@unittest.skipIf(
    sys.version_info < (3, 7), 'The tests require at least Python 3.7 to work.')
@isolated_env
class InteractiveEnvironmentInspectorTest(unittest.TestCase):
  def test_inspect(self):
    with self.cell:  # Cell 1
      pipeline = beam.Pipeline(ir.InteractiveRunner())
      # Early watch the pipeline so that cell re-execution can be handled.
      ib.watch({'pipeline': pipeline})
      # pylint: disable=bad-option-value
      pcoll = pipeline | 'Create' >> beam.Create(range(10))

    with self.cell:  # Cell 2
      # Re-executes the line that created the pcoll causing the original
      # pcoll no longer inspectable.
      # pylint: disable=bad-option-value
      pcoll = pipeline | 'Create' >> beam.Create(range(10))

    ib.watch(locals())
    pipeline_metadata = inspector.meta('pipeline', pipeline)
    pcoll_metadata = inspector.meta('pcoll', pcoll)
    expected_inspectables = {
        obfuscate(pipeline_metadata): {
            'metadata': pipeline_metadata, 'value': pipeline
        },
        obfuscate(pcoll_metadata): {
            'metadata': pcoll_metadata, 'value': pcoll
        }
    }
    for inspectable in inspector.inspect().items():
      self.assertTrue(inspectable in expected_inspectables.items())

  def test_inspect_pipelines(self):
    with self.cell:  # Cell 1
      pipeline_1 = beam.Pipeline(ir.InteractiveRunner())
      pipeline_2 = beam.Pipeline(ir.InteractiveRunner())

    with self.cell:  # Cell 2
      # Re-executes the line that created pipeline_1 causing the original
      # pipeline_1 no longer inspectable.
      pipeline_1 = beam.Pipeline(ir.InteractiveRunner())

    ib.watch(locals())
    expected_inspectable_pipelines = {
        pipeline_1: 'pipeline_1', pipeline_2: 'pipeline_2'
    }
    for inspectable_pipeline in inspector.inspect_pipelines().items():
      self.assertTrue(
          inspectable_pipeline in expected_inspectable_pipelines.items())

    pipeline_1_metadata = inspector.meta('pipeline_1', pipeline_1)
    pipeline_2_metadata = inspector.meta('pipeline_2', pipeline_2)
    expected_inspectables = {
        obfuscate(pipeline_2_metadata): {
            'metadata': pipeline_2_metadata, 'value': pipeline_2
        },
        obfuscate(pipeline_1_metadata): {
            'metadata': pipeline_1_metadata, 'value': pipeline_1
        }
    }
    for inspectable in inspector.inspect().items():
      self.assertTrue(inspectable in expected_inspectables.items())

  def test_list_inspectables(self):
    with self.cell:  # Cell 1
      pipeline = beam.Pipeline(ir.InteractiveRunner())
      # pylint: disable=bad-option-value
      pcoll_1 = pipeline | 'Create' >> beam.Create(range(10))
      pcoll_2 = pcoll_1 | 'Square' >> beam.Map(lambda x: x * x)

    with self.cell:  # Cell 2
      # Re-executes the line that created pipeline causing the original
      # pipeline become an anonymous pipeline that is still inspectable because
      # its pcoll_1 and pcoll_2 are still inspectable.
      pipeline = beam.Pipeline(ir.InteractiveRunner())

    ib.watch(locals())
    anonymous_pipeline_name = inspector.synthesize_pipeline_name(
        pcoll_1.pipeline)
    anonymous_pipeline_metadata = inspector.meta(
        anonymous_pipeline_name, pcoll_1.pipeline)
    pipeline_metadata = inspector.meta('pipeline', pipeline)
    pcoll_1_metadata = inspector.meta('pcoll_1', pcoll_1)
    pcoll_2_metadata = inspector.meta('pcoll_2', pcoll_2)
    expected_inspectable_list = {
        obfuscate(pipeline_metadata): {
            'metadata': pipeline_metadata, 'pcolls': {}
        },
        obfuscate(anonymous_pipeline_metadata): {
            'metadata': anonymous_pipeline_metadata,
            'pcolls': {
                obfuscate(pcoll_1_metadata): pcoll_1_metadata,
                obfuscate(pcoll_2_metadata): pcoll_2_metadata
            }
        }
    }
    ins = inspector.InteractiveEnvironmentInspector()
    actual_listings = ins.list_inspectables()
    self.assertEqual(actual_listings, json.dumps(expected_inspectable_list))

  def test_get_val(self):
    with self.cell:  # Cell 1
      pipeline = beam.Pipeline(ir.InteractiveRunner())
      # pylint: disable=bad-option-value
      pcoll = pipeline | 'Create' >> beam.Create(range(10))

    with self.cell:  # Cell 2
      # Re-executes the line that created pipeline causing the original
      # pipeline become an anonymous pipeline that is still inspectable because
      # its pcoll is still inspectable.
      pipeline = beam.Pipeline(ir.InteractiveRunner())

    ib.watch(locals())
    ins = inspector.InteractiveEnvironmentInspector()
    _ = ins.list_inspectables()
    pipeline_identifier = obfuscate(inspector.meta('pipeline', pipeline))
    self.assertIs(ins.get_val(pipeline_identifier), pipeline)
    pcoll_identifier = obfuscate(inspector.meta('pcoll', pcoll))
    self.assertIs(ins.get_val(pcoll_identifier), pcoll)
    anonymous_pipeline_name = inspector.synthesize_pipeline_name(pcoll.pipeline)
    anonymous_pipeline_identifier = obfuscate(
        inspector.meta(anonymous_pipeline_name, pcoll.pipeline))
    self.assertIs(ins.get_val(anonymous_pipeline_identifier), pcoll.pipeline)

  def test_get_pcoll_data(self):
    pipeline = beam.Pipeline(ir.InteractiveRunner())
    # pylint: disable=bad-option-value
    pcoll = pipeline | 'Create' >> beam.Create(list(range(10)))
    counts = pcoll | beam.combiners.Count.PerElement()

    ib.watch(locals())
    ie.current_env().track_user_pipelines()
    counts_identifier = obfuscate(inspector.meta('counts', counts))
    ins = inspector.InteractiveEnvironmentInspector()
    _ = ins.list_inspectables()

    actual_counts_pcoll_data = ins.get_pcoll_data(counts_identifier)
    expected_counts_pcoll_data = ib.collect(
        counts, n=10).to_json(orient='table')
    self.assertEqual(actual_counts_pcoll_data, expected_counts_pcoll_data)

    actual_counts_with_window_info = ins.get_pcoll_data(counts_identifier, True)
    expected_counts_with_window_info = ib.collect(
        counts, include_window_info=True).to_json(orient='table')
    self.assertEqual(
        actual_counts_with_window_info, expected_counts_with_window_info)

  def test_list_clusters(self):
    self.current_env.options.cache_root = 'gs://fake'
    meta = ClusterMetadata(project_id='project')
    dcm = self.current_env.clusters.create(meta)
    p = beam.Pipeline()
    dcm.pipelines.add(p)
    self.current_env.clusters.pipelines[p] = dcm
    cluster_id = obfuscate(meta)
    self.assertEqual({
        cluster_id: {
            'cluster_name': meta.cluster_name,
            'project': meta.project_id,
            'region': meta.region,
            'master_url': meta.master_url,
            'dashboard': meta.dashboard,
            'pipelines': [str(id(p)) for p in dcm.pipelines]
        }
    },
                     json.loads(self.current_env.inspector.list_clusters()))
    self.current_env.options.cache_root = None


if __name__ == '__main__':
  unittest.main()
