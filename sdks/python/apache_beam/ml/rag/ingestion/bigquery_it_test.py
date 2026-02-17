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

import logging
import os
import secrets
import time
import unittest

import hamcrest as hc
import pytest

import apache_beam as beam
from apache_beam.io.gcp.bigquery_tools import BigQueryWrapper
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io.gcp.tests.bigquery_matcher import BigqueryFullResultMatcher
from apache_beam.ml.rag.ingestion.bigquery import BigQueryVectorWriterConfig
from apache_beam.ml.rag.ingestion.bigquery import SchemaConfig
from apache_beam.ml.rag.types import Chunk
from apache_beam.ml.rag.types import Content
from apache_beam.ml.rag.types import Embedding
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms.periodicsequence import PeriodicImpulse


@pytest.mark.uses_gcp_java_expansion_service
@unittest.skipUnless(
    os.environ.get('EXPANSION_JARS'),
    "EXPANSION_JARS environment var is not provided, "
    "indicating that jars have not been built")
class BigQueryVectorWriterConfigTest(unittest.TestCase):
  BIG_QUERY_DATASET_ID = 'python_rag_bigquery_'

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self._runner = type(self.test_pipeline.runner).__name__
    self.project = self.test_pipeline.get_option('project')

    self.bigquery_client = BigQueryWrapper()
    self.dataset_id = '%s%d%s' % (
        self.BIG_QUERY_DATASET_ID, int(time.time()), secrets.token_hex(3))
    self.bigquery_client.get_or_create_dataset(self.project, self.dataset_id)
    _LOGGER = logging.getLogger(__name__)
    _LOGGER.info(
        "Created dataset %s in project %s", self.dataset_id, self.project)

  def tearDown(self):
    request = bigquery.BigqueryDatasetsDeleteRequest(
        projectId=self.project, datasetId=self.dataset_id, deleteContents=True)
    try:
      _LOGGER = logging.getLogger(__name__)
      _LOGGER.info(
          "Deleting dataset %s in project %s", self.dataset_id, self.project)
      self.bigquery_client.client.datasets.Delete(request)
    # Failing to delete a dataset should not cause a test failure.
    except Exception:
      _LOGGER = logging.getLogger(__name__)
      _LOGGER.debug(
          'Failed to clean up dataset %s in project %s',
          self.dataset_id,
          self.project)

  def test_default_schema(self):
    table_name = 'python_default_schema_table'
    table_id = '{}.{}.{}'.format(self.project, self.dataset_id, table_name)

    config = BigQueryVectorWriterConfig(write_config={'table': table_id})
    chunks = [
        Chunk(
            id="1",
            embedding=Embedding(dense_embedding=[0.1, 0.2]),
            content=Content(text="foo"),
            metadata={"a": "b"}),
        Chunk(
            id="2",
            embedding=Embedding(dense_embedding=[0.3, 0.4]),
            content=Content(text="bar"),
            metadata={"c": "d"})
    ]

    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT id, content, embedding, metadata FROM %s" % table_id,
            data=[("1", "foo", [0.1, 0.2], [{
                "key": "a", "value": "b"
            }]), ("2", "bar", [0.3, 0.4], [{
                "key": "c", "value": "d"
            }])])
    ]

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=hc.all_of(*pipeline_verifiers))
    with beam.Pipeline(argv=args) as p:
      _ = (p | beam.Create(chunks) | config.create_write_transform())

  def test_default_schema_missing_embedding(self):
    table_name = 'python_default_schema_table'
    table_id = '{}.{}.{}'.format(self.project, self.dataset_id, table_name)

    config = BigQueryVectorWriterConfig(write_config={'table': table_id})
    chunks = [
        Chunk(id="1", content=Content(text="foo"), metadata={"a": "b"}),
        Chunk(id="2", content=Content(text="bar"), metadata={"c": "d"})
    ]
    with self.assertRaisesRegex(Exception, "must contain dense embedding"):
      with beam.Pipeline() as p:
        _ = (p | beam.Create(chunks) | config.create_write_transform())

  def test_custom_schema(self):
    table_name = 'python_custom_schema_table'
    table_id = '{}.{}.{}'.format(self.project, self.dataset_id, table_name)

    schema_config = SchemaConfig(
        schema={
            'fields': [{
                'name': 'id', 'type': 'STRING'
            },
                       {
                           'name': 'embedding',
                           'type': 'FLOAT64',
                           'mode': 'REPEATED'
                       }, {
                           'name': 'source', 'type': 'STRING'
                       }]
        },
        chunk_to_dict_fn=lambda chunk: {
            'id': chunk.id, 'embedding': chunk.embedding.dense_embedding,
            'source': chunk.metadata.get('source')
        })
    config = BigQueryVectorWriterConfig(
        write_config={'table': table_id}, schema_config=schema_config)
    chunks = [
        Chunk(
            id="1",
            embedding=Embedding(dense_embedding=[0.1, 0.2]),
            content=Content(text="foo content"),
            metadata={"source": "foo"}),
        Chunk(
            id="2",
            embedding=Embedding(dense_embedding=[0.3, 0.4]),
            content=Content(text="bar content"),
            metadata={"source": "bar"})
    ]

    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT id, embedding, source FROM %s" % table_id,
            data=[("1", [0.1, 0.2], "foo"), ("2", [0.3, 0.4], "bar")])
    ]

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=hc.all_of(*pipeline_verifiers))

    with beam.Pipeline(argv=args) as p:
      _ = (p | beam.Create(chunks) | config.create_write_transform())

  def test_streaming_default_schema(self):
    self.skip_if_not_dataflow_runner()

    table_name = 'python_streaming_default_schema_table'
    table_id = '{}.{}.{}'.format(self.project, self.dataset_id, table_name)

    config = BigQueryVectorWriterConfig(write_config={'table': table_id})
    chunks = [
        Chunk(
            id="1",
            embedding=Embedding(dense_embedding=[0.1, 0.2]),
            content=Content(text="foo"),
            metadata={"a": "b"}),
        Chunk(
            id="2",
            embedding=Embedding(dense_embedding=[0.3, 0.4]),
            content=Content(text="bar"),
            metadata={"c": "d"}),
        Chunk(
            id="3",
            embedding=Embedding(dense_embedding=[0.5, 0.6]),
            content=Content(text="foo"),
            metadata={"e": "f"}),
        Chunk(
            id="4",
            embedding=Embedding(dense_embedding=[0.7, 0.8]),
            content=Content(text="bar"),
            metadata={"g": "h"})
    ]

    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT id, content, embedding, metadata FROM %s" % table_id,
            data=[("1", "foo", [0.1, 0.2], [{
                "key": "a", "value": "b"
            }]), ("2", "bar", [0.3, 0.4], [{
                "key": "c", "value": "d"
            }]), ("3", "foo", [0.5, 0.6], [{
                "key": "e", "value": "f"
            }]), ("4", "bar", [0.7, 0.8], [{
                "key": "g", "value": "h"
            }])])
    ]
    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=hc.all_of(*pipeline_verifiers),
        streaming=True,
        allow_unsafe_triggers=True)

    with beam.Pipeline(argv=args) as p:
      _ = (
          p
          | PeriodicImpulse(0, 4, 1)
          | beam.Map(lambda t: chunks[t])
          | config.create_write_transform())

  def skip_if_not_dataflow_runner(self):
    # skip if dataflow runner is not specified
    if not self._runner or "dataflowrunner" not in self._runner.lower():
      self.skipTest(
          "Streaming with exactly-once route has the requirement "
          "`beam:requirement:pardo:on_window_expiration:v1`, "
          "which is currently only supported by the Dataflow runner")


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
