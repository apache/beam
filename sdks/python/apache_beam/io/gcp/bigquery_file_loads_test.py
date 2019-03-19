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

"""Unit tests for BigQuery file loads utilities."""

from __future__ import absolute_import

import json
import logging
import os
import random
import time
import unittest

import mock
from hamcrest.core import assert_that as hamcrest_assert
from hamcrest.core.core.allof import all_of
from hamcrest.core.core.is_ import is_
from nose.plugins.attrib import attr

import apache_beam as beam
from apache_beam.io.filebasedsink_test import _TestCaseWithTempDirCleanUp
from apache_beam.io.gcp import bigquery_file_loads as bqfl
from apache_beam.io.gcp import bigquery
from apache_beam.io.gcp import bigquery_tools
from apache_beam.io.gcp.internal.clients import bigquery as bigquery_api
from apache_beam.io.gcp.tests.bigquery_matcher import BigqueryFullResultMatcher
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

try:
  from apitools.base.py.exceptions import HttpError
except ImportError:
  HttpError = None


_DESTINATION_ELEMENT_PAIRS = [
    # DESTINATION 1
    ('project1:dataset1.table1', '{"name":"beam", "language":"py"}'),
    ('project1:dataset1.table1', '{"name":"beam", "language":"java"}'),
    ('project1:dataset1.table1', '{"name":"beam", "language":"go"}'),
    ('project1:dataset1.table1', '{"name":"flink", "language":"java"}'),
    ('project1:dataset1.table1', '{"name":"flink", "language":"scala"}'),

    # DESTINATION 3
    ('project1:dataset1.table3', '{"name":"spark", "language":"scala"}'),

    # DESTINATION 1
    ('project1:dataset1.table1', '{"name":"spark", "language":"py"}'),
    ('project1:dataset1.table1', '{"name":"spark", "language":"scala"}'),

    # DESTINATION 2
    ('project1:dataset1.table2', '{"name":"beam", "foundation":"apache"}'),
    ('project1:dataset1.table2', '{"name":"flink", "foundation":"apache"}'),
    ('project1:dataset1.table2', '{"name":"spark", "foundation":"apache"}'),
]

_NAME_LANGUAGE_ELEMENTS = [
    json.loads(elm[1])
    for elm in _DESTINATION_ELEMENT_PAIRS if "language" in elm[1]
]


_DISTINCT_DESTINATIONS = list(
    set([elm[0] for elm in _DESTINATION_ELEMENT_PAIRS]))


_ELEMENTS = list([json.loads(elm[1]) for elm in _DESTINATION_ELEMENT_PAIRS])


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class TestWriteRecordsToFile(_TestCaseWithTempDirCleanUp):
  maxDiff = None

  def _consume_input(self, fn, checks=None):
    if checks is None:
      return

    with TestPipeline() as p:
      output_pcs = (
          p
          | beam.Create(_DESTINATION_ELEMENT_PAIRS)
          | beam.ParDo(fn, self.tmpdir)
          .with_outputs(fn.WRITTEN_FILE_TAG, fn.UNWRITTEN_RECORD_TAG))

      checks(output_pcs)
      return output_pcs

  def test_files_created(self):
    """Test that the files are created and written."""

    fn = bqfl.WriteRecordsToFile()
    self.tmpdir = self._new_tempdir()

    def check_files_created(output_pcs):
      dest_file_pc = output_pcs[bqfl.WriteRecordsToFile.WRITTEN_FILE_TAG]

      files = dest_file_pc | "GetFiles" >> beam.Map(lambda x: x[1])
      file_count = files | "CountFiles" >> beam.combiners.Count.Globally()

      _ = files | "FilesExist" >> beam.Map(
          lambda x: hamcrest_assert(os.path.exists(x), is_(True)))
      assert_that(file_count, equal_to([3]), label='check file count')

      destinations = (
          dest_file_pc
          | "GetDests" >> beam.Map(
              lambda x: bigquery_tools.get_hashable_destination(x[0])))
      assert_that(destinations, equal_to(list(_DISTINCT_DESTINATIONS)),
                  label='check destinations ')

    self._consume_input(fn, check_files_created)

  def test_many_files(self):
    """Forces records to be written to many files.

    For each destination multiple files are necessary. This is because the max
    file length is very small, so only a couple records fit in each file.
    """

    fn = bqfl.WriteRecordsToFile(max_file_size=50)
    self.tmpdir = self._new_tempdir()

    def check_many_files(output_pcs):
      dest_file_pc = output_pcs[bqfl.WriteRecordsToFile.WRITTEN_FILE_TAG]

      files_per_dest = (dest_file_pc
                        | beam.Map(lambda x: x).with_output_types(
                            beam.typehints.KV[str, str])
                        | beam.combiners.Count.PerKey())
      files_per_dest = (
          files_per_dest
          | "GetDests" >> beam.Map(
              lambda x: (bigquery_tools.get_hashable_destination(x[0]),
                         x[1]))
      )
      assert_that(files_per_dest,
                  equal_to([('project1:dataset1.table1', 4),
                            ('project1:dataset1.table2', 2),
                            ('project1:dataset1.table3', 1)]))

      # Check that the files exist
      _ = dest_file_pc | beam.Map(lambda x: x[1]) | beam.Map(
          lambda x: hamcrest_assert(os.path.exists(x), is_(True)))

    self._consume_input(fn, check_many_files)

  def test_records_are_spilled(self):
    """Forces records to be written to many files.

    For each destination multiple files are necessary, and at most two files can
    be created. This forces records to be spilled to the next stage of
    processing.
    """

    fn = bqfl.WriteRecordsToFile(max_files_per_bundle=2)
    self.tmpdir = self._new_tempdir()

    def check_many_files(output_pcs):
      dest_file_pc = output_pcs[bqfl.WriteRecordsToFile.WRITTEN_FILE_TAG]
      spilled_records_pc = output_pcs[
          bqfl.WriteRecordsToFile.UNWRITTEN_RECORD_TAG]

      spilled_records_count = (spilled_records_pc |
                               beam.combiners.Count.Globally())
      assert_that(spilled_records_count, equal_to([3]), label='spilled count')

      files_per_dest = (dest_file_pc
                        | beam.Map(lambda x: x).with_output_types(
                            beam.typehints.KV[str, str])
                        | beam.combiners.Count.PerKey())
      files_per_dest = (
          files_per_dest
          | "GetDests" >> beam.Map(
              lambda x: (bigquery_tools.get_hashable_destination(x[0]),
                         x[1])))

      # Only table1 and table3 get files. table2 records get spilled.
      assert_that(files_per_dest,
                  equal_to([('project1:dataset1.table1', 1),
                            ('project1:dataset1.table3', 1)]),
                  label='file count')

      # Check that the files exist
      _ = dest_file_pc | beam.Map(lambda x: x[1]) | beam.Map(
          lambda x: hamcrest_assert(os.path.exists(x), is_(True)))

    self._consume_input(fn, check_many_files)


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class TestWriteGroupedRecordsToFile(_TestCaseWithTempDirCleanUp):

  def _consume_input(self, fn, input, checks):
    if checks is None:
      return

    with TestPipeline() as p:
      res = (p
             | beam.Create(input)
             | beam.GroupByKey()
             | beam.ParDo(fn, self.tmpdir))

      checks(res)
      return res

  def test_files_are_created(self):
    """Test that the files are created and written."""

    fn = bqfl.WriteGroupedRecordsToFile()
    self.tmpdir = self._new_tempdir()

    def check_files_created(output_pc):
      files = output_pc | "GetFiles" >> beam.Map(lambda x: x[1])
      file_count = files | "CountFiles" >> beam.combiners.Count.Globally()

      _ = files | "FilesExist" >> beam.Map(
          lambda x: hamcrest_assert(os.path.exists(x), is_(True)))
      assert_that(file_count, equal_to([3]), label='check file count')

      destinations = (
          output_pc
          | "GetDests" >> beam.Map(
              lambda x: bigquery_tools.get_hashable_destination(x[0])))
      assert_that(destinations, equal_to(list(_DISTINCT_DESTINATIONS)),
                  label='check destinations ')

    self._consume_input(
        fn, _DESTINATION_ELEMENT_PAIRS, check_files_created)

  def test_multiple_files(self):
    """Forces records to be written to many files.

    For each destination multiple files are necessary. This is because the max
    file length is very small, so only a couple records fit in each file.
    """
    fn = bqfl.WriteGroupedRecordsToFile(max_file_size=50)
    self.tmpdir = self._new_tempdir()

    def check_multiple_files(output_pc):
      files_per_dest = output_pc | beam.combiners.Count.PerKey()
      files_per_dest = (
          files_per_dest
          | "GetDests" >> beam.Map(
              lambda x: (bigquery_tools.get_hashable_destination(x[0]),
                         x[1])))
      assert_that(files_per_dest,
                  equal_to([('project1:dataset1.table1', 4),
                            ('project1:dataset1.table2', 2),
                            ('project1:dataset1.table3', 1), ]))

      # Check that the files exist
      _ = output_pc | beam.Map(lambda x: x[1]) | beam.Map(os.path.exists)

    self._consume_input(fn, _DESTINATION_ELEMENT_PAIRS, check_multiple_files)


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class TestBigQueryFileLoads(_TestCaseWithTempDirCleanUp):

  def test_records_traverse_transform_with_mocks(self):
    destination = 'project1:dataset1.table1'

    job_reference = bigquery_api.JobReference()
    job_reference.projectId = 'project1'
    job_reference.jobId = 'job_name1'
    result_job = bigquery_api.Job()
    result_job.jobReference = job_reference

    mock_job = mock.Mock()
    mock_job.status.state = 'DONE'
    mock_job.status.errorResult = None
    mock_job.jobReference = job_reference

    bq_client = mock.Mock()
    bq_client.jobs.Get.return_value = mock_job

    bq_client.jobs.Insert.return_value = result_job

    transform = bqfl.BigQueryBatchFileLoads(
        destination,
        custom_gcs_temp_location=self._new_tempdir(),
        test_client=bq_client,
        validate=False)

    # Need to test this with the DirectRunner to avoid serializing mocks
    with TestPipeline('DirectRunner') as p:
      outputs = p | beam.Create(_ELEMENTS) | transform

      dest_files = outputs[bqfl.BigQueryBatchFileLoads.DESTINATION_FILE_PAIRS]
      dest_job = outputs[bqfl.BigQueryBatchFileLoads.DESTINATION_JOBID_PAIRS]

      jobs = dest_job | "GetJobs" >> beam.Map(lambda x: x[1])

      files = dest_files | "GetFiles" >> beam.Map(lambda x: x[1])
      destinations = (
          dest_files
          | "GetDests" >> beam.Map(
              lambda x: (
                  bigquery_tools.get_hashable_destination(x[0]), x[1]))
          | "GetUniques" >> beam.combiners.Count.PerKey()
          | "GetFinalDests" >>beam.Keys())

      # All files exist
      _ = (files | beam.Map(
          lambda x: hamcrest_assert(os.path.exists(x), is_(True))))

      # One file per destination
      assert_that(files | beam.combiners.Count.Globally(),
                  equal_to([1]),
                  label='CountFiles')

      assert_that(destinations,
                  equal_to([destination]),
                  label='CheckDestinations')

      assert_that(jobs,
                  equal_to([job_reference]), label='CheckJobs')


@unittest.skipIf(HttpError is None, 'GCP dependencies are not installed')
class BigQueryFileLoadsIT(unittest.TestCase):

  BIG_QUERY_DATASET_ID = 'python_bq_file_loads_'
  BIG_QUERY_SCHEMA = (
      '{"fields": [{"name": "name","type": "STRING"},'
      '{"name": "language","type": "STRING"}]}'
  )

  BIG_QUERY_SCHEMA_2 = (
      '{"fields": [{"name": "name","type": "STRING"},'
      '{"name": "foundation","type": "STRING"}]}'
  )

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.runner_name = type(self.test_pipeline.runner).__name__
    self.project = self.test_pipeline.get_option('project')

    self.dataset_id = '%s%s%d' % (self.BIG_QUERY_DATASET_ID,
                                  str(int(time.time())),
                                  random.randint(0, 10000))
    self.bigquery_client = bigquery_tools.BigQueryWrapper()
    self.bigquery_client.get_or_create_dataset(self.project, self.dataset_id)
    self.output_table = "%s.output_table" % (self.dataset_id)
    logging.info("Created dataset %s in project %s",
                 self.dataset_id, self.project)

  @attr('IT')
  def test_multiple_destinations_transform(self):
    output_table_1 = '%s%s' % (self.output_table, 1)
    output_table_2 = '%s%s' % (self.output_table, 2)
    output_table_3 = '%s%s' % (self.output_table, 3)
    output_table_4 = '%s%s' % (self.output_table, 4)
    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT * FROM %s" % output_table_1,
            data=[(d['name'], d['language'])
                  for d in _ELEMENTS
                  if 'language' in d]),
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT * FROM %s" % output_table_2,
            data=[(d['name'], d['foundation'])
                  for d in _ELEMENTS
                  if 'foundation' in d]),
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT * FROM %s" % output_table_3,
            data=[(d['name'], d['language'])
                  for d in _ELEMENTS
                  if 'language' in d]),
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT * FROM %s" % output_table_4,
            data=[(d['name'], d['foundation'])
                  for d in _ELEMENTS
                  if 'foundation' in d])]

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=all_of(*pipeline_verifiers),
        experiments='use_beam_bq_sink')

    with beam.Pipeline(argv=args) as p:
      input = p | beam.Create(_ELEMENTS)

      # Get all input in same machine
      input = (input
               | beam.Map(lambda x: (None, x))
               | beam.GroupByKey()
               | beam.FlatMap(lambda elm: elm[1]))

      _ = (input |
           "WriteWithMultipleDestsFreely" >> bigquery.WriteToBigQuery(
               table=lambda x: (output_table_1
                                if 'language' in x
                                else output_table_2),
               create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
               write_disposition=beam.io.BigQueryDisposition.WRITE_EMPTY))

      _ = (input |
           "WriteWithMultipleDests" >> bigquery.WriteToBigQuery(
               table=lambda x: (output_table_3
                                if 'language' in x
                                else output_table_4),
               create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
               write_disposition=beam.io.BigQueryDisposition.WRITE_EMPTY,
               max_file_size=20,
               max_files_per_bundle=-1))

  @attr('IT')
  def test_one_job_fails_all_jobs_fail(self):

    # If one of the import jobs fails, then other jobs must not be performed.
    # This is to avoid reinsertion of some records when a pipeline fails and
    # is rerun.
    output_table_1 = '%s%s' % (self.output_table, 1)
    output_table_2 = '%s%s' % (self.output_table, 2)

    self.bigquery_client.get_or_create_table(
        self.project, self.dataset_id, output_table_1.split('.')[1],
        bigquery_tools.parse_table_schema_from_json(self.BIG_QUERY_SCHEMA),
        None, None)
    self.bigquery_client.get_or_create_table(
        self.project, self.dataset_id, output_table_2.split('.')[1],
        bigquery_tools.parse_table_schema_from_json(self.BIG_QUERY_SCHEMA_2),
        None, None)

    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT * FROM %s" % output_table_1,
            data=[]),
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT * FROM %s" % output_table_2,
            data=[])]

    args = self.test_pipeline.get_full_options_as_args(
        experiments='use_beam_bq_sink')

    with self.assertRaises(Exception):
      with beam.Pipeline(argv=args) as p:
        input = p | beam.Create(_ELEMENTS)
        input2 = p | "Broken record" >> beam.Create(['language_broken_record'])

        input = (input, input2) | beam.Flatten()

        _ = (input |
             "WriteWithMultipleDests" >> bigquery.WriteToBigQuery(
                 table=lambda x: (output_table_1
                                  if 'language' in x
                                  else output_table_2),
                 create_disposition=(
                     beam.io.BigQueryDisposition.CREATE_IF_NEEDED),
                 write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))

    hamcrest_assert(p, all_of(*pipeline_verifiers))

  def tearDown(self):
    request = bigquery_api.BigqueryDatasetsDeleteRequest(
        projectId=self.project, datasetId=self.dataset_id,
        deleteContents=True)
    try:
      logging.info("Deleting dataset %s in project %s",
                   self.dataset_id, self.project)
      self.bigquery_client.client.datasets.Delete(request)
    except HttpError:
      logging.debug('Failed to clean up dataset %s in project %s',
                    self.dataset_id, self.project)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
