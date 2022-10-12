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

# pytype: skip-file

import logging
import os
import random
import time
import unittest

import mock
import pytest
from hamcrest.core import assert_that as hamcrest_assert
from hamcrest.core.core.allof import all_of
from hamcrest.core.core.is_ import is_
from parameterized import param
from parameterized import parameterized

import apache_beam as beam
from apache_beam.io.filebasedsink_test import _TestCaseWithTempDirCleanUp
from apache_beam.io.gcp import bigquery_file_loads as bqfl
from apache_beam.io.gcp import bigquery
from apache_beam.io.gcp import bigquery_tools
from apache_beam.io.gcp.internal.clients import bigquery as bigquery_api
from apache_beam.io.gcp.tests.bigquery_matcher import BigqueryFullResultMatcher
from apache_beam.io.gcp.tests.bigquery_matcher import BigqueryFullResultStreamingMatcher
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.runners.dataflow.test_dataflow_runner import TestDataflowRunner
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms import combiners
from apache_beam.transforms.window import TimestampedValue
from apache_beam.typehints.typehints import Tuple
from apache_beam.utils import timestamp

try:
  from apitools.base.py.exceptions import HttpError
except ImportError:
  raise unittest.SkipTest('GCP dependencies are not installed')

_LOGGER = logging.getLogger(__name__)

_DESTINATION_ELEMENT_PAIRS = [
    # DESTINATION 1
    ('project1:dataset1.table1', {
        'name': 'beam', 'language': 'py'
    }),
    ('project1:dataset1.table1', {
        'name': 'beam', 'language': 'java'
    }),
    ('project1:dataset1.table1', {
        'name': 'beam', 'language': 'go'
    }),
    ('project1:dataset1.table1', {
        'name': 'flink', 'language': 'java'
    }),
    ('project1:dataset1.table1', {
        'name': 'flink', 'language': 'scala'
    }),

    # DESTINATION 3
    ('project1:dataset1.table3', {
        'name': 'spark', 'language': 'scala'
    }),

    # DESTINATION 1
    ('project1:dataset1.table1', {
        'name': 'spark', 'language': 'py'
    }),
    ('project1:dataset1.table1', {
        'name': 'spark', 'language': 'scala'
    }),

    # DESTINATION 2
    ('project1:dataset1.table2', {
        'name': 'beam', 'foundation': 'apache'
    }),
    ('project1:dataset1.table2', {
        'name': 'flink', 'foundation': 'apache'
    }),
    ('project1:dataset1.table2', {
        'name': 'spark', 'foundation': 'apache'
    }),
]

_DISTINCT_DESTINATIONS = list({elm[0] for elm in _DESTINATION_ELEMENT_PAIRS})

_ELEMENTS = [elm[1] for elm in _DESTINATION_ELEMENT_PAIRS]

_ELEMENTS_SCHEMA = bigquery.WriteToBigQuery.get_dict_table_schema(
    bigquery_api.TableSchema(
        fields=[
            bigquery_api.TableFieldSchema(
                name="name", type="STRING", mode="REQUIRED"),
            bigquery_api.TableFieldSchema(name="language", type="STRING"),
            bigquery_api.TableFieldSchema(name="foundation", type="STRING"),
        ]))


class TestWriteRecordsToFile(_TestCaseWithTempDirCleanUp):
  maxDiff = None

  def _consume_input(self, fn, checks=None):
    if checks is None:
      return

    with TestPipeline() as p:
      output_pcs = (
          p
          | beam.Create(_DESTINATION_ELEMENT_PAIRS, reshuffle=False)
          | beam.ParDo(fn, self.tmpdir).with_outputs(
              fn.WRITTEN_FILE_TAG, fn.UNWRITTEN_RECORD_TAG))

      checks(output_pcs)
      return output_pcs

  @parameterized.expand([
      param(file_format=bigquery_tools.FileFormat.AVRO),
      param(file_format=bigquery_tools.FileFormat.JSON),
      param(file_format=None),
  ])
  def test_files_created(self, file_format):
    """Test that the files are created and written."""

    fn = bqfl.WriteRecordsToFile(
        schema=_ELEMENTS_SCHEMA, file_format=file_format)
    self.tmpdir = self._new_tempdir()

    def check_files_created(output_pcs):
      dest_file_pc = output_pcs[bqfl.WriteRecordsToFile.WRITTEN_FILE_TAG]

      files = dest_file_pc | "GetFiles" >> beam.Map(lambda x: x[1][0])
      file_count = files | "CountFiles" >> combiners.Count.Globally()

      _ = files | "FilesExist" >> beam.Map(
          lambda x: hamcrest_assert(os.path.exists(x), is_(True)))
      assert_that(file_count, equal_to([3]), label='check file count')

      destinations = (
          dest_file_pc
          | "GetDests" >>
          beam.Map(lambda x: bigquery_tools.get_hashable_destination(x[0])))
      assert_that(
          destinations,
          equal_to(list(_DISTINCT_DESTINATIONS)),
          label='check destinations ')

    self._consume_input(fn, check_files_created)

  def test_many_files(self):
    """Forces records to be written to many files.

    For each destination multiple files are necessary. This is because the max
    file length is very small, so only a couple records fit in each file.
    """

    fn = bqfl.WriteRecordsToFile(schema=_ELEMENTS_SCHEMA, max_file_size=50)
    self.tmpdir = self._new_tempdir()

    def check_many_files(output_pcs):
      dest_file_pc = output_pcs[bqfl.WriteRecordsToFile.WRITTEN_FILE_TAG]

      files_per_dest = (
          dest_file_pc
          | beam.Map(lambda x: x).with_output_types(
              beam.typehints.KV[str, Tuple[str, int]])
          | combiners.Count.PerKey())
      files_per_dest = (
          files_per_dest
          | "GetDests" >> beam.Map(
              lambda x: (bigquery_tools.get_hashable_destination(x[0]), x[1])))
      assert_that(
          files_per_dest,
          equal_to([('project1:dataset1.table1', 4),
                    ('project1:dataset1.table2', 2),
                    ('project1:dataset1.table3', 1)]))

      # Check that the files exist
      _ = dest_file_pc | beam.Map(lambda x: x[1][0]) | beam.Map(
          lambda x: hamcrest_assert(os.path.exists(x), is_(True)))

    self._consume_input(fn, check_many_files)

  @parameterized.expand([
      param(file_format=bigquery_tools.FileFormat.AVRO),
      param(file_format=bigquery_tools.FileFormat.JSON),
  ])
  def test_records_are_spilled(self, file_format):
    """Forces records to be written to many files.

    For each destination multiple files are necessary, and at most two files
    can be created. This forces records to be spilled to the next stage of
    processing.
    """

    fn = bqfl.WriteRecordsToFile(
        schema=_ELEMENTS_SCHEMA,
        max_files_per_bundle=2,
        file_format=file_format)
    self.tmpdir = self._new_tempdir()

    def check_many_files(output_pcs):
      dest_file_pc = output_pcs[bqfl.WriteRecordsToFile.WRITTEN_FILE_TAG]
      spilled_records_pc = output_pcs[
          bqfl.WriteRecordsToFile.UNWRITTEN_RECORD_TAG]

      spilled_records_count = (spilled_records_pc | combiners.Count.Globally())
      assert_that(spilled_records_count, equal_to([3]), label='spilled count')

      files_per_dest = (
          dest_file_pc
          | beam.Map(lambda x: x).with_output_types(
              beam.typehints.KV[str, Tuple[str, int]])
          | combiners.Count.PerKey())
      files_per_dest = (
          files_per_dest
          | "GetDests" >> beam.Map(
              lambda x: (bigquery_tools.get_hashable_destination(x[0]), x[1])))

      # Only table1 and table3 get files. table2 records get spilled.
      assert_that(
          files_per_dest,
          equal_to([('project1:dataset1.table1', 1),
                    ('project1:dataset1.table3', 1)]),
          label='file count')

      # Check that the files exist
      _ = dest_file_pc | beam.Map(lambda x: x[1][0]) | beam.Map(
          lambda x: hamcrest_assert(os.path.exists(x), is_(True)))

    self._consume_input(fn, check_many_files)


class TestWriteGroupedRecordsToFile(_TestCaseWithTempDirCleanUp):
  def _consume_input(self, fn, input, checks):
    if checks is None:
      return

    with TestPipeline() as p:
      res = (
          p
          | beam.Create(input)
          | beam.GroupByKey()
          | beam.ParDo(fn, self.tmpdir))

      checks(res)
      return res

  @parameterized.expand([
      param(file_format=bigquery_tools.FileFormat.AVRO),
      param(file_format=bigquery_tools.FileFormat.JSON),
      param(file_format=None),
  ])
  def test_files_are_created(self, file_format):
    """Test that the files are created and written."""

    fn = bqfl.WriteGroupedRecordsToFile(
        schema=_ELEMENTS_SCHEMA, file_format=file_format)
    self.tmpdir = self._new_tempdir()

    def check_files_created(output_pc):
      files = output_pc | "GetFiles" >> beam.Map(lambda x: x[1][0])
      file_count = files | "CountFiles" >> combiners.Count.Globally()

      _ = files | "FilesExist" >> beam.Map(
          lambda x: hamcrest_assert(os.path.exists(x), is_(True)))
      assert_that(file_count, equal_to([3]), label='check file count')

      destinations = (
          output_pc
          | "GetDests" >>
          beam.Map(lambda x: bigquery_tools.get_hashable_destination(x[0])))
      assert_that(
          destinations,
          equal_to(list(_DISTINCT_DESTINATIONS)),
          label='check destinations ')

    self._consume_input(fn, _DESTINATION_ELEMENT_PAIRS, check_files_created)

  def test_multiple_files(self):
    """Forces records to be written to many files.

    For each destination multiple files are necessary. This is because the max
    file length is very small, so only a couple records fit in each file.
    """
    fn = bqfl.WriteGroupedRecordsToFile(
        schema=_ELEMENTS_SCHEMA, max_file_size=50)
    self.tmpdir = self._new_tempdir()

    def check_multiple_files(output_pc):
      files_per_dest = output_pc | combiners.Count.PerKey()
      files_per_dest = (
          files_per_dest
          | "GetDests" >> beam.Map(
              lambda x: (bigquery_tools.get_hashable_destination(x[0]), x[1])))
      assert_that(
          files_per_dest,
          equal_to([
              ('project1:dataset1.table1', 4),
              ('project1:dataset1.table2', 2),
              ('project1:dataset1.table3', 1),
          ]))

      # Check that the files exist
      _ = output_pc | beam.Map(lambda x: x[1][0]) | beam.Map(os.path.exists)

    self._consume_input(fn, _DESTINATION_ELEMENT_PAIRS, check_multiple_files)


class TestPartitionFiles(unittest.TestCase):

  _ELEMENTS = [(
      'destination0', [('file0', 50), ('file1', 50), ('file2', 50),
                       ('file3', 50)]),
               ('destination1', [('file0', 50), ('file1', 50)])]

  def test_partition(self):
    partition = bqfl.PartitionFiles.Partition(1000, 1)
    self.assertEqual(partition.can_accept(50), True)
    self.assertEqual(partition.can_accept(2000), False)
    self.assertEqual(partition.can_accept(1000), True)

    partition.add('file1', 50)
    self.assertEqual(partition.files, ['file1'])
    self.assertEqual(partition.size, 50)
    self.assertEqual(partition.can_accept(50), False)
    self.assertEqual(partition.can_accept(0), False)

  def test_partition_files_dofn_file_split(self):
    """Force partitions to split based on max_files"""
    multiple_partitions_result = [('destination0', ['file0', 'file1']),
                                  ('destination0', ['file2', 'file3'])]
    single_partition_result = [('destination1', ['file0', 'file1'])]
    with TestPipeline() as p:
      destination_file_pairs = p | beam.Create(self._ELEMENTS, reshuffle=False)
      partitioned_files = (
          destination_file_pairs
          | beam.ParDo(bqfl.PartitionFiles(1000, 2)).with_outputs(
              bqfl.PartitionFiles.MULTIPLE_PARTITIONS_TAG,
              bqfl.PartitionFiles.SINGLE_PARTITION_TAG))
      multiple_partitions = partitioned_files[bqfl.PartitionFiles\
                                              .MULTIPLE_PARTITIONS_TAG]
      single_partition = partitioned_files[bqfl.PartitionFiles\
                                           .SINGLE_PARTITION_TAG]

    assert_that(
        multiple_partitions,
        equal_to(multiple_partitions_result),
        label='CheckMultiplePartitions')
    assert_that(
        single_partition,
        equal_to(single_partition_result),
        label='CheckSinglePartition')

  def test_partition_files_dofn_size_split(self):
    """Force partitions to split based on max_partition_size"""
    multiple_partitions_result = [('destination0', ['file0', 'file1', 'file2']),
                                  ('destination0', ['file3'])]
    single_partition_result = [('destination1', ['file0', 'file1'])]
    with TestPipeline() as p:
      destination_file_pairs = p | beam.Create(self._ELEMENTS, reshuffle=False)
      partitioned_files = (
          destination_file_pairs
          | beam.ParDo(bqfl.PartitionFiles(150, 10)).with_outputs(
              bqfl.PartitionFiles.MULTIPLE_PARTITIONS_TAG,
              bqfl.PartitionFiles.SINGLE_PARTITION_TAG))
      multiple_partitions = partitioned_files[bqfl.PartitionFiles\
                                              .MULTIPLE_PARTITIONS_TAG]
      single_partition = partitioned_files[bqfl.PartitionFiles\
                                           .SINGLE_PARTITION_TAG]

    assert_that(
        multiple_partitions,
        equal_to(multiple_partitions_result),
        label='CheckMultiplePartitions')
    assert_that(
        single_partition,
        equal_to(single_partition_result),
        label='CheckSinglePartition')


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
        validate=False,
        temp_file_format=bigquery_tools.FileFormat.JSON)

    # Need to test this with the DirectRunner to avoid serializing mocks
    with TestPipeline('DirectRunner') as p:
      outputs = p | beam.Create(_ELEMENTS) | transform

      dest_files = outputs[bqfl.BigQueryBatchFileLoads.DESTINATION_FILE_PAIRS]
      dest_job = outputs[bqfl.BigQueryBatchFileLoads.DESTINATION_JOBID_PAIRS]

      jobs = dest_job | "GetJobs" >> beam.Map(lambda x: x[1])

      files = dest_files | "GetFiles" >> beam.Map(lambda x: x[1][0])
      destinations = (
          dest_files
          | "GetDests" >> beam.Map(
              lambda x: (bigquery_tools.get_hashable_destination(x[0]), x[1]))
          | "GetUniques" >> combiners.Count.PerKey()
          | "GetFinalDests" >> beam.Keys())

      # All files exist
      _ = (
          files
          | beam.Map(lambda x: hamcrest_assert(os.path.exists(x), is_(True))))

      # One file per destination
      assert_that(
          files | combiners.Count.Globally(), equal_to([1]), label='CountFiles')

      assert_that(
          destinations, equal_to([destination]), label='CheckDestinations')

      assert_that(jobs, equal_to([job_reference]), label='CheckJobs')

  def test_load_job_id_used(self):
    job_reference = bigquery_api.JobReference()
    job_reference.projectId = 'loadJobProject'
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
        'project1:dataset1.table1',
        custom_gcs_temp_location=self._new_tempdir(),
        test_client=bq_client,
        validate=False,
        load_job_project_id='loadJobProject')

    with TestPipeline('DirectRunner') as p:
      outputs = p | beam.Create(_ELEMENTS) | transform
      jobs = outputs[bqfl.BigQueryBatchFileLoads.DESTINATION_JOBID_PAIRS] \
             | "GetJobs" >> beam.Map(lambda x: x[1])

      assert_that(jobs, equal_to([job_reference]), label='CheckJobProjectIds')

  def test_load_job_id_use_for_copy_job(self):
    destination = 'project1:dataset1.table1'

    job_reference = bigquery_api.JobReference()
    job_reference.projectId = 'loadJobProject'
    job_reference.jobId = 'job_name1'
    result_job = mock.Mock()
    result_job.jobReference = job_reference

    mock_job = mock.Mock()
    mock_job.status.state = 'DONE'
    mock_job.status.errorResult = None
    mock_job.jobReference = job_reference

    bq_client = mock.Mock()
    bq_client.jobs.Get.return_value = mock_job

    bq_client.jobs.Insert.return_value = result_job
    bq_client.tables.Delete.return_value = None

    with TestPipeline('DirectRunner') as p:
      outputs = (
          p
          | beam.Create(_ELEMENTS, reshuffle=False)
          | bqfl.BigQueryBatchFileLoads(
              destination,
              custom_gcs_temp_location=self._new_tempdir(),
              test_client=bq_client,
              validate=False,
              temp_file_format=bigquery_tools.FileFormat.JSON,
              max_file_size=45,
              max_partition_size=80,
              max_files_per_partition=2,
              load_job_project_id='loadJobProject'))

      dest_copy_jobs = outputs[
          bqfl.BigQueryBatchFileLoads.DESTINATION_COPY_JOBID_PAIRS]

      copy_jobs = dest_copy_jobs | "GetCopyJobs" >> beam.Map(lambda x: x[1])

      assert_that(
          copy_jobs,
          equal_to([
              job_reference,
              job_reference,
              job_reference,
              job_reference,
              job_reference,
              job_reference
          ]),
          label='CheckCopyJobProjectIds')

  @mock.patch('time.sleep')
  def test_wait_for_load_job_completion(self, sleep_mock):
    job_1 = bigquery_api.Job()
    job_1.jobReference = bigquery_api.JobReference()
    job_1.jobReference.projectId = 'project1'
    job_1.jobReference.jobId = 'jobId1'
    job_2 = bigquery_api.Job()
    job_2.jobReference = bigquery_api.JobReference()
    job_2.jobReference.projectId = 'project1'
    job_2.jobReference.jobId = 'jobId2'

    job_1_waiting = mock.Mock()
    job_1_waiting.status.state = 'RUNNING'
    job_2_done = mock.Mock()
    job_2_done.status.state = 'DONE'
    job_2_done.status.errorResult = None

    job_1_done = mock.Mock()
    job_1_done.status.state = 'DONE'
    job_1_done.status.errorResult = None

    bq_client = mock.Mock()
    bq_client.jobs.Get.side_effect = [
        job_1_waiting, job_2_done, job_1_done, job_2_done
    ]
    partition_1 = ('project:dataset.table0', ['file0'])
    partition_2 = ('project:dataset.table1', ['file1'])
    bq_client.jobs.Insert.side_effect = [job_1, job_2]
    test_job_prefix = "test_job"

    expected_dest_jobref_list = [(partition_1[0], job_1.jobReference),
                                 (partition_2[0], job_2.jobReference)]
    with TestPipeline('DirectRunner') as p:
      partitions = p | beam.Create([partition_1, partition_2])
      outputs = (
          partitions
          | beam.ParDo(
              bqfl.TriggerLoadJobs(test_client=bq_client), test_job_prefix))

      assert_that(outputs, equal_to(expected_dest_jobref_list))

    sleep_mock.assert_called_once()

  @mock.patch('time.sleep')
  def test_one_load_job_failed_after_waiting(self, sleep_mock):
    job_1 = bigquery_api.Job()
    job_1.jobReference = bigquery_api.JobReference()
    job_1.jobReference.projectId = 'project1'
    job_1.jobReference.jobId = 'jobId1'
    job_2 = bigquery_api.Job()
    job_2.jobReference = bigquery_api.JobReference()
    job_2.jobReference.projectId = 'project1'
    job_2.jobReference.jobId = 'jobId2'

    job_1_waiting = mock.Mock()
    job_1_waiting.status.state = 'RUNNING'
    job_2_done = mock.Mock()
    job_2_done.status.state = 'DONE'
    job_2_done.status.errorResult = None

    job_1_error = mock.Mock()
    job_1_error.status.state = 'DONE'
    job_1_error.status.errorResult = 'Some problems happened'

    bq_client = mock.Mock()
    bq_client.jobs.Get.side_effect = [
        job_1_waiting, job_2_done, job_1_error, job_2_done
    ]
    partition_1 = ('project:dataset.table0', ['file0'])
    partition_2 = ('project:dataset.table1', ['file1'])
    bq_client.jobs.Insert.side_effect = [job_1, job_2]
    test_job_prefix = "test_job"

    with self.assertRaises(Exception):
      with TestPipeline('DirectRunner') as p:
        partitions = p | beam.Create([partition_1, partition_2])
        _ = (
            partitions
            | beam.ParDo(
                bqfl.TriggerLoadJobs(test_client=bq_client), test_job_prefix))

    sleep_mock.assert_called_once()

  def test_multiple_partition_files(self):
    destination = 'project1:dataset1.table1'

    job_reference = bigquery_api.JobReference()
    job_reference.projectId = 'project1'
    job_reference.jobId = 'job_name1'
    result_job = mock.Mock()
    result_job.jobReference = job_reference

    mock_job = mock.Mock()
    mock_job.status.state = 'DONE'
    mock_job.status.errorResult = None
    mock_job.jobReference = job_reference

    bq_client = mock.Mock()
    bq_client.jobs.Get.return_value = mock_job

    bq_client.jobs.Insert.return_value = result_job
    bq_client.tables.Delete.return_value = None

    with TestPipeline('DirectRunner') as p:
      outputs = (
          p
          | beam.Create(_ELEMENTS, reshuffle=False)
          | bqfl.BigQueryBatchFileLoads(
              destination,
              custom_gcs_temp_location=self._new_tempdir(),
              test_client=bq_client,
              validate=False,
              temp_file_format=bigquery_tools.FileFormat.JSON,
              max_file_size=45,
              max_partition_size=80,
              max_files_per_partition=2))

      dest_files = outputs[bqfl.BigQueryBatchFileLoads.DESTINATION_FILE_PAIRS]
      dest_load_jobs = outputs[
          bqfl.BigQueryBatchFileLoads.DESTINATION_JOBID_PAIRS]
      dest_copy_jobs = outputs[
          bqfl.BigQueryBatchFileLoads.DESTINATION_COPY_JOBID_PAIRS]

      load_jobs = dest_load_jobs | "GetLoadJobs" >> beam.Map(lambda x: x[1])
      copy_jobs = dest_copy_jobs | "GetCopyJobs" >> beam.Map(lambda x: x[1])

      files = dest_files | "GetFiles" >> beam.Map(lambda x: x[1][0])
      destinations = (
          dest_files
          | "GetDests" >> beam.Map(
              lambda x: (bigquery_tools.get_hashable_destination(x[0]), x[1]))
          | "GetUniques" >> combiners.Count.PerKey()
          | "GetFinalDests" >> beam.Keys())

      # All files exist
      _ = (
          files
          | beam.Map(lambda x: hamcrest_assert(os.path.exists(x), is_(True))))

      # One file per destination
      assert_that(
          files | "CountFiles" >> combiners.Count.Globally(),
          equal_to([6]),
          label='CheckFileCount')

      assert_that(
          destinations, equal_to([destination]), label='CheckDestinations')

      assert_that(
          load_jobs | "CountLoadJobs" >> combiners.Count.Globally(),
          equal_to([6]),
          label='CheckLoadJobCount')
      assert_that(
          copy_jobs | "CountCopyJobs" >> combiners.Count.Globally(),
          equal_to([6]),
          label='CheckCopyJobCount')

  @parameterized.expand([
      param(is_streaming=False, with_auto_sharding=False),
      param(is_streaming=True, with_auto_sharding=False),
      param(is_streaming=True, with_auto_sharding=True),
  ])
  def test_triggering_frequency(self, is_streaming, with_auto_sharding):
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

    # Insert a fake clock to work with auto-sharding which needs a processing
    # time timer.
    class _FakeClock(object):
      def __init__(self, now=time.time()):
        self._now = now

      def __call__(self):
        return self._now

    start_time = timestamp.Timestamp(0)
    bq_client.test_clock = _FakeClock(now=start_time)

    triggering_frequency = 20 if is_streaming else None
    transform = bqfl.BigQueryBatchFileLoads(
        destination,
        custom_gcs_temp_location=self._new_tempdir(),
        test_client=bq_client,
        validate=False,
        temp_file_format=bigquery_tools.FileFormat.JSON,
        is_streaming_pipeline=is_streaming,
        triggering_frequency=triggering_frequency,
        with_auto_sharding=with_auto_sharding)

    # Need to test this with the DirectRunner to avoid serializing mocks
    test_options = PipelineOptions(flags=['--allow_unsafe_triggers'])
    test_options.view_as(StandardOptions).streaming = is_streaming
    with TestPipeline(runner='BundleBasedDirectRunner',
                      options=test_options) as p:
      if is_streaming:
        _SIZE = len(_ELEMENTS)
        fisrt_batch = [
            TimestampedValue(value, start_time + i + 1) for i,
            value in enumerate(_ELEMENTS[:_SIZE // 2])
        ]
        second_batch = [
            TimestampedValue(value, start_time + _SIZE // 2 + i + 1) for i,
            value in enumerate(_ELEMENTS[_SIZE // 2:])
        ]
        # Advance processing time between batches of input elements to fire the
        # user triggers. Intentionally advance the processing time twice for the
        # auto-sharding case since we need to first fire the timer and then
        # fire the trigger.
        test_stream = (
            TestStream().advance_watermark_to(start_time).add_elements(
                fisrt_batch).advance_processing_time(
                    30).advance_processing_time(30).add_elements(second_batch).
            advance_processing_time(30).advance_processing_time(
                30).advance_watermark_to_infinity())
        input = p | test_stream
      else:
        input = p | beam.Create(_ELEMENTS)
      outputs = input | transform

      dest_files = outputs[bqfl.BigQueryBatchFileLoads.DESTINATION_FILE_PAIRS]
      dest_job = outputs[bqfl.BigQueryBatchFileLoads.DESTINATION_JOBID_PAIRS]

      files = dest_files | "GetFiles" >> beam.Map(lambda x: x[1][0])
      destinations = (
          dest_files
          | "GetDests" >> beam.Map(
              lambda x: (bigquery_tools.get_hashable_destination(x[0]), x[1]))
          | "GetUniques" >> combiners.Count.PerKey()
          | "GetFinalDests" >> beam.Keys())
      jobs = dest_job | "GetJobs" >> beam.Map(lambda x: x[1])

      # Check that all files exist.
      _ = (
          files
          | beam.Map(lambda x: hamcrest_assert(os.path.exists(x), is_(True))))

      # Expect two load jobs are generated in the streaming case due to the
      # triggering frequency. Grouping is per trigger so we expect two entries
      # in the output as opposed to one.
      file_count = files | combiners.Count.Globally().without_defaults()
      expected_file_count = [1, 1] if is_streaming else [1]
      expected_destinations = [destination, destination
                               ] if is_streaming else [destination]
      expected_jobs = [job_reference, job_reference
                       ] if is_streaming else [job_reference]
      assert_that(file_count, equal_to(expected_file_count), label='CountFiles')
      assert_that(
          destinations,
          equal_to(expected_destinations),
          label='CheckDestinations')
      assert_that(jobs, equal_to(expected_jobs), label='CheckJobs')


class BigQueryFileLoadsIT(unittest.TestCase):

  BIG_QUERY_DATASET_ID = 'python_bq_file_loads_'
  BIG_QUERY_SCHEMA = (
      '{"fields": [{"name": "name","type": "STRING"},'
      '{"name": "language","type": "STRING"}]}')

  BIG_QUERY_SCHEMA_2 = (
      '{"fields": [{"name": "name","type": "STRING"},'
      '{"name": "foundation","type": "STRING"}]}')

  BIG_QUERY_STREAMING_SCHEMA = ({
      'fields': [{
          'name': 'Integr', 'type': 'INTEGER', 'mode': 'NULLABLE'
      }]
  })

  def setUp(self):
    self.test_pipeline = TestPipeline(is_integration_test=True)
    self.runner_name = type(self.test_pipeline.runner).__name__
    self.project = self.test_pipeline.get_option('project')

    self.dataset_id = '%s%s%d' % (
        self.BIG_QUERY_DATASET_ID,
        str(int(time.time())),
        random.randint(0, 10000))
    self.bigquery_client = bigquery_tools.BigQueryWrapper()
    self.bigquery_client.get_or_create_dataset(self.project, self.dataset_id)
    self.output_table = "%s.output_table" % (self.dataset_id)
    _LOGGER.info(
        "Created dataset %s in project %s", self.dataset_id, self.project)

  @pytest.mark.it_postcommit
  def test_multiple_destinations_transform(self):
    output_table_1 = '%s%s' % (self.output_table, 1)
    output_table_2 = '%s%s' % (self.output_table, 2)
    output_table_3 = '%s%s' % (self.output_table, 3)
    output_table_4 = '%s%s' % (self.output_table, 4)
    schema1 = bigquery.WriteToBigQuery.get_dict_table_schema(
        bigquery_tools.parse_table_schema_from_json(self.BIG_QUERY_SCHEMA))
    schema2 = bigquery.WriteToBigQuery.get_dict_table_schema(
        bigquery_tools.parse_table_schema_from_json(self.BIG_QUERY_SCHEMA_2))

    schema_kv_pairs = [(output_table_1, schema1), (output_table_2, schema2),
                       (output_table_3, schema1), (output_table_4, schema2)]
    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT name, language FROM %s" % output_table_1,
            data=[(d['name'], d['language']) for d in _ELEMENTS
                  if 'language' in d]),
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT name, foundation FROM %s" % output_table_2,
            data=[(d['name'], d['foundation']) for d in _ELEMENTS
                  if 'foundation' in d]),
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT name, language FROM %s" % output_table_3,
            data=[(d['name'], d['language']) for d in _ELEMENTS
                  if 'language' in d]),
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT name, foundation FROM %s" % output_table_4,
            data=[(d['name'], d['foundation']) for d in _ELEMENTS
                  if 'foundation' in d])
    ]

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=all_of(*pipeline_verifiers))

    with beam.Pipeline(argv=args) as p:
      input = p | beam.Create(_ELEMENTS, reshuffle=False)

      schema_map_pcv = beam.pvalue.AsDict(
          p | "MakeSchemas" >> beam.Create(schema_kv_pairs))

      table_record_pcv = beam.pvalue.AsDict(
          p | "MakeTables" >> beam.Create([('table1', output_table_1),
                                           ('table2', output_table_2)]))

      # Get all input in same machine
      input = (
          input
          | beam.Map(lambda x: (None, x))
          | beam.GroupByKey()
          | beam.FlatMap(lambda elm: elm[1]))

      _ = (
          input | "WriteWithMultipleDestsFreely" >> bigquery.WriteToBigQuery(
              table=lambda x,
              tables:
              (tables['table1'] if 'language' in x else tables['table2']),
              table_side_inputs=(table_record_pcv, ),
              schema=lambda dest,
              schema_map: schema_map.get(dest, None),
              schema_side_inputs=(schema_map_pcv, ),
              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
              write_disposition=beam.io.BigQueryDisposition.WRITE_EMPTY))

      _ = (
          input | "WriteWithMultipleDests" >> bigquery.WriteToBigQuery(
              table=lambda x:
              (output_table_3 if 'language' in x else output_table_4),
              schema=lambda dest,
              schema_map: schema_map.get(dest, None),
              schema_side_inputs=(schema_map_pcv, ),
              create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
              write_disposition=beam.io.BigQueryDisposition.WRITE_EMPTY,
              max_file_size=20,
              max_files_per_bundle=-1))

  @pytest.mark.it_postcommit
  def test_bqfl_streaming(self):
    if isinstance(self.test_pipeline.runner, TestDataflowRunner):
      self.skipTest("TestStream is not supported on TestDataflowRunner")
    output_table = '%s_%s' % (self.output_table, 'ints')
    _SIZE = 100
    schema = self.BIG_QUERY_STREAMING_SCHEMA
    l = [{'Integr': i} for i in range(_SIZE)]

    bq_matcher = BigqueryFullResultStreamingMatcher(
        project=self.project,
        query="SELECT Integr FROM %s" % output_table,
        data=[(i, ) for i in range(100)])

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=bq_matcher,
        streaming=True,
        allow_unsafe_triggers=True)
    with beam.Pipeline(argv=args) as p:
      stream_source = (
          TestStream().advance_watermark_to(0).advance_processing_time(
              100).add_elements(l[:_SIZE // 4]).
          advance_processing_time(100).advance_watermark_to(100).add_elements(
              l[_SIZE // 4:2 * _SIZE // 4]).advance_processing_time(
                  100).advance_watermark_to(200).add_elements(
                      l[2 * _SIZE // 4:3 * _SIZE // 4]).advance_processing_time(
                          100).advance_watermark_to(300).add_elements(
                              l[3 * _SIZE // 4:]).advance_processing_time(
                                  100).advance_watermark_to_infinity())
      _ = (p
           | stream_source
           | bigquery.WriteToBigQuery(output_table,
                                      schema=schema,
                                      method=bigquery.WriteToBigQuery \
                                        .Method.FILE_LOADS,
                                      triggering_frequency=100))

    hamcrest_assert(p, bq_matcher)

  @pytest.mark.it_postcommit
  def test_bqfl_streaming_with_copy_jobs(self):
    if isinstance(self.test_pipeline.runner, TestDataflowRunner):
      self.skipTest("TestStream is not supported on TestDataflowRunner")
    output_table = '%s_%s' % (self.output_table, 'with_copy_jobs')
    _SIZE = 100
    schema = self.BIG_QUERY_STREAMING_SCHEMA
    l = [{'Integr': i} for i in range(_SIZE)]

    bq_matcher = BigqueryFullResultStreamingMatcher(
        project=self.project,
        query="SELECT Integr FROM %s" % output_table,
        data=[(i, ) for i in range(100)])

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=bq_matcher,
        streaming=True,
        allow_unsafe_triggers=True)

    # Override these parameters to induce copy jobs
    bqfl._DEFAULT_MAX_FILE_SIZE = 100
    bqfl._MAXIMUM_LOAD_SIZE = 400

    with beam.Pipeline(argv=args) as p:
      stream_source = (
          TestStream().advance_watermark_to(0).advance_processing_time(
              100).add_elements(l[:_SIZE // 4]).
          advance_processing_time(100).advance_watermark_to(100).add_elements(
              l[_SIZE // 4:2 * _SIZE // 4]).advance_processing_time(
                  100).advance_watermark_to(200).add_elements(
                      l[2 * _SIZE // 4:3 * _SIZE // 4]).advance_processing_time(
                          100).advance_watermark_to(300).add_elements(
                              l[3 * _SIZE // 4:]).advance_processing_time(100).
          advance_watermark_to_infinity().advance_processing_time(100))

      _ = (p
           | stream_source
           | bigquery.WriteToBigQuery(output_table,
                                      schema=schema,
                                      method=bigquery.WriteToBigQuery \
                                      .Method.FILE_LOADS,
                                      triggering_frequency=100))

    hamcrest_assert(p, bq_matcher)

  @pytest.mark.it_postcommit
  def test_bqfl_streaming_with_dynamic_destinations(self):
    if isinstance(self.test_pipeline.runner, TestDataflowRunner):
      self.skipTest("TestStream is not supported on TestDataflowRunner")
    even_table = '%s_%s' % (self.output_table, "dynamic_dest_0")
    odd_table = '%s_%s' % (self.output_table, "dynamic_dest_1")
    output_table = lambda row: even_table if (
        row['Integr'] % 2 == 0) else odd_table
    _SIZE = 100
    schema = self.BIG_QUERY_STREAMING_SCHEMA
    l = [{'Integr': i} for i in range(_SIZE)]

    pipeline_verifiers = [
        BigqueryFullResultStreamingMatcher(
            project=self.project,
            query="SELECT Integr FROM %s" % even_table,
            data=[(i, ) for i in range(0, 100, 2)]),
        BigqueryFullResultStreamingMatcher(
            project=self.project,
            query="SELECT Integr FROM %s" % odd_table,
            data=[(i, ) for i in range(1, 100, 2)])
    ]

    args = self.test_pipeline.get_full_options_as_args(
        on_success_matcher=all_of(*pipeline_verifiers),
        streaming=True,
        allow_unsafe_triggers=True)

    with beam.Pipeline(argv=args) as p:
      stream_source = (
          TestStream().advance_watermark_to(0).advance_processing_time(
              100).add_elements(l[:_SIZE // 4]).
          advance_processing_time(100).advance_watermark_to(100).add_elements(
              l[_SIZE // 4:2 * _SIZE // 4]).advance_processing_time(
                  100).advance_watermark_to(200).add_elements(
                      l[2 * _SIZE // 4:3 * _SIZE // 4]).advance_processing_time(
                          100).advance_watermark_to(300).add_elements(
                              l[3 * _SIZE // 4:]).advance_processing_time(100).
          advance_watermark_to_infinity().advance_processing_time(100))

      _ = (p
           | stream_source
           | bigquery.WriteToBigQuery(output_table,
                                      schema=schema,
                                      method=bigquery.WriteToBigQuery \
                                      .Method.FILE_LOADS,
                                      triggering_frequency=100))
    hamcrest_assert(p, all_of(*pipeline_verifiers))

  @pytest.mark.it_postcommit
  def test_one_job_fails_all_jobs_fail(self):

    # If one of the import jobs fails, then other jobs must not be performed.
    # This is to avoid reinsertion of some records when a pipeline fails and
    # is rerun.
    output_table_1 = '%s%s' % (self.output_table, 1)
    output_table_2 = '%s%s' % (self.output_table, 2)

    self.bigquery_client.get_or_create_table(
        self.project,
        self.dataset_id,
        output_table_1.split('.')[1],
        bigquery_tools.parse_table_schema_from_json(self.BIG_QUERY_SCHEMA),
        None,
        None)
    self.bigquery_client.get_or_create_table(
        self.project,
        self.dataset_id,
        output_table_2.split('.')[1],
        bigquery_tools.parse_table_schema_from_json(self.BIG_QUERY_SCHEMA_2),
        None,
        None)

    pipeline_verifiers = [
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT name, language FROM %s" % output_table_1,
            data=[]),
        BigqueryFullResultMatcher(
            project=self.project,
            query="SELECT name, foundation FROM %s" % output_table_2,
            data=[])
    ]

    args = self.test_pipeline.get_full_options_as_args()

    with self.assertRaises(Exception):
      # The pipeline below fails because neither a schema nor SCHEMA_AUTODETECT
      # are specified.
      with beam.Pipeline(argv=args) as p:
        input = p | beam.Create(_ELEMENTS)
        input2 = p | "Broken record" >> beam.Create(['language_broken_record'])

        input = (input, input2) | beam.Flatten()

        _ = (
            input | "WriteWithMultipleDests" >> bigquery.WriteToBigQuery(
                table=lambda x:
                (output_table_1 if 'language' in x else output_table_2),
                create_disposition=(
                    beam.io.BigQueryDisposition.CREATE_IF_NEEDED),
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                temp_file_format=bigquery_tools.FileFormat.JSON))

    hamcrest_assert(p, all_of(*pipeline_verifiers))

  def tearDown(self):
    request = bigquery_api.BigqueryDatasetsDeleteRequest(
        projectId=self.project, datasetId=self.dataset_id, deleteContents=True)
    try:
      _LOGGER.info(
          "Deleting dataset %s in project %s", self.dataset_id, self.project)
      self.bigquery_client.client.datasets.Delete(request)
    except HttpError:
      _LOGGER.debug(
          'Failed to clean up dataset %s in project %s',
          self.dataset_id,
          self.project)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
