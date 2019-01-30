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

"""
Functionality to perform file loads into BigQuery for Batch and Streaming
pipelines.

NOTHING IN THIS FILE HAS BACKWARDS COMPATIBILITY GUARANTEES.
"""

from __future__ import absolute_import

import datetime
import logging
import random
import time
import uuid

from future.utils import iteritems

import apache_beam as beam
from apache_beam import pvalue
from apache_beam.io import filesystems as fs
from apache_beam.io.gcp import bigquery_tools
from apache_beam.io.gcp.internal.clients import bigquery as bigquery_api
from apache_beam.options import value_provider as vp

ONE_TERABYTE = (1 << 40)

# The maximum file size for imports is 5TB. We keep our files under that.
_DEFAULT_MAX_FILE_SIZE = 4 * ONE_TERABYTE

_DEFAULT_MAX_WRITERS_PER_BUNDLE = 20

# The maximum size for a single load job is one terabyte
_MAXIMUM_LOAD_SIZE = 15 * ONE_TERABYTE


def _generate_load_job_name():
  datetime_component = datetime.datetime.now().strftime("%Y_%m_%d_%H%M%S")
  # TODO(pabloem): include job id / pipeline component?
  return 'beam_load_%s_%s' % (datetime_component, random.randint(0, 100))


def _generate_file_prefix(pipeline_gcs_location):
  # If a gcs location is provided to the pipeline, then we shall use that.
  # Otherwise, we shall use the temp_location from pipeline options.
  gcs_base = str(pipeline_gcs_location or
                 vp.RuntimeValueProvider.get_value('temp_location', str, ''))
  return fs.FileSystems.join(gcs_base, 'bq_load')


def _make_new_file_writer(file_prefix, destination):
  if isinstance(destination, bigquery_api.TableReference):
    destination = '%s:%s.%s' % (
        destination.projectId, destination.datasetId, destination.tableId)

  directory = fs.FileSystems.join(file_prefix, destination)

  if not fs.FileSystems.exists(directory):
    fs.FileSystems.mkdirs(directory)

  file_name = str(uuid.uuid4())
  file_path = fs.FileSystems.join(file_prefix, destination, file_name)

  return file_path, fs.FileSystems.create(file_path, 'application/text')


def _bq_uuid():
  return str(uuid.uuid4()).replace("-", "")


class _AppendDestinationsFn(beam.DoFn):
  """Adds the destination to an element, making it a KV pair.

  Outputs a PCollection of KV-pairs where the key is a TableReference for the
  destination, and the value is the record itself.

  Experimental; no backwards compatibility guarantees.
  """

  def __init__(self, destination):
    if callable(destination):
      self.destination = destination
    else:
      self.destination = lambda x: destination

  def process(self, element):
    logging.info((self.destination(element), element))
    yield (self.destination(element), element)


class _ShardDestinations(beam.DoFn):
  """Adds a shard number to the key of the KV element.

  Experimental; no backwards compatibility guarantees."""
  DEFAULT_SHARDING_FACTOR = 10

  def __init__(self, sharding_factor=DEFAULT_SHARDING_FACTOR):
    self.sharding_factor = sharding_factor

  def start_bundle(self):
    self._shard_count = random.randrange(self.sharding_factor)

  def process(self, element):
    destination = element[0]
    row = element[1]

    sharded_destination = (destination,
                           self._shard_count % self.sharding_factor)
    self._shard_count += 1
    yield (sharded_destination, row)


class WriteRecordsToFile(beam.DoFn):
  """Write input records to files before triggering a load job.

  It outputs two PCollections.
  """

  UNWRITTEN_RECORD_TAG = 'UnwrittenRecords'
  WRITTEN_FILE_TAG = 'WrittenFiles'

  def __init__(self,
               max_files_per_bundle=_DEFAULT_MAX_WRITERS_PER_BUNDLE,
               max_file_size=_DEFAULT_MAX_FILE_SIZE,
               coder=None):
    """Initialize a :class:`WriteRecordsToFile`.

    Args:
      max_files_per_bundle (int): The maximum number of files that can be kept
        open during execution of this step in a worker. This is to avoid over-
        whelming the worker memory.
      max_file_size (int): The maximum size in bytes for a file to be used in
        an export job.

    """
    self.max_files_per_bundle = max_files_per_bundle
    self.max_file_size = max_file_size
    self.coder = coder or bigquery_tools.RowAsDictJsonCoder()

  def display_data(self):
    return {
        'max_files_per_bundle': self.max_files_per_bundle,
        'max_file_size': str(self.max_file_size),
        'coder': self.coder.__class__.__name__
    }

  def start_bundle(self):
    self._destination_to_file_writer = {}

  def process(self, element, file_prefix):
    destination = element[0]
    row = element[1]

    if destination in self._destination_to_file_writer:
      writer = self._destination_to_file_writer[destination]
    elif len(self._destination_to_file_writer) < self.max_files_per_bundle:
      (file_path, writer) = _make_new_file_writer(file_prefix, destination)
      self._destination_to_file_writer[destination] = writer
      yield pvalue.TaggedOutput(WriteRecordsToFile.WRITTEN_FILE_TAG,
                                (destination, file_path))
    else:
      yield pvalue.TaggedOutput(
          WriteRecordsToFile.UNWRITTEN_RECORD_TAG, element)
      return

    # TODO(pabloem): Is it possible for this to throw exception?
    writer.write(self.coder.encode(row))
    writer.write(b'\n')

    if writer.tell() > self.max_file_size:
      writer.close()
      self._destination_to_file_writer.pop(destination)

  def finish_bundle(self):
    # TODO(pabloem): Maybe output to WRITTEN_FILE_TAG here instead of the
    #  begining? - it would permit to add file size.
    for _, writer in iteritems(self._destination_to_file_writer):
      writer.close()
    self._destination_to_file_writer = {}


class WriteGroupedRecordsToFile(beam.DoFn):
  """Receives collection of dest-iterable(records), writes it to files.

  Experimental; no backwards compatibility guarantees.
  """

  def __init__(self, max_file_size=_DEFAULT_MAX_FILE_SIZE,
               coder=None):
    self.max_file_size = max_file_size
    self.coder = coder or bigquery_tools.RowAsDictJsonCoder()

  def process(self, element, file_prefix):
    destination = element[0]
    rows = element[1]

    writer = None

    for row in rows:
      if writer is None:
        (file_path, writer) = _make_new_file_writer(file_prefix, destination)
        yield (destination, file_path)

      writer.write(self.coder.encode(row))
      writer.write(b'\n')

      if writer.tell() > self.max_file_size:
        writer.close()
        writer = None


class TriggerCopyJobs(beam.DoFn):
  def __init__(self,
               create_disposition=None,
               write_disposition=None,
               test_client=None,
               temporary_tables=False):
    self.create_disposition = create_disposition
    self.write_disposition = write_disposition
    self.test_client = test_client
    self.temporary_tables = temporary_tables

  def start_bundle(self):
    self.bq_wrapper = bigquery_tools.BigQueryWrapper(client=self.test_client)

  def process(self, element, load_job_name_prefix=None):
    destination = element[0]
    job_reference = element[1]

    if not self.temporary_tables:
      # If we did not use temporary tables, then we do not need to trigger any
      # copy jobs.
      return

    copy_job_name = '%s_copy_%s' % (load_job_name_prefix, _bq_uuid())

    copy_to_reference = bigquery_tools.parse_table_reference(destination)
    if copy_to_reference.projectId is None:
      copy_to_reference.projectId = vp.RuntimeValueProvider.get_value(
          'project', str, '')

    copy_from_reference = bigquery_tools.parse_table_reference(destination)
    copy_from_reference.tableId = job_reference.jobId
    if copy_from_reference.projectId is None:
      copy_from_reference.projectId = vp.RuntimeValueProvider.get_value(
          'project', str, '')

    logging.info("Must trigger copy job from %s to %s",
                 copy_from_reference, copy_to_reference)
    job_reference = self.bq_wrapper._insert_copy_job(
        copy_to_reference.projectId,
        copy_job_name,
        copy_from_reference,
        copy_to_reference,
        create_disposition=self.create_disposition,
        write_disposition=self.write_disposition)

    yield (destination, job_reference)


class TriggerLoadJobs(beam.DoFn):
  """Triggers the import jobs to BQ.

  Experimental; no backwards compatibility guarantees.
  """
  def __init__(self,
               schema=None,
               create_disposition=None,
               write_disposition=None,
               test_client=None,
               temporary_tables=False):
    self.schema = schema
    self.test_client = test_client
    self.temporary_tables = temporary_tables
    if self.temporary_tables:
      # If we are loading into temporary tables, we rely on the default create
      # and write dispositions, which mean that a new table will be created.
      self.create_disposition = None
      self.write_disposition = None
    else:
      self.create_disposition = create_disposition
      self.write_disposition = write_disposition

  def display_data(self):
    result = {'create_disposition': str(self.create_disposition),
              'write_disposition': str(self.write_disposition)}
    if self.schema is not None:
      result['schema'] = str(self.schema)
    else:
      result['schema'] = 'AUTODETECT'

    return result

  def start_bundle(self):
    self.bq_wrapper = bigquery_tools.BigQueryWrapper(client=self.test_client)

  def process(self, element, load_job_name_prefix):
    destination = element[0]
    files = element[1]
    job_name = '%s_%s' % (load_job_name_prefix, _bq_uuid())

    table_reference = bigquery_tools.parse_table_reference(destination)
    if table_reference.projectId is None:
      table_reference.projectId = vp.RuntimeValueProvider.get_value(
          'project', str, '')

    if self.temporary_tables:
      # For temporary tables, we create a new table with the name with JobId.
      table_reference.tableId = job_name
      #TODO(pabloem): How to ensure that tables are removed?

    job_reference = self.bq_wrapper.perform_load_job(
        table_reference, list(files), job_name,
        schema=self.schema,
        write_disposition=self.write_disposition,
        create_disposition=self.create_disposition)
    logging.info("Triggered job %s to load data to BigQuery table %s.",
                 job_reference, table_reference)
    yield (destination, job_reference)


class WaitForBQJobs(beam.DoFn):
  """Takes in a series of BQ job names as side input, and waits for all of them.

  If any job fails, it will fail. If all jobs succeed, it will succeed.

  Experimental; no backwards compatibility guarantees.
  """
  ALL_DONE = object()
  FAILED = object()
  WAITING = object()

  def __init__(self, test_client):
    self.test_client = test_client

  def start_bundle(self):
    self.bq_wrapper = bigquery_tools.BigQueryWrapper(client=self.test_client)

  def process(self, element, dest_ids_list):
    job_references = [elm[1] for elm in dest_ids_list]

    while True:
      status = self._check_job_states(job_references)
      if status == WaitForBQJobs.FAILED:
        raise Exception(
            'BigQuery jobs failed. BQ error: %s', self._latest_error)
      elif status == WaitForBQJobs.ALL_DONE:
        return dest_ids_list  # Pass the list of destination-jobs downstream
      time.sleep(10)

  def _check_job_states(self, job_references):
    for ref in job_references:
      job = self.bq_wrapper.get_job(ref.projectId,
                                    ref.jobId,
                                    ref.location)

      logging.info("Job status: %s", job.status)
      if job.status.state == 'DONE' and job.status.errorResult:
        logging.warn("Job %s seems to have failed. Error Result: %s",
                     ref.jobId, job.status.errorResult)
        self._latest_error = job.status
        return WaitForBQJobs.FAILED
      elif job.status.state == 'DONE':
        continue

    return WaitForBQJobs.ALL_DONE


class BigQueryBatchFileLoads(beam.PTransform):
  """Takes in a set of elements, and inserts them to BigQuery via batch loads.

  """

  DESTINATION_JOBID_PAIRS = 'destination_load_jobid_pairs'
  DESTINATION_FILE_PAIRS = 'destination_file_pairs'
  DESTINATION_COPY_JOBID_PAIRS = 'destination_copy_jobid_pairs'

  def __init__(
      self,
      destination,
      schema=None,
      gs_location=None,
      create_disposition=None,
      write_disposition=None,
      coder=None,
      max_file_size=_DEFAULT_MAX_FILE_SIZE,
      max_files_per_bundle=_DEFAULT_MAX_WRITERS_PER_BUNDLE,
      test_client=None):
    self.destination = destination
    self.create_disposition = create_disposition
    self.write_disposition = write_disposition
    self.max_file_size = max_file_size
    self.max_files_per_bundle = max_files_per_bundle
    self._input_gs_location = gs_location
    self.test_client = test_client
    self.schema = schema
    self.coder = coder or bigquery_tools.RowAsDictJsonCoder()

    # If we have multiple destinations, then we will have multiple load jobs,
    # thus we will need temporary tables for atomicity.
    # If the destination is a single one, we assume that we will have only one
    # job to run - and thus we avoid using temporary tables
    self.temp_tables = True if callable(destination) else False

  def expand(self, pcoll):
    p = pcoll.pipeline

    load_job_name_pcv = pvalue.AsSingleton(
        p
        | "ImpulseJobName" >> beam.Create([None])
        | beam.Map(lambda _: _generate_load_job_name()))

    file_prefix_pcv = pvalue.AsSingleton(
        p
        | "CreateFilePrefixView" >> beam.Create([self._input_gs_location])
        | "GenerateFilePrefix" >> beam.Map(_generate_file_prefix))

    outputs = (
        pcoll
        | "AppendDestination" >> beam.ParDo(_AppendDestinationsFn(
            self.destination))
        | beam.ParDo(
            WriteRecordsToFile(max_files_per_bundle=self.max_files_per_bundle,
                               max_file_size=self.max_file_size,
                               coder=self.coder),
            file_prefix=file_prefix_pcv).with_outputs(
                WriteRecordsToFile.UNWRITTEN_RECORD_TAG,
                WriteRecordsToFile.WRITTEN_FILE_TAG))

    destination_files_kv_pc = outputs[WriteRecordsToFile.WRITTEN_FILE_TAG]

    unwritten_records_pc = outputs[WriteRecordsToFile.UNWRITTEN_RECORD_TAG]

    more_destination_files_kv_pc = (
        unwritten_records_pc
        | beam.ParDo(_ShardDestinations())
        | "GroupShardedRows" >> beam.GroupByKey()
        | "DropShardNumber" >> beam.Map(lambda x: (x[0][0], x[1]))
        | "WriteGroupedRecordsToFile" >> beam.ParDo(WriteGroupedRecordsToFile(
            coder=self.coder))
    )

    all_destination_file_pairs_pc = (
        (destination_files_kv_pc, more_destination_files_kv_pc)
        | "DestinationFilesUnion" >> beam.Flatten())

    grouped_files_pc = (
        all_destination_file_pairs_pc
        | "GroupFilesByTableDestinations" >> beam.GroupByKey())

    # Load Jobs are triggered to temporary tables, and those are later copied to
    # the actual appropriate destination query. This ensures atomicity when only
    # some of the load jobs would fail but not other.
    # If any of them fails, then copy jobs are not triggered.
    destination_job_ids_pc = (
        grouped_files_pc | beam.ParDo(TriggerLoadJobs(
            schema=self.schema,
            write_disposition=self.write_disposition,
            create_disposition=self.create_disposition,
            test_client=self.test_client,
            temporary_tables=self.temp_tables), load_job_name_pcv)
    )

    destination_copy_job_ids_pc = (
        p
        | "ImpulseMonitorLoadJobs" >> beam.Create([None])
        | "WaitForLoadJobs" >> beam.ParDo(
            WaitForBQJobs(self.test_client),
            beam.pvalue.AsList(destination_job_ids_pc))
        | beam.ParDo(TriggerCopyJobs(
            create_disposition=self.create_disposition,
            write_disposition=self.write_disposition,
            temporary_tables=self.temp_tables,
            test_client=self.test_client), load_job_name_pcv))

    _ = (p
         | "ImpulseMonitorCopyJobs" >> beam.Create([None])
         | "WaitForCopyJobs" >> beam.ParDo(
             WaitForBQJobs(self.test_client),
             beam.pvalue.AsList(destination_copy_job_ids_pc)))

    # TODO: Must delete temporary tables.

    return {
        self.DESTINATION_JOBID_PAIRS: destination_job_ids_pc,
        self.DESTINATION_FILE_PAIRS: all_destination_file_pairs_pc,
        self.DESTINATION_COPY_JOBID_PAIRS: destination_copy_job_ids_pc,
    }
