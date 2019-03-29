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

This source is able to work around BigQuery load quotas and limitations. When
destinations are dynamic, or when data for a single job is too large, the data
will be split into multiple jobs.

NOTHING IN THIS FILE HAS BACKWARDS COMPATIBILITY GUARANTEES.
"""

from __future__ import absolute_import

import datetime
import hashlib
import itertools
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
from apache_beam.options.pipeline_options import GoogleCloudOptions

ONE_TERABYTE = (1 << 40)

# The maximum file size for imports is 5TB. We keep our files under that.
_DEFAULT_MAX_FILE_SIZE = 4 * ONE_TERABYTE

_DEFAULT_MAX_WRITERS_PER_BUNDLE = 20

# The maximum size for a single load job is one terabyte
_MAXIMUM_LOAD_SIZE = 15 * ONE_TERABYTE

# Big query only supports up to 10 thousand URIs for a single load job.
_MAXIMUM_SOURCE_URIS = 10*1000


def _generate_load_job_name():
  datetime_component = datetime.datetime.now().strftime("%Y_%m_%d_%H%M%S")
  # TODO(pabloem): include job id / pipeline component?
  return 'beam_load_%s_%s' % (datetime_component, random.randint(0, 100))


def file_prefix_generator(with_validation=True):
  def _generate_file_prefix(pipeline_gcs_location):
    # If a gcs location is provided to the pipeline, then we shall use that.
    # Otherwise, we shall use the temp_location from pipeline options.
    gcs_base = str(pipeline_gcs_location or
                   vp.RuntimeValueProvider.get_value('temp_location', str, ''))

    # This will fail at pipeline execution time, but will fail early, as this
    # step doesn't have any dependencies (and thus will be one of the first
    # stages to be run).
    if with_validation and (not gcs_base or not gcs_base.startswith('gs://')):
      raise ValueError('Invalid GCS location.\n'
                       'Writing to BigQuery with FILE_LOADS method requires a '
                       'GCS location to be provided to write files to be loaded'
                       ' loaded into BigQuery. Please provide a GCS bucket, or '
                       'pass method="STREAMING_INSERTS" to WriteToBigQuery.')

    prefix_uuid = _bq_uuid()
    return fs.FileSystems.join(gcs_base, 'bq_load', prefix_uuid)

  return _generate_file_prefix


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


def _bq_uuid(seed=None):
  if not seed:
    return str(uuid.uuid4()).replace("-", "")
  else:
    return str(hashlib.md5(seed.encode('utf8')).hexdigest())


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

  This transform keeps up to ``max_files_per_bundle`` files open to write to. It
  receives (destination, record) tuples, and it writes the records to different
  files for each destination.

  If there are more than ``max_files_per_bundle`` destinations that we need to
  write to, then those records are grouped by their destination, and later
  written to files by ``WriteGroupedRecordsToFile``.

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
    """Take a tuple with (destination, row) and write to file or spill out.

    Destination may be a ``TableReference`` or a string, and row is a
    Python dictionary for a row to be inserted to BigQuery."""
    destination = bigquery_tools.get_hashable_destination(element[0])
    row = element[1]

    if destination in self._destination_to_file_writer:
      writer = self._destination_to_file_writer[destination]
    elif len(self._destination_to_file_writer) < self.max_files_per_bundle:
      (file_path, writer) = _make_new_file_writer(file_prefix, destination)
      self._destination_to_file_writer[destination] = writer
      yield pvalue.TaggedOutput(WriteRecordsToFile.WRITTEN_FILE_TAG,
                                (element[0], file_path))
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
    for _, writer in iteritems(self._destination_to_file_writer):
      writer.close()
    self._destination_to_file_writer = {}


class WriteGroupedRecordsToFile(beam.DoFn):
  """Receives collection of dest-iterable(records), writes it to files.

  This is different from ``WriteRecordsToFile`` because it receives records
  grouped by destination. This means that it's not necessary to keep multiple
  file descriptors open, because we know for sure when records for a single
  destination have been written out.

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
  """Launches jobs to copy from temporary tables into the main target table.

  When a job needs to write to multiple destination tables, or when a single
  destination table needs to have multiple load jobs to write to it, files are
  loaded into temporary tables, and those tables are later copied to the
  destination tables.

  This transform emits (destination, job_reference) pairs.
  """
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

  def process(self, element, job_name_prefix=None):
    destination = element[0]
    job_reference = element[1]

    if not self.temporary_tables:
      # If we did not use temporary tables, then we do not need to trigger any
      # copy jobs.
      return

    copy_to_reference = bigquery_tools.parse_table_reference(destination)
    if copy_to_reference.projectId is None:
      copy_to_reference.projectId = vp.RuntimeValueProvider.get_value('project',
                                                                      str, '')

    copy_from_reference = bigquery_tools.parse_table_reference(destination)
    copy_from_reference.tableId = job_reference.jobId
    if copy_from_reference.projectId is None:
      copy_from_reference.projectId = vp.RuntimeValueProvider.get_value(
          'project', str, '')

    copy_job_name = '%s_copy_%s_to_%s' % (
        job_name_prefix,
        _bq_uuid('%s:%s.%s' % (copy_from_reference.projectId,
                               copy_from_reference.datasetId,
                               copy_from_reference.tableId)),
        _bq_uuid('%s:%s.%s' % (copy_to_reference.projectId,
                               copy_to_reference.datasetId,
                               copy_to_reference.tableId)))

    logging.info("Triggering copy job from %s to %s",
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

  TEMP_TABLES = 'TemporaryTables'

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
    files = iter(element[1])

    if callable(self.schema):
      schema = self.schema(destination)
    elif isinstance(self.schema, vp.ValueProvider):
      schema = self.schema.get()
    else:
      schema = self.schema

    job_count = 0
    batch_of_files = list(itertools.islice(files, _MAXIMUM_SOURCE_URIS))
    while batch_of_files:

      table_reference = bigquery_tools.parse_table_reference(destination)
      if table_reference.projectId is None:
        table_reference.projectId = vp.RuntimeValueProvider.get_value(
            'project', str, '')

      # Load jobs for a single destination are always triggered from the same
      # worker. This means that we can generate a deterministic numbered job id,
      # and not need to worry.
      job_name = '%s_%s_%s' % (
          load_job_name_prefix,
          _bq_uuid('%s:%s.%s' % (table_reference.projectId,
                                 table_reference.datasetId,
                                 table_reference.tableId)),
          job_count)
      logging.debug("Batch of files has %s files. Job name is %s",
                    len(batch_of_files), job_name)

      if self.temporary_tables:
        # For temporary tables, we create a new table with the name with JobId.
        table_reference.tableId = job_name
        yield pvalue.TaggedOutput(TriggerLoadJobs.TEMP_TABLES, table_reference)

      logging.info("Triggering job %s to load data to BigQuery table %s.",
                   job_name, table_reference)
      job_reference = self.bq_wrapper.perform_load_job(
          table_reference, batch_of_files, job_name,
          schema=schema,
          write_disposition=self.write_disposition,
          create_disposition=self.create_disposition)
      yield (destination, job_reference)

      # Prepare to trigger the next job
      job_count += 1
      batch_of_files = list(itertools.islice(files, _MAXIMUM_SOURCE_URIS))


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


class DeleteTablesFn(beam.DoFn):
  def __init__(self, test_client=None):
    self.test_client = test_client

  def start_bundle(self):
    self.bq_wrapper = bigquery_tools.BigQueryWrapper(client=self.test_client)

  def process(self, table_reference):
    logging.info("Deleting table %s", table_reference)
    table_reference = bigquery_tools.parse_table_reference(table_reference)
    self.bq_wrapper._delete_table(
        table_reference.projectId,
        table_reference.datasetId,
        table_reference.tableId)


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
      custom_gcs_temp_location=None,
      create_disposition=None,
      write_disposition=None,
      coder=None,
      max_file_size=None,
      max_files_per_bundle=None,
      test_client=None,
      validate=True):
    self.destination = destination
    self.create_disposition = create_disposition
    self.write_disposition = write_disposition
    self.max_file_size = max_file_size or _DEFAULT_MAX_FILE_SIZE
    self.max_files_per_bundle = (max_files_per_bundle or
                                 _DEFAULT_MAX_WRITERS_PER_BUNDLE)
    self._custom_gcs_temp_location = custom_gcs_temp_location
    self.test_client = test_client
    self.schema = schema
    self.coder = coder or bigquery_tools.RowAsDictJsonCoder()

    # If we have multiple destinations, then we will have multiple load jobs,
    # thus we will need temporary tables for atomicity.
    # If the destination is a single one, we assume that we will have only one
    # job to run - and thus we avoid using temporary tables
    self.temp_tables = True if callable(destination) else False

    self._validate = validate
    if self._validate:
      self.verify()

  def verify(self):
    if (isinstance(self._custom_gcs_temp_location, str) and
        not self._custom_gcs_temp_location.startswith('gs://')):
      # Only fail if the custom location is provided, and it is not a GCS
      # location.
      raise ValueError('Invalid GCS location.\n'
                       'Writing to BigQuery with FILE_LOADS method requires a '
                       'GCS location to be provided to write files to be '
                       'loaded into BigQuery. Please provide a GCS bucket, or '
                       'pass method="STREAMING_INSERTS" to WriteToBigQuery.')

  def expand(self, pcoll):
    p = pcoll.pipeline

    self._custom_gcs_temp_location = (
        self._custom_gcs_temp_location
        or p.options.view_as(GoogleCloudOptions).temp_location)

    load_job_name_pcv = pvalue.AsSingleton(
        p
        | "ImpulseJobName" >> beam.Create([None])
        | beam.Map(lambda _: _generate_load_job_name()))

    file_prefix_pcv = pvalue.AsSingleton(
        p
        | "CreateFilePrefixView" >> beam.Create(
            [self._custom_gcs_temp_location])
        | "GenerateFilePrefix" >> beam.Map(
            file_prefix_generator(self._validate)))

    outputs = (
        pcoll
        | "ApplyGlobalWindow" >> beam.WindowInto(beam.window.GlobalWindows())
        | "AppendDestination" >> beam.ParDo(bigquery_tools.AppendDestinationsFn(
            self.destination))
        | beam.ParDo(
            WriteRecordsToFile(max_files_per_bundle=self.max_files_per_bundle,
                               max_file_size=self.max_file_size,
                               coder=self.coder),
            file_prefix=file_prefix_pcv).with_outputs(
                WriteRecordsToFile.UNWRITTEN_RECORD_TAG,
                WriteRecordsToFile.WRITTEN_FILE_TAG))

    # A PCollection of (destination, file) tuples. It lists files with records,
    # and the destination each file is meant to be imported into.
    destination_files_kv_pc = outputs[WriteRecordsToFile.WRITTEN_FILE_TAG]

    # A PCollection of (destination, record) tuples. These are later sharded,
    # grouped, and all records for each destination-shard is written to files.
    # This PCollection is necessary because not all records can be written into
    # files in ``WriteRecordsToFile``.
    unwritten_records_pc = outputs[WriteRecordsToFile.UNWRITTEN_RECORD_TAG]

    more_destination_files_kv_pc = (
        unwritten_records_pc
        | beam.ParDo(_ShardDestinations())
        | "GroupShardedRows" >> beam.GroupByKey()
        | "DropShardNumber" >> beam.Map(lambda x: (x[0][0], x[1]))
        | "WriteGroupedRecordsToFile" >> beam.ParDo(WriteGroupedRecordsToFile(
            coder=self.coder), file_prefix=file_prefix_pcv)
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
    trigger_loads_outputs = (
        grouped_files_pc | beam.ParDo(TriggerLoadJobs(
            schema=self.schema,
            write_disposition=self.write_disposition,
            create_disposition=self.create_disposition,
            test_client=self.test_client,
            temporary_tables=self.temp_tables), load_job_name_pcv).with_outputs(
                TriggerLoadJobs.TEMP_TABLES, main='main')
    )

    destination_job_ids_pc = trigger_loads_outputs['main']
    temp_tables_pc = trigger_loads_outputs[TriggerLoadJobs.TEMP_TABLES]

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

    finished_copy_jobs_pc = (p
                             | "ImpulseMonitorCopyJobs" >> beam.Create([None])
                             | "WaitForCopyJobs" >> beam.ParDo(
                                 WaitForBQJobs(self.test_client),
                                 beam.pvalue.AsList(destination_copy_job_ids_pc)
                             ))

    _ = (finished_copy_jobs_pc
         | "RemoveTempTables/PassTables" >> beam.FlatMap(
             lambda x, deleting_tables: deleting_tables,
             pvalue.AsIter(temp_tables_pc))
         | "RemoveTempTables/AddUselessValue" >> beam.Map(lambda x: (x, None))
         | "RemoveTempTables/DeduplicateTables" >> beam.GroupByKey()
         | "RemoveTempTables/GetTableNames" >> beam.Map(lambda elm: elm[0])
         | "RemoveTempTables/Delete" >> beam.ParDo(DeleteTablesFn()))

    return {
        self.DESTINATION_JOBID_PAIRS: destination_job_ids_pc,
        self.DESTINATION_FILE_PAIRS: all_destination_file_pairs_pc,
        self.DESTINATION_COPY_JOBID_PAIRS: destination_copy_job_ids_pc,
    }
