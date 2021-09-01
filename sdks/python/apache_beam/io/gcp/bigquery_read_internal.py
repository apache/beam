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
Internal library for reading data from BigQuery.

NOTHING IN THIS FILE HAS BACKWARDS COMPATIBILITY GUARANTEES.
"""
import collections
import decimal
import json
import logging
import random
import time
import uuid
from typing import TYPE_CHECKING
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Union

import apache_beam as beam
from apache_beam.coders import coders
from apache_beam.io.avroio import _create_avro_source
from apache_beam.io.filesystem import CompressionTypes
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp import bigquery_tools
from apache_beam.io.gcp.bigquery_io_metadata import create_bigquery_io_metadata
from apache_beam.io.iobase import BoundedSource
from apache_beam.io.textio import _TextSource
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.value_provider import ValueProvider
from apache_beam.transforms import PTransform

if TYPE_CHECKING:
  from apache_beam.io.gcp.bigquery import ReadFromBigQueryRequest

try:
  from apache_beam.io.gcp.internal.clients.bigquery import DatasetReference
  from apache_beam.io.gcp.internal.clients.bigquery import TableReference
except ImportError:
  DatasetReference = None
  TableReference = None

_LOGGER = logging.getLogger(__name__)


def bigquery_export_destination_uri(
    gcs_location_vp: Optional[ValueProvider],
    temp_location: Optional[str],
    unique_id: str,
    directory_only: bool = False,
) -> str:
  """Returns the fully qualified Google Cloud Storage URI where the
  extracted table should be written.
  """
  file_pattern = 'bigquery-table-dump-*.json'

  gcs_location = None
  if gcs_location_vp is not None:
    gcs_location = gcs_location_vp.get()

  if gcs_location is not None:
    gcs_base = gcs_location
  elif temp_location is not None:
    gcs_base = temp_location
    _LOGGER.debug("gcs_location is empty, using temp_location instead")
  else:
    raise ValueError(
        'ReadFromBigQuery requires a GCS location to be provided. Neither '
        'gcs_location in the constructor nor the fallback option '
        '--temp_location is set.')

  if not unique_id:
    unique_id = uuid.uuid4().hex

  if directory_only:
    return FileSystems.join(gcs_base, unique_id)
  else:
    return FileSystems.join(gcs_base, unique_id, file_pattern)


class _PassThroughThenCleanup(PTransform):
  """A PTransform that invokes a DoFn after the input PCollection has been
    processed.

    DoFn should have arguments (element, side_input, cleanup_signal).

    Utilizes readiness of PCollection to trigger DoFn.
  """
  def __init__(self, side_input=None):
    self.side_input = side_input

  def expand(self, input):
    class PassThrough(beam.DoFn):
      def process(self, element):
        yield element

    class RemoveExtractedFiles(beam.DoFn):
      def process(self, unused_element, unused_signal, gcs_locations):
        FileSystems.delete(list(gcs_locations))

    main_output, cleanup_signal = input | beam.ParDo(
        PassThrough()).with_outputs(
        'cleanup_signal', main='main')

    cleanup_input = input.pipeline | beam.Create([None])

    _ = cleanup_input | beam.ParDo(
        RemoveExtractedFiles(),
        beam.pvalue.AsSingleton(cleanup_signal),
        self.side_input,
    )

    return main_output


class _BigQueryReadSplit(beam.transforms.DoFn):
  """Starts the process of reading from BigQuery.

  This transform will start a BigQuery export job, and output a number of
  file sources that are consumed downstream.
  """
  def __init__(
      self,
      options: PipelineOptions,
      gcs_location: Union[str, ValueProvider] = None,
      use_json_exports: bool = False,
      bigquery_job_labels: Dict[str, str] = None,
      step_name: str = None,
      job_name: str = None,
      unique_id: str = None,
      kms_key: str = None,
      project: str = None,
      temp_dataset: Union[str, DatasetReference] = None):
    self.options = options
    self.use_json_exports = use_json_exports
    self.gcs_location = gcs_location
    self.bigquery_job_labels = bigquery_job_labels or {}
    self._step_name = step_name
    self._job_name = job_name or 'BQ_READ_SPLIT'
    self._source_uuid = unique_id
    self.kms_key = kms_key
    self.project = project
    self.temp_dataset = temp_dataset or 'bq_read_all_%s' % uuid.uuid4().hex
    self.bq_io_metadata = None

  def display_data(self):
    return {
        'use_json_exports': str(self.use_json_exports),
        'gcs_location': str(self.gcs_location),
        'bigquery_job_labels': json.dumps(self.bigquery_job_labels),
        'kms_key': str(self.kms_key),
        'project': str(self.project),
        'temp_dataset': str(self.temp_dataset)
    }

  def _get_temp_dataset(self):
    if isinstance(self.temp_dataset, str):
      return DatasetReference(
          datasetId=self.temp_dataset, projectId=self._get_project())
    else:
      return self.temp_dataset

  def process(self,
              element: 'ReadFromBigQueryRequest') -> Iterable[BoundedSource]:
    bq = bigquery_tools.BigQueryWrapper(
        temp_dataset_id=self._get_temp_dataset().datasetId)

    if element.query is not None:
      self._setup_temporary_dataset(bq, element)
      table_reference = self._execute_query(bq, element)
    else:
      assert element.table
      table_reference = bigquery_tools.parse_table_reference(
          element.table, project=self._get_project())

    if not table_reference.projectId:
      table_reference.projectId = self._get_project()

    schema, metadata_list = self._export_files(bq, element, table_reference)

    for metadata in metadata_list:
      yield self._create_source(metadata.path, schema)

    if element.query is not None:
      bq._delete_table(
          table_reference.projectId,
          table_reference.datasetId,
          table_reference.tableId)

    if bq.created_temp_dataset:
      self._clean_temporary_dataset(bq, element)

  def _get_bq_metadata(self):
    if not self.bq_io_metadata:
      self.bq_io_metadata = create_bigquery_io_metadata(self._step_name)
    return self.bq_io_metadata

  def _create_source(self, path, schema):
    if not self.use_json_exports:
      return _create_avro_source(path, use_fastavro=True)
    else:
      return _TextSource(
          path,
          min_bundle_size=0,
          compression_type=CompressionTypes.UNCOMPRESSED,
          strip_trailing_newlines=True,
          coder=_JsonToDictCoder(schema))

  def _setup_temporary_dataset(
      self,
      bq: bigquery_tools.BigQueryWrapper,
      element: 'ReadFromBigQueryRequest'):
    location = bq.get_query_location(
        self._get_project(), element.query, not element.use_standard_sql)
    bq.create_temporary_dataset(self._get_project(), location)

  def _clean_temporary_dataset(
      self,
      bq: bigquery_tools.BigQueryWrapper,
      element: 'ReadFromBigQueryRequest'):
    bq.clean_up_temporary_dataset(self._get_project())

  def _execute_query(
      self,
      bq: bigquery_tools.BigQueryWrapper,
      element: 'ReadFromBigQueryRequest'):
    query_job_name = bigquery_tools.generate_bq_job_name(
        self._job_name,
        self._source_uuid,
        bigquery_tools.BigQueryJobTypes.QUERY,
        '%s_%s' % (int(time.time()), random.randint(0, 1000)))
    job = bq._start_query_job(
        self._get_project(),
        element.query,
        not element.use_standard_sql,
        element.flatten_results,
        job_id=query_job_name,
        kms_key=self.kms_key,
        job_labels=self._get_bq_metadata().add_additional_bq_job_labels(
            self.bigquery_job_labels))
    job_ref = job.jobReference
    bq.wait_for_bq_job(job_ref, max_retries=0)
    return bq._get_temp_table(self._get_project())

  def _export_files(
      self,
      bq: bigquery_tools.BigQueryWrapper,
      element: 'ReadFromBigQueryRequest',
      table_reference: TableReference):
    """Runs a BigQuery export job.

    Returns:
      bigquery.TableSchema instance, a list of FileMetadata instances
    """
    job_labels = self._get_bq_metadata().add_additional_bq_job_labels(
        self.bigquery_job_labels)
    export_job_name = bigquery_tools.generate_bq_job_name(
        self._job_name,
        self._source_uuid,
        bigquery_tools.BigQueryJobTypes.EXPORT,
        element.obj_id)
    temp_location = self.options.view_as(GoogleCloudOptions).temp_location
    gcs_location = bigquery_export_destination_uri(
        self.gcs_location,
        temp_location,
        '%s%s' % (self._source_uuid, element.obj_id))
    if self.use_json_exports:
      job_ref = bq.perform_extract_job([gcs_location],
                                       export_job_name,
                                       table_reference,
                                       bigquery_tools.FileFormat.JSON,
                                       project=self._get_project(),
                                       job_labels=job_labels,
                                       include_header=False)
    else:
      job_ref = bq.perform_extract_job([gcs_location],
                                       export_job_name,
                                       table_reference,
                                       bigquery_tools.FileFormat.AVRO,
                                       project=self._get_project(),
                                       include_header=False,
                                       job_labels=job_labels,
                                       use_avro_logical_types=True)
    bq.wait_for_bq_job(job_ref)
    metadata_list = FileSystems.match([gcs_location])[0].metadata_list

    if isinstance(table_reference, ValueProvider):
      table_ref = bigquery_tools.parse_table_reference(
          element.table, project=self._get_project())
    else:
      table_ref = table_reference
    table = bq.get_table(
        table_ref.projectId, table_ref.datasetId, table_ref.tableId)

    return table.schema, metadata_list

  def _get_project(self):
    """Returns the project that queries and exports will be billed to."""

    project = self.options.view_as(GoogleCloudOptions).project
    if isinstance(project, ValueProvider):
      project = project.get()
    if not project:
      project = self.project
    return project


FieldSchema = collections.namedtuple('FieldSchema', 'fields mode name type')


class _JsonToDictCoder(coders.Coder):
  """A coder for a JSON string to a Python dict."""
  def __init__(self, table_schema):
    self.fields = self._convert_to_tuple(table_schema.fields)
    self._converters = {
        'INTEGER': int,
        'INT64': int,
        'FLOAT': float,
        'FLOAT64': float,
        'NUMERIC': self._to_decimal,
        'BYTES': self._to_bytes,
    }

  @staticmethod
  def _to_decimal(value):
    return decimal.Decimal(value)

  @staticmethod
  def _to_bytes(value):
    """Converts value from str to bytes."""
    return value.encode('utf-8')

  @classmethod
  def _convert_to_tuple(cls, table_field_schemas):
    """Recursively converts the list of TableFieldSchema instances to the
    list of tuples to prevent errors when pickling and unpickling
    TableFieldSchema instances.
    """
    if not table_field_schemas:
      return []

    return [
        FieldSchema(cls._convert_to_tuple(x.fields), x.mode, x.name, x.type)
        for x in table_field_schemas
    ]

  def decode(self, value):
    value = json.loads(value.decode('utf-8'))
    return self._decode_row(value, self.fields)

  def _decode_row(self, row: Dict[str, Any], schema_fields: List[FieldSchema]):
    for field in schema_fields:
      if field.name not in row:
        # The field exists in the schema, but it doesn't exist in this row.
        # It probably means its value was null, as the extract to JSON job
        # doesn't preserve null fields
        row[field.name] = None
        continue

      if field.mode == 'REPEATED':
        for i, elem in enumerate(row[field.name]):
          row[field.name][i] = self._decode_data(elem, field)
      else:
        row[field.name] = self._decode_data(row[field.name], field)
    return row

  def _decode_data(self, obj: Any, field: FieldSchema):
    if not field.fields:
      try:
        return self._converters[field.type](obj)
      except KeyError:
        # No need to do any conversion
        return obj
    return self._decode_row(obj, field.fields)

  def is_deterministic(self):
    return True

  def to_type_hint(self):
    return dict
