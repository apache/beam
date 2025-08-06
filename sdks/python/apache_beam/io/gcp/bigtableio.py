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

"""BigTable connector

This module implements writing to BigTable tables.
The default mode is to set row data to write to BigTable tables.
The syntax supported is described here:
https://cloud.google.com/bigtable/docs/quickstart-cbt

BigTable connector can be used as main outputs. A main output
(common case) is expected to be massive and will be split into
manageable chunks and processed in parallel. In the example below
we created a list of rows then passed to the GeneratedDirectRows
DoFn to set the Cells and then we call the BigTableWriteFn to insert
those generated rows in the table.

  main_table = (p
                | beam.Create(self._generate())
                | WriteToBigTable(project_id,
                                  instance_id,
                                  table_id))
"""
# pytype: skip-file

import logging
import struct
from typing import Dict
from typing import List

import apache_beam as beam
from apache_beam.internal.metrics.metric import ServiceCallMetric
from apache_beam.io.gcp import resource_identifiers
from apache_beam.metrics import Metrics
from apache_beam.metrics import monitoring_infos
from apache_beam.metrics.metric import Lineage
from apache_beam.transforms import PTransform
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import SchemaAwareExternalTransform
from apache_beam.typehints.row_type import RowTypeConstraint

_LOGGER = logging.getLogger(__name__)
FLUSH_COUNT = 1000
MAX_ROW_BYTES = 5242880  # 5MB

try:
  from google.cloud.bigtable import Client
  from google.cloud.bigtable.row import Cell, PartialRowData
  from google.cloud.bigtable.batcher import MutationsBatcher

except ImportError:
  _LOGGER.warning(
      'ImportError: from google.cloud.bigtable import Client', exc_info=True)

__all__ = ['WriteToBigTable', 'ReadFromBigtable']


class _BigTableWriteFn(beam.DoFn):
  """ Creates the connector can call and add_row to the batcher using each
  row in beam pipe line
  Args:
    project_id(str): GCP Project ID
    instance_id(str): GCP Instance ID
    table_id(str): GCP Table ID
    flush_count(int): Max number of rows to flush
    max_row_bytes(int) Max number of row mutations size to flush

  """
  def __init__(
      self, project_id, instance_id, table_id, flush_count, max_row_bytes):
    """ Constructor of the Write connector of Bigtable
    Args:
      project_id(str): GCP Project of to write the Rows
      instance_id(str): GCP Instance to write the Rows
      table_id(str): GCP Table to write the `DirectRows`
      flush_count(int): Max number of rows to flush
      max_row_bytes(int) Max number of row mutations size to flush
    """
    super().__init__()
    self.beam_options = {
        'project_id': project_id,
        'instance_id': instance_id,
        'table_id': table_id,
        'flush_count': flush_count,
        'max_row_bytes': max_row_bytes,
    }
    self.table = None
    self.batcher = None
    self.service_call_metric = None
    self.written = Metrics.counter(self.__class__, 'Written Row')

  def __getstate__(self):
    return self.beam_options

  def __setstate__(self, options):
    self.beam_options = options
    self.table = None
    self.batcher = None
    self.service_call_metric = None
    self.written = Metrics.counter(self.__class__, 'Written Row')

  def write_mutate_metrics(self, status_list):
    for status in status_list:
      code = status.code if status else None
      grpc_status_string = (
          ServiceCallMetric.bigtable_error_code_to_grpc_status_string(code))
      self.service_call_metric.call(grpc_status_string)

  def start_service_call_metrics(self, project_id, instance_id, table_id):
    resource = resource_identifiers.BigtableTable(
        project_id, instance_id, table_id)
    labels = {
        monitoring_infos.SERVICE_LABEL: 'BigTable',
        # TODO(JIRA-11985): Add Ptransform label.
        monitoring_infos.METHOD_LABEL: 'google.bigtable.v2.MutateRows',
        monitoring_infos.RESOURCE_LABEL: resource,
        monitoring_infos.BIGTABLE_PROJECT_ID_LABEL: (
            self.beam_options['project_id']),
        monitoring_infos.INSTANCE_ID_LABEL: self.beam_options['instance_id'],
        monitoring_infos.TABLE_ID_LABEL: self.beam_options['table_id']
    }
    return ServiceCallMetric(
        request_count_urn=monitoring_infos.API_REQUEST_COUNT_URN,
        base_labels=labels)

  def start_bundle(self):
    if self.table is None:
      client = Client(project=self.beam_options['project_id'])
      instance = client.instance(self.beam_options['instance_id'])
      self.table = instance.table(self.beam_options['table_id'])
    self.service_call_metric = self.start_service_call_metrics(
        self.beam_options['project_id'],
        self.beam_options['instance_id'],
        self.beam_options['table_id'])
    self.batcher = MutationsBatcher(
        self.table,
        batch_completed_callback=self.write_mutate_metrics,
        flush_count=self.beam_options['flush_count'],
        max_row_bytes=self.beam_options['max_row_bytes'])

  def process(self, row):
    self.written.inc()
    # You need to set the timestamp in the cells in this row object,
    # when we do a retry we will mutating the same object, but, with this
    # we are going to set our cell with new values.
    # Example:
    # direct_row.set_cell('cf1',
    #                     'field1',
    #                     'value1',
    #                     timestamp=datetime.now())
    self.batcher.mutate(row)

  def finish_bundle(self):
    if self.batcher:
      self.batcher.close()
      self.batcher = None
      # Report Lineage metrics on write
      Lineage.sinks().add(
          'bigtable',
          self.beam_options['project_id'],
          self.beam_options['instance_id'],
          self.beam_options['table_id'])

  def display_data(self):
    return {
        'projectId': DisplayDataItem(
            self.beam_options['project_id'], label='Bigtable Project Id'),
        'instanceId': DisplayDataItem(
            self.beam_options['instance_id'], label='Bigtable Instance Id'),
        'tableId': DisplayDataItem(
            self.beam_options['table_id'], label='Bigtable Table Id')
    }


class WriteToBigTable(beam.PTransform):
  """A transform that writes rows to a Bigtable table.

  Takes an input PCollection of `DirectRow` objects containing un-committed
  mutations. For more information about this row object, visit
  https://cloud.google.com/python/docs/reference/bigtable/latest/row#class-googlecloudbigtablerowdirectrowrowkey-tablenone

  If flag `use_cross_language` is set to true, this transform will use the
  multi-language transforms framework to inject the Java native write transform
  into the pipeline.
  """
  URN = "beam:schematransform:org.apache.beam:bigtable_write:v1"

  def __init__(
      self,
      project_id,
      instance_id,
      table_id,
      use_cross_language=False,
      expansion_service=None,
      flush_count=FLUSH_COUNT,
      max_row_bytes=MAX_ROW_BYTES,
  ):
    """Initialize an WriteToBigTable transform.

    :param table_id:
      The ID of the table to write to.
    :param instance_id:
      The ID of the instance where the table resides.
    :param project_id:
      The GCP project ID.
    :param use_cross_language:
      If set to True, will use the Java native transform via cross-language.
    :param expansion_service:
      The address of the expansion service in the case of using cross-language.
      If no expansion service is provided, will attempt to run the default GCP
      expansion service.
    :type flush_count: int
    :param flush_count: (Optional) Max number of rows to flush.
      Default is FLUSH_COUNT (1000 rows).
    :type max_row_bytes: int
    :param max_row_bytes: (Optional) Max number of row mutations size to flush.
      Default is MAX_ROW_BYTES (5 MB).
    """
    super().__init__()
    self._table_id = table_id
    self._instance_id = instance_id
    self._project_id = project_id
    self._use_cross_language = use_cross_language
    if use_cross_language:
      self._expansion_service = (
          expansion_service or BeamJarExpansionService(
              'sdks:java:io:google-cloud-platform:expansion-service:build'))
      self.schematransform_config = (
          SchemaAwareExternalTransform.discover_config(
              self._expansion_service, self.URN))

    self._flush_count = flush_count
    self._max_row_bytes = max_row_bytes

  def expand(self, input):
    if self._use_cross_language:
      external_write = SchemaAwareExternalTransform(
          identifier=self.schematransform_config.identifier,
          expansion_service=self._expansion_service,
          rearrange_based_on_discovery=True,
          table_id=self._table_id,
          instance_id=self._instance_id,
          project_id=self._project_id)

      return (
          input
          | beam.ParDo(self._DirectRowMutationsToBeamRow()).with_output_types(
              RowTypeConstraint.from_fields(
                  [("key", bytes), ("mutations", List[Dict[str, bytes]])]))
          | external_write)
    else:
      return (
          input
          | beam.ParDo(
              _BigTableWriteFn(
                  self._project_id,
                  self._instance_id,
                  self._table_id,
                  flush_count=self._flush_count,
                  max_row_bytes=self._max_row_bytes)))

  class _DirectRowMutationsToBeamRow(beam.DoFn):
    def process(self, direct_row):
      args = {"key": direct_row.row_key, "mutations": []}
      # start accumulating mutations in a list
      for mutation in direct_row._get_mutations():
        if mutation.__contains__("set_cell"):
          mutation_dict = {
              "type": b'SetCell',
              "family_name": mutation.set_cell.family_name.encode('utf-8'),
              "column_qualifier": mutation.set_cell.column_qualifier,
              "value": mutation.set_cell.value,
              "timestamp_micros": struct.pack(
                  '>q', mutation.set_cell.timestamp_micros)
          }
        elif mutation.__contains__("delete_from_column"):
          mutation_dict = {
              "type": b'DeleteFromColumn',
              "family_name": mutation.delete_from_column.family_name.encode(
                  'utf-8'),
              "column_qualifier": mutation.delete_from_column.column_qualifier
          }
          time_range = mutation.delete_from_column.time_range
          if time_range.start_timestamp_micros:
            mutation_dict['start_timestamp_micros'] = struct.pack(
                '>q', time_range.start_timestamp_micros)
          if time_range.end_timestamp_micros:
            mutation_dict['end_timestamp_micros'] = struct.pack(
                '>q', time_range.end_timestamp_micros)
        elif mutation.__contains__("delete_from_family"):
          mutation_dict = {
              "type": b'DeleteFromFamily',
              "family_name": mutation.delete_from_family.family_name.encode(
                  'utf-8')
          }
        elif mutation.__contains__("delete_from_row"):
          mutation_dict = {"type": b'DeleteFromRow'}
        else:
          raise ValueError("Unexpected mutation")

        args["mutations"].append(mutation_dict)

      yield beam.Row(**args)


class ReadFromBigtable(PTransform):
  """Reads rows from Bigtable.

  Returns a PCollection of PartialRowData objects, each representing a
  Bigtable row. For more information about this row object, visit
  https://cloud.google.com/python/docs/reference/bigtable/latest/row#class-googlecloudbigtablerowpartialrowdatarowkey
  """
  URN = "beam:schematransform:org.apache.beam:bigtable_read:v1"

  def __init__(self, project_id, instance_id, table_id, expansion_service=None):
    """Initialize a ReadFromBigtable transform.

    :param table_id:
      The ID of the table to read from.
    :param instance_id:
      The ID of the instance where the table resides.
    :param project_id:
      The GCP project ID.
    :param expansion_service:
      The address of the expansion service. If no expansion service is
      provided, will attempt to run the default GCP expansion service.
    """
    super().__init__()
    self._table_id = table_id
    self._instance_id = instance_id
    self._project_id = project_id
    self._expansion_service = (
        expansion_service or BeamJarExpansionService(
            'sdks:java:io:google-cloud-platform:expansion-service:build'))
    self.schematransform_config = SchemaAwareExternalTransform.discover_config(
        self._expansion_service, self.URN)

  def expand(self, input):
    external_read = SchemaAwareExternalTransform(
        identifier=self.schematransform_config.identifier,
        expansion_service=self._expansion_service,
        rearrange_based_on_discovery=True,
        table_id=self._table_id,
        instance_id=self._instance_id,
        project_id=self._project_id,
        flatten=False)

    return (
        input.pipeline
        | external_read
        | beam.ParDo(self._BeamRowToPartialRowData()))

  # PartialRowData has some useful methods for querying data within a row.
  # To make use of those methods and to give Python users a more familiar
  # object, we process each Beam Row and return a PartialRowData equivalent.
  class _BeamRowToPartialRowData(beam.DoFn):
    def process(self, row):
      key = row.key
      families = row.column_families

      # initialize PartialRowData object
      partial_row: PartialRowData = PartialRowData(key)
      for fam_name, col_fam in families.items():
        if fam_name not in partial_row.cells:
          partial_row.cells[fam_name] = {}
        for col_qualifier, cells in col_fam.items():
          # store column qualifier as bytes to follow PartialRowData behavior
          col_qualifier_bytes = col_qualifier.encode()
          if col_qualifier not in partial_row.cells[fam_name]:
            partial_row.cells[fam_name][col_qualifier_bytes] = []
          for cell in cells:
            value = cell.value
            timestamp_micros = cell.timestamp_micros
            partial_row.cells[fam_name][col_qualifier_bytes].append(
                Cell(value, timestamp_micros))
      yield partial_row
