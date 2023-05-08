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
from datetime import datetime, timezone
from typing import List

import apache_beam as beam
from apache_beam.internal.metrics.metric import ServiceCallMetric
from apache_beam.io.gcp import resource_identifiers
from apache_beam.metrics import Metrics
from apache_beam.metrics import monitoring_infos
from apache_beam.transforms import PTransform
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.transforms.external import BeamJarExpansionService
from apache_beam.transforms.external import SchemaAwareExternalTransform

_LOGGER = logging.getLogger(__name__)

try:
  from google.cloud.bigtable import Client
  from google.cloud.bigtable.row import Cell, DirectRow, PartialRowData
  from google.cloud.bigtable.row_filters import TimestampRange
  from google.cloud.bigtable.batcher import MutationsBatcher

  FLUSH_COUNT = 1000
  MAX_ROW_BYTES = 5242880  # 5MB

  class _MutationsBatcher(MutationsBatcher):
    def __init__(
        self, table, flush_count=FLUSH_COUNT, max_row_bytes=MAX_ROW_BYTES):
      super().__init__(table, flush_count, max_row_bytes)
      self.rows = []

    def set_flush_callback(self, callback_fn):
      self.callback_fn = callback_fn

    def flush(self):
      if len(self.rows) != 0:
        status_list = self.table.mutate_rows(self.rows)
        self.callback_fn(status_list)

        # If even one request fails we retry everything. BigTable mutations are
        # idempotent so this should be correct.
        # TODO(https://github.com/apache/beam/issues/21396): make this more
        # efficient by retrying only re-triable failed requests.
        for status in status_list:
          if not status:
            # BigTable client may return 'None' instead of a valid status in
            # some cases due to
            # https://github.com/googleapis/python-bigtable/issues/485
            raise Exception(
                'Failed to write a batch of %r records' % len(self.rows))
          elif status.code != 0:
            raise Exception(
                'Failed to write a batch of %r records due to %r' % (
                    len(self.rows),
                    ServiceCallMetric.bigtable_error_code_to_grpc_status_string(
                        status.code)))

        self.total_mutation_count = 0
        self.total_size = 0
        self.rows = []

except ImportError:
  _LOGGER.warning(
      'ImportError: from google.cloud.bigtable import Client', exc_info=True)

__all__ = ['WriteToBigTable', 'ReadFromBigTable', 'BigtableRow']


class _BigTableWriteFn(beam.DoFn):
  """ Creates the connector can call and add_row to the batcher using each
  row in beam pipe line
  Args:
    project_id(str): GCP Project ID
    instance_id(str): GCP Instance ID
    table_id(str): GCP Table ID

  """
  def __init__(self, project_id, instance_id, table_id):
    """ Constructor of the Write connector of Bigtable
    Args:
      project_id(str): GCP Project of to write the Rows
      instance_id(str): GCP Instance to write the Rows
      table_id(str): GCP Table to write the `DirectRows`
    """
    super().__init__()
    self.beam_options = {
        'project_id': project_id,
        'instance_id': instance_id,
        'table_id': table_id
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
    self.batcher = _MutationsBatcher(self.table)
    self.batcher.set_flush_callback(self.write_mutate_metrics)

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
    self.batcher.flush()
    self.batcher = None

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
  """ A transform to write to the Bigtable Table.

  A PTransform that write a list of `DirectRow` into the Bigtable Table

  """
  def __init__(self, project_id=None, instance_id=None, table_id=None):
    """ The PTransform to access the Bigtable Write connector
    Args:
      project_id(str): GCP Project of to write the Rows
      instance_id(str): GCP Instance to write the Rows
      table_id(str): GCP Table to write the `DirectRows`
    """
    super().__init__()
    self.beam_options = {
        'project_id': project_id,
        'instance_id': instance_id,
        'table_id': table_id
    }

  def expand(self, pvalue):
    beam_options = self.beam_options
    return (
        pvalue
        | beam.ParDo(
            _BigTableWriteFn(
                beam_options['project_id'],
                beam_options['instance_id'],
                beam_options['table_id'])))


class BigtableRow(DirectRow, PartialRowData):
  """Representation of a BigTable row that can both be read and written to
   BigTable.

   Row cells are accessible for reads via:
    * :meth:`find_cells()`
    * :meth:`cell_value()`
    * :meth:`cell_values()`

    Mutations can be created with:
    * :meth:`set_cell`
    * :meth:`delete`
    * :meth:`delete_cell`
    * :meth:`delete_cells`
    and sent directly to the Google Cloud Bigtable API with :meth:`commit`:.

    These methods can be used directly::

       >>> row = BigtableRow('row-key', 'table')
       >>> row.set_cell('fam', 'col1', 'cell-val')
       >>> row.delete_cell(u'fam', b'col2')
       >>> cells = row.find_cells('fam', 'col1')
       >>> row.commit()

    :param row_key: The key for the current row.

    :param table: (Optional) The table that owns the row. This is
                  used for the :meth: `commit` only.

    :param track_mutations: (Defaults to True) When this flag is set, the
      mutations created for this row will be applied to the BigtableRow object.
      The mutations are created and committed with :class:`DirectRow`'s
      methods, and the data is read using :class:`PartialRowData`'s methods.
      This allows us to read from and mutate the same object.
    """
  def __init__(self, row_key, table=None, track_mutations=True):
    DirectRow.__init__(self, row_key, table)
    self._track_mutations = track_mutations
    if track_mutations:
      PartialRowData.__init__(self, row_key)

  def set_cell(
      self, column_family_id, column, value, timestamp: datetime = None):
    """Sets a cell value in this row.

      :param column_family_id: The column family that contains the column.
        Must be of the form ``[_a-zA-Z0-9][-_.a-zA-Z0-9]*``.

      :param column: The column within the column family where the cell
        is located.

      :param value: The value to set in the cell.

      :param timestamp: (Optional) A :class:`datetime.datetime` object
        representing the timestamp of the operation.
     """
    DirectRow.set_cell(self, column_family_id, column, value, timestamp)

    if self._track_mutations:
      if column_family_id not in self._cells:
        self._cells[column_family_id] = {}

      if column not in self._cells[column_family_id]:
        self._cells[column_family_id][column] = []

      if timestamp:
        micros = timestamp.timestamp() * 1e6
      else:
        # TODO: is there a better default value to set here?
        # DirectRow uses timestamp of -1 by default. When committed, the cell
        # in Bigtable will contain the current server timestamp.
        # We set the current UTC timestamp by default to try and mimic this,
        # but it will always be earlier than the true Bigtable cell timestamp.
        micros = datetime.now(timezone.utc)
      self._cells[column_family_id][column].append(Cell(value, micros))

  def delete_cell(
      self, column_family_id, column, time_range: TimestampRange = None):
    """Deletes cells in a specified column.
      :param column_family_id: The column family that contains the column.
        Must be of the form ``[_a-zA-Z0-9][-_.a-zA-Z0-9]*``.

      :param column: The column within the column family where cells will be
        deleted from.

      :param time_range: (Optional) The :class:`TimestampRange` object
        representing the range of time within which cells should be deleted.
    """
    self.delete_cells(column_family_id, [column], time_range)

  def delete_cells(
      self, column_family_id, columns, time_range: TimestampRange = None):
    """Deletes cells across a list of columns.
      :param column_family_id: The column family that contains the columns.
        Must be of the form ``[_a-zA-Z0-9][-_.a-zA-Z0-9]*``.

      :param columns: The columns within the column family where cells will be
        deleted from.

      :param time_range: (Optional) The :class:`TimestampRange` object
        representing the range of time within which cells should be deleted.
    """
    DirectRow.delete_cells(self, column_family_id, columns, time_range)

    if self._track_mutations:
      if columns == self.ALL_COLUMNS:
        del self._cells[column_family_id]
      else:
        for column in columns:
          # if no time range is set, the whole column is deleted
          if not time_range:
            del self._cells[column_family_id][column]
          # otherwise, iterate over the cells and delete those that fall within
          # the time range
          else:
            start: datetime = time_range.start or datetime.min
            end: datetime = time_range.end or datetime.max
            cells: List[Cell] = self._cells[column_family_id][column]

            # make timerange objects timezone-aware to be able to compare them
            # with the cell's timestamp. default to UTC timezone.
            start_end = [start, end]
            for i, dt in enumerate(start_end):
              if dt.tzinfo == None or dt.tzinfo.utcoffset(dt) == None:
                start_end[i] = dt.replace(tzinfo=timezone.utc)
            start, end = start_end

            for i in reversed(range(len(cells))):
              if start <= cells[i].timestamp < end:
                del cells[i]

            if cells == []:
              del self._cells[column_family_id][column]
        if self._cells[column_family_id] == {}:
          del self._cells[column_family_id]

  def delete(self):
    """Deletes this row from the table."""
    DirectRow.delete(self)

    if self._track_mutations:
      self._cells = {}
