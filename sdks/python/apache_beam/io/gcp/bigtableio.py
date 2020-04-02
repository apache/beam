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
from __future__ import absolute_import

import apache_beam as beam
from apache_beam.io import iobase
from apache_beam.metrics import Metrics
from apache_beam.transforms import util
from apache_beam.transforms.display import DisplayDataItem
from collections import namedtuple

try:
  from google.cloud.bigtable import Client
except ImportError:
  pass

__all__ = ['WriteToBigTable', 'ReadFromBigtable']


class _BigtableReadFn(beam.DoFn):
  """ A DoFn to parallelize reading from a Bigtable table

  :type project_id: str
  :param project_id: The ID of the project used for Bigtable access

  :type instance_id: str
  :param instance_id: The ID of the instance that owns the table.

  :type table_id: str
  :param table_id: The ID of the table.

  :type filter_: :class:`.RowFilter`
  :param filter_: (Optional) The filter to apply to the contents of the
                  specified row(s). If unset, reads every column in
                  each row.
  """
  def __init__(self, project_id, instance_id, table_id, filter_=None):
    super(self.__class__, self).__init__()
    self._initialize({'project_id': project_id,
                     'instance_id': instance_id,
                     'table_id': table_id,
                     'filter_': filter_})

  def _initialize(self, options):
    """ The defaults initializer, to assist with pickling

    :return: None
    """
    self._options = options
    self._table = None
    self._counter = Metrics.counter(self.__class__, 'Rows Read')

  def __getstate__(self):
    return self._options

  def __setstate__(self, options):
    self._initialize(options)

  def start_bundle(self):
    # from google.cloud.bigtable import Client
    if self._table is None:
      # noinspection PyAttributeOutsideInit
      self._table = Client(project=self._options['project_id'])\
        .instance(self._options['instance_id'])\
        .table(self._options['table_id'])

  def process(self, source_bundle):
    _start_key = source_bundle.start_position
    _end_key = source_bundle.stop_position
    for row in self._table.read_rows(_start_key, _end_key):
      self._counter.inc()
      yield row

  def display_data(self):
    return {'projectId': DisplayDataItem(self._options['project_id'],
                                         label='Bigtable Project Id'),
            'instanceId': DisplayDataItem(self._options['instance_id'],
                                          label='Bigtable Instance Id'),
            'tableId': DisplayDataItem(self._options['table_id'],
                                       label='Bigtable Table Id')}


class ReadFromBigtable(beam.PTransform):
  def __init__(self, project_id, instance_id, table_id, filter_=None):
    """ A PTransform wrapper for parallel reading rows from s Bigtable table.

    :type project_id: str
    :param project_id: The ID of the project used for Bigtable access

    :type instance_id: str
    :param instance_id: The ID of the instance that owns the table.

    :type table_id: str
    :param table_id: The ID of the table.

    :type filter_: :class:`.RowFilter`
    :param filter_: (Optional) The filter to apply to the contents of the
                    specified row(s). If unset, reads every column in
                    each row. If noe is provided, all rows are read by default.
    """
    super(self.__class__, self).__init__()
    self._options = {'project_id': project_id,
                     'instance_id': instance_id,
                     'table_id': table_id,
                     'filter_': filter_}

  def __getstate__(self):
    return self._options

  def __setstate__(self, options):
    self._options = options

  def expand(self, pbegin):
    table = Client(project=self._options['project_id'], admin=True) \
      .instance(instance_id=self._options['instance_id']) \
      .table(table_id=self._options['table_id'])

    keys = list(table.sample_row_keys())

    SampleRowKey = namedtuple("SampleRowKey", "row_key offset_bytes")
    keys.insert(0, SampleRowKey(b'', 0))

    def chunks():
      for i in range(1, len(keys)):
        key_1 = keys[i - 1].row_key
        key_2 = keys[i].row_key
        size = keys[i].offset_bytes - keys[i - 1].offset_bytes
        yield iobase.SourceBundle(size, None, key_1, key_2)

    return (pbegin
            | 'Bundles' >> beam.Create(iter(chunks()))
            | 'Reshuffle' >> util.Reshuffle()
            | 'Read' >> beam.ParDo(_BigtableReadFn(self._options['project_id'],
                                                   self._options['instance_id'],
                                                   self._options['table_id'],
                                                   self._options['filter_'])))


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
    super(_BigTableWriteFn, self).__init__()
    self.beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id}
    self.table = None
    self.batcher = None
    self.written = Metrics.counter(self.__class__, 'Written Row')

  def __getstate__(self):
    return self.beam_options

  def __setstate__(self, options):
    self.beam_options = options
    self.table = None
    self.batcher = None
    self.written = Metrics.counter(self.__class__, 'Written Row')

  def start_bundle(self):
    if self.table is None:
      client = Client(project=self.beam_options['project_id'])
      instance = client.instance(self.beam_options['instance_id'])
      self.table = instance.table(self.beam_options['table_id'])
    self.batcher = self.table.mutations_batcher()

  def process(self, row):
    self.written.inc()
    # You need to set the timestamp in the cells in this row object,
    # when we do a retry we will mutating the same object, but, with this
    # we are going to set our cell with new values.
    # Example:
    # direct_row.set_cell('cf1',
    #                     'field1',
    #                     'value1',
    #                     timestamp=datetime.datetime.now())
    self.batcher.mutate(row)

  def finish_bundle(self):
    self.batcher.flush()
    self.batcher = None

  def display_data(self):
    return {'projectId': DisplayDataItem(self.beam_options['project_id'],
                                         label='Bigtable Project Id'),
            'instanceId': DisplayDataItem(self.beam_options['instance_id'],
                                          label='Bigtable Instance Id'),
            'tableId': DisplayDataItem(self.beam_options['table_id'],
                                       label='Bigtable Table Id')
           }


class WriteToBigTable(beam.PTransform):
  """ A transform to write to the Bigtable Table.

  A PTransform that write a list of `DirectRow` into the Bigtable Table

  """
  def __init__(self, project_id=None, instance_id=None,
               table_id=None):
    """ The PTransform to access the Bigtable Write connector
    Args:
      project_id(str): GCP Project of to write the Rows
      instance_id(str): GCP Instance to write the Rows
      table_id(str): GCP Table to write the `DirectRows`
    """
    super(WriteToBigTable, self).__init__()
    self.beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id}

  def expand(self, pvalue):
    beam_options = self.beam_options
    return (pvalue
            | beam.ParDo(_BigTableWriteFn(beam_options['project_id'],
                                          beam_options['instance_id'],
                                          beam_options['table_id'])))
