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

from collections import namedtuple

import apache_beam as beam
from apache_beam.io import iobase
from apache_beam.metrics import Metrics
from apache_beam.transforms import core
from apache_beam.transforms.display import DisplayDataItem

try:
  from google.cloud.bigtable import Client
  from google.cloud.bigtable.row_set import RowRange
  from google.cloud.bigtable.row_set import RowSet
except ImportError:
  Client = None

__all__ = ['ReadFromBigTable', 'WriteToBigTable']


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
    self._beam_options = {'project_id': project_id,
                          'instance_id': instance_id,
                          'table_id': table_id}
    self.table = None
    self.batcher = None
    self.written = Metrics.counter(self.__class__, 'Written Row')

  def __getstate__(self):
    return self._beam_options

  def __setstate__(self, options):
    self._beam_options = options
    self.table = None
    self.batcher = None
    self.written = Metrics.counter(self.__class__, 'Written Row')

  def start_bundle(self):
    if self.table is None:
      client = Client(project=self._beam_options['project_id'])
      instance = client.instance(self._beam_options['instance_id'])
      self.table = instance.table(self._beam_options['table_id'])
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
    return {'projectId': DisplayDataItem(self._beam_options['project_id'],
                                         label='Bigtable Project Id'),
            'instanceId': DisplayDataItem(self._beam_options['instance_id'],
                                          label='Bigtable Instance Id'),
            'tableId': DisplayDataItem(self._beam_options['table_id'],
                                       label='Bigtable Table Id')
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
    super(WriteToBigTable, self).__init__()
    self._beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id}

  def expand(self, pvalue):
    beam_options = self._beam_options
    return (pvalue
            | beam.ParDo(_BigTableWriteFn(beam_options['project_id'],
                                          beam_options['instance_id'],
                                          beam_options['table_id'])))


class _BigtableReadFn(beam.DoFn):
  """ Creates the connector that can read rows for Beam pipeline

  Args:
    project_id(str): GCP Project ID
    instance_id(str): GCP Instance ID
    table_id(str): GCP Table ID

  """

  def __init__(self, project_id, instance_id, table_id, filter_=b''):
    """ Constructor of the Read connector of Bigtable

    Args:
      project_id: [str] GCP Project of to write the Rows
      instance_id: [str] GCP Instance to write the Rows
      table_id: [str] GCP Table to write the `DirectRows`
      filter_: [RowFilter] Filter to apply to columns in a row.
    """
    super(self.__class__, self).__init__()
    self._initialize({'project_id': project_id,
                      'instance_id': instance_id,
                      'table_id': table_id,
                      'filter_': filter_})

  def __getstate__(self):
    return self._beam_options

  def __setstate__(self, options):
    self._initialize(options)

  def _initialize(self, options):
    self._beam_options = options
    self.table = None
    self.sample_row_keys = None
    self.row_count = Metrics.counter(self.__class__.__name__, 'Rows read')

  def start_bundle(self):
    if self.table is None:
      self.table = Client(project=self._beam_options['project_id'])\
                    .instance(self._beam_options['instance_id'])\
                    .table(self._beam_options['table_id'])

  def process(self, element, **kwargs):
    for row in self.table.read_rows(start_key=element.start_position,
                                    end_key=element.end_position,
                                    filter_=self._beam_options['filter_']):
      self.row_count.inc()
      yield row

  def display_data(self):
    return {'projectId': DisplayDataItem(self._beam_options['project_id'],
                                         label='Bigtable Project Id'),
            'instanceId': DisplayDataItem(self._beam_options['instance_id'],
                                          label='Bigtable Instance Id'),
            'tableId': DisplayDataItem(self._beam_options['table_id'],
                                       label='Bigtable Table Id'),
            'filter_': DisplayDataItem(str(self._beam_options['filter_']),
                                       label='Bigtable Filter')
            }


class ReadFromBigTable(beam.PTransform):
  def __init__(self, project_id, instance_id, table_id, filter_=b''):
    """ The PTransform to access the Bigtable Read connector

    Args:
      project_id: [str] GCP Project of to read the Rows
      instance_id): [str] GCP Instance to read the Rows
      table_id): [str] GCP Table to read the Rows
      filter_: [RowFilter] Filter to apply to columns in a row.
    """
    super(self.__class__, self).__init__()
    self._beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id,
                         'filter_': filter_}

  def __getstate__(self):
      return self._beam_options

  def __setstate__(self, options):
      self._beam_options = options

  def expand(self, pbegin):
    from apache_beam.transforms import util

    beam_options = self._beam_options
    table = Client(project=beam_options['project_id'])\
                .instance(beam_options['instance_id'])\
                .table(beam_options['table_id'])
    sample_row_keys = list(table.sample_row_keys())

    if len(sample_row_keys) > 1 and sample_row_keys[0].row_key != b'':
        SampleRowKey = namedtuple("SampleRowKey", "row_key offset_bytes")
        first_key = SampleRowKey(b'', 0)
        sample_row_keys.insert(0, first_key)
        sample_row_keys = list(sample_row_keys)

    def split_source(unused_impulse):
      bundles = []
      for i in range(1, len(sample_row_keys)):
        key_1 = sample_row_keys[i - 1].row_key
        key_2 = sample_row_keys[i].row_key
        size = sample_row_keys[i].offset_bytes - sample_row_keys[i - 1].offset_bytes
        bundles.append(iobase.SourceBundle(size, None, key_1, key_2))

      from random import shuffle
      # Shuffle is needed to allow reading from different locations of the table for better efficiency
      shuffle(bundles)
      return bundles

    return (pbegin
            | core.Impulse()
            | 'Split' >> core.FlatMap(split_source)
            | util.Reshuffle()
            | 'Read Bundles' >> beam.ParDo(_BigtableReadFn(project_id=beam_options['project_id'],
                                                           instance_id=beam_options['instance_id'],
                                                           table_id=beam_options['table_id'],
                                                           filter_=beam_options['filter_'])))
