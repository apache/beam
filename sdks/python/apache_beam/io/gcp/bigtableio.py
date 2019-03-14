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
from __future__ import division

import math

import apache_beam as beam
from apache_beam.io import iobase
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker
from apache_beam.metrics import Metrics
from apache_beam.transforms.display import DisplayDataItem

try:
  from google.cloud.bigtable import Client
except ImportError:
  pass

__all__ = ['WriteToBigTable', 'ReadFromBigTable']


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
    return row

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


class _BigTableSource(iobase.BoundedSource):
  """ Creates the connector to get the rows in Bigtable and using each
  row in beam pipe line
  Args:
    project_id(str): GCP Project ID
    instance_id(str): GCP Instance ID
    table_id(str): GCP Table ID
    row_set(RowSet): Creating a set of row keys and row ranges.
    filter_(RowFilter): Filter to apply to cells in a row.

  """
  def __init__(self, project_id, instance_id, table_id,
               row_set=None, filter_=None):
    """ Constructor of the Read connector of Bigtable
    Args:
      project_id(str): GCP Project of to write the Rows
      instance_id(str): GCP Instance to write the Rows
      table_id(str): GCP Table to write the `DirectRows`
      row_set(RowSet): This variable represents the RowRanges
      you want to use, It used on the split, to set the split
      only in that ranges.
      filter_(RowFilter): Get some expected rows, bases on
      certainly information in the row.
    """
    super(self.__class__, self).__init__()
    self.beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id,
                         'row_set': row_set,
                         'filter_': filter_}
    self.table = None
    self.read_row = Metrics.counter(self.__class__.__name__, 'read_row')

  def __getstate__(self):
    return self.beam_options

  def __setstate__(self, options):
    self.beam_options = {'project_id': options['project_id'],
                         'instance_id': options['instance_id'],
                         'table_id': options['table_id'],
                         'row_set': options['row_set'],
                         'filter_': options['filter_']}

  def _getTable(self):
    if self.table is None:
      client = Client(project=self.beam_options['project_id'])
      instance = client.instance(self.beam_options['instance_id'])
      self.table = instance.table(self.beam_options['table_id'])
    return self.table

  def estimate_size(self):
    size = [k.offset_bytes for k in self.get_sample_row_keys()][-1]
    return size

  def get_sample_row_keys(self):
    ''' Get a sample of row keys in the table.

    The returned row keys will delimit contiguous sections of the table of
    approximately equal size, which can be used to break up the data for
    distributed tasks like mapreduces.

    :returns: A cancel-able iterator. Can be consumed by calling ``next()``
                  or by casting to a :class:`list` and can be cancelled by
                  calling ``cancel()``.
    '''
    return self._getTable().sample_row_keys()

  def get_range_tracker(self, start_position, stop_position):
    return LexicographicKeyRangeTracker(start_position, stop_position)

  def split(self, desired_bundle_size, start_position=None, stop_position=None):
    ''' Splits the source into a set of bundles, using the row_set if it is
    neccessary.
    Bundles should be approximately of ``desired_bundle_size`` bytes, if this
    bundle its bigger, it use the ``range_split_fraction`` to split the bundles
    in fractions.

    :param desired_bundle_size: the desired size (in bytes) of the bundles
    returned.
    :param start_position: if specified the given position must be used as
    the starting position of the first bundle.
    :param stop_position: if specified the given position must be used as
    the ending position of the last bundle.

    Returns:
      an iterator of objects of type 'SourceBundle' that gives information
      about the generated bundles.
    '''

    # The row_set is variable to get only certain ranges of rows, this
    # variable is set in the constructor of this class.
    if self.beam_options['row_set'] is not None:
      for sample_row_key in self.beam_options['row_set'].row_ranges:
        for row_split in self.split_range_size(desired_bundle_size,
                                               self.get_sample_row_keys(),
                                               sample_row_key):
          yield row_split
    else:
      addition_size = 0
      last_offset = 0
      current_size = 0

      start_key = b''
      end_key = b''

      for sample_row_key in self.get_sample_row_keys():
        current_size = sample_row_key.offset_bytes - last_offset
        addition_size += current_size
        if addition_size >= desired_bundle_size:
          end_key = sample_row_key.row_key
          for fraction in self.range_split_fraction(addition_size,
                                                    desired_bundle_size,
                                                    start_key, end_key):
            yield fraction
          start_key = sample_row_key.row_key
          addition_size = 0
        last_offset = sample_row_key.offset_bytes

      full_size = 10468982784
      if current_size == full_size:
        last_bundle_size = full_size
      else:
        last_bundle_size = full_size - current_size
      yield iobase.SourceBundle(last_bundle_size, self, start_key, end_key)

  def split_range_size(self, desired_size, sample_row_keys, range_):
    ''' This method split the row_set ranges using the desired_bundle_size
    you get.
    :param desired_size(int):  The size you need to split the ranges.
    :param sample_row_keys(list): A list of row keys with a end size.
    :param range_: A Row Range Element, to split if it is necessary.
    '''

    start = None
    end = None
    last_offset = 0
    for sample_row in sample_row_keys:
      current = sample_row.offset_bytes - last_offset

      # if the sample_row is the first or last element in the sample_row
      # keys parameter, it avoids this sample row element.
      if sample_row.row_key == b'':
        continue

      if(range_.start_key <= sample_row.row_key and
         range_.end_key >= sample_row.row_key):
        if start is not None:
          end = sample_row.row_key

          range_tracker = LexicographicKeyRangeTracker(start, end)

          for split_key_range in self.split_range_subranges(current,
                                                            desired_size,
                                                            range_tracker):
            yield split_key_range
        start = sample_row.row_key
      last_offset = sample_row.offset_bytes

  def range_split_fraction(self,
                           current_size,
                           desired_bundle_size,
                           start_key,
                           end_key):
    ''' This method is used to send a range[start_key, end_key) to the
    ``split_range_subranges`` method.

    :param current_size: the size of the range.
    :param desired_bundle_size: the size you want to split.
    :param start_key(byte): The start key row in the range.
    :param end_key(byte): The end key row in the range.
    '''
    range_tracker = LexicographicKeyRangeTracker(start_key, end_key)
    return self.split_range_subranges(current_size,
                                      desired_bundle_size,
                                      range_tracker)

  def fraction_to_position(self, position, range_start, range_stop):
    ''' We use the ``fraction_to_position`` method in
    ``LexicographicKeyRangeTracker`` class to split a
    range into two chunks.

    :param position:
    :param range_start:
    :param range_stop:
    :return:
    '''
    return LexicographicKeyRangeTracker.fraction_to_position(position,
                                                             range_start,
                                                             range_stop)

  def split_range_subranges(self,
                            sample_size_bytes,
                            desired_bundle_size,
                            ranges):
    ''' This method split the range you get using the
    ``desired_bundle_size`` as a limit size, It compares the
    size of the range and the ``desired_bundle size`` if it is necessary
    to split a range, it uses the ``fraction_to_position`` method.

    :param sample_size_bytes: The size of the Range.
    :param desired_bundle_size: The desired size to split the Range.
    :param ranges: the Range to split.
    '''
    start_key = ranges.start_position()
    end_key = ranges.stop_position()

    split_ = float(desired_bundle_size)/float(sample_size_bytes)
    split_count = int(math.ceil(split_))
    portions = (1/split_)
    if portions > 1:
      for i in range(split_count):
        estimate_position = ((i + 1) * split_)

        for i in range(int(portions)):
          temp_estimate = estimate_position*(i+1)
          position = self.fraction_to_position(temp_estimate,
                                               ranges.start_position(),
                                               ranges.stop_position())
          end_key = position
          yield iobase.SourceBundle(sample_size_bytes * split_,
                                    self,
                                    start_key,
                                    end_key)
          start_key = position
    else:
      yield iobase.SourceBundle(sample_size_bytes, self, start_key, end_key)

  def read(self, range_tracker):
    filter_ = self.beam_options['filter_']
    table = self._getTable()
    read_rows = table.read_rows(start_key=range_tracker.start_position(),
                                end_key=range_tracker.stop_position(),
                                filter_=filter_)

    for row in read_rows:
      if range_tracker.stop_position() != b'':
        if range_tracker.try_claim(row.row_key):
          return
      self.read_row.inc()
      yield row

  def display_data(self):
    ret = {'projectId': DisplayDataItem(self.beam_options['project_id'],
                                        label='Bigtable Project Id',
                                        key='projectId'),
           'instanceId': DisplayDataItem(self.beam_options['instance_id'],
                                         label='Bigtable Instance Id',
                                         key='instanceId'),
           'tableId': DisplayDataItem(self.beam_options['table_id'],
                                      label='Bigtable Table Id',
                                      key='tableId')}
    return ret


class ReadFromBigTable(beam.PTransform):
  def __init__(self, project_id, instance_id, table_id):
    self.beam_options = {'project_id': project_id,
                         'instance_id': instance_id,
                         'table_id': table_id}

  def expand(self, pvalue):
    project_id = self.beam_options['project_id']
    instance_id = self.beam_options['instance_id']
    table_id = self.beam_options['table_id']
    return (pvalue
            | 'ReadFromBigtable' >> beam.io.Read(_BigTableSource(project_id,
                                                                 instance_id,
                                                                 table_id)))
