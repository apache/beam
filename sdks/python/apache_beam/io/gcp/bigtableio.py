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

# from collections import namedtuple
# from random import shuffle

import apache_beam as beam
from apache_beam.io import iobase
# from apache_beam.io.range_trackers import LexicographicKeyRangeTracker
from apache_beam.metrics import Metrics
from apache_beam.transforms.display import DisplayDataItem

try:
  from google.cloud.bigtable import Client
except ImportError:
  pass

__all__ = ['WriteToBigTable']


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


class BigtableSource(iobase.BoundedSource):
  def __init__(self, project_id, instance_id, table_id, filter_=None):
    """ Constructor of the Read connector of Bigtable

    Args:
      project_id: [string] GCP Project of to write the Rows
      instance_id: [string] GCP Instance to write the Rows
      table_id: [string] GCP Table to write the `DirectRows`
      filter_: [RowFilter] Filter to apply to columns in a row.
    """
    super(self.__class__, self).__init__()
    self._init({'project_id': project_id,
                'instance_id': instance_id,
                'table_id': table_id,
                'filter_': filter_})

  # def __getstate__(self):
  #   return self.beam_options
  #
  # def __setstate__(self, options):
  #   self._init(options)

  def _init(self, options):
    self.beam_options = options
    self.table = None
    self.sample_row_keys = None
    self.row_count = Metrics.counter(self.__class__.__name__, 'Row count')

  # def _get_table(self):
  #   if self.table is None:
  #     self.table = Client(project=self.beam_options['project_id'])\
  #                     .instance(self.beam_options['instance_id'])\
  #                     .table(self.beam_options['table_id'])
  #   return self.table
  #
  # def get_sample_row_keys(self):
  #   """ Get a sample of row keys in the table.
  #
  #   The returned row keys will delimit contiguous sections of the table of
  #   approximately equal size, which can be used to break up the data for
  #   distributed tasks like mapreduces.
  #   :returns: A cancel-able iterator. Can be consumed by calling ``next()``
  #   			  or by casting to a :class:`list` and can be cancelled by
  #   			  calling ``cancel()``.
  #
  #   ***** NOTE: For unclear reasons, the function returns generator even
  #   after wrapping the result as a list. In order to be used as a list, the
  #   result should be wrapped as a list AGAIN! E.g., see 'estimate_size()'
  #   """
  #   if self.sample_row_keys is None:
  #     self.sample_row_keys = list(self._get_table().sample_row_keys())
  #   return self.sample_row_keys
  #
  # def get_range_tracker(self, start_position=b'', stop_position=b''):
  #   if stop_position == b'':
  #     return LexicographicKeyRangeTracker(start_position)
  #   else:
  #     return LexicographicKeyRangeTracker(start_position, stop_position)
  #
  # def estimate_size(self):
  #   return list(self.get_sample_row_keys())[-1].offset_bytes
  #
  # def split(self, desired_bundle_size, start_position=None, stop_position=None):
  #   """ Splits the source into a set of bundles, using the row_set if it is set.
  #
  #   *** At this point, only splitting an entire table into samples based on the sample row keys is supported ***
  #
  #   :param desired_bundle_size: the desired size (in bytes) of the bundles returned.
  #   :param start_position: if specified, the position must be used as the starting position of the first bundle.
  #   :param stop_position: if specified, the position must be used as the ending position of the last bundle.
  #   Returns:
  #   	an iterator of objects of type 'SourceBundle' that gives information about the generated bundles.
  #   """
  #
  #   if start_position is not None or stop_position is not None:
  #     raise NotImplementedError
  #
  #   # TODO: Use the desired bundle size to split accordingly
  #   # TODO: Allow users to provide their own row sets
  #
  #   sample_row_keys = list(self.get_sample_row_keys())
  #
  #   if len(sample_row_keys) > 1 and sample_row_keys[0].row_key != b'':
  #     SampleRowKey = namedtuple("SampleRowKey", "row_key offset_bytes")
  #     first_key = SampleRowKey(b'', 0)
  #     sample_row_keys.insert(0, first_key)
  #     sample_row_keys = list(sample_row_keys)
  #
  #   bundles = []
  #   for i in range(1, len(sample_row_keys)):
  #     key_1 = sample_row_keys[i - 1].row_key
  #     key_2 = sample_row_keys[i].row_key
  #     size = sample_row_keys[i].offset_bytes - sample_row_keys[i - 1].offset_bytes
  #     bundles.append(iobase.SourceBundle(size, self, key_1, key_2))
  #
  #   # Shuffle is needed to allow reading from different locations of the table for better efficiency
  #   shuffle(bundles)
  #   return bundles
  #
  # def read(self, range_tracker):
  #   for row in self._get_table().read_rows(start_key=range_tracker.start_position(),
  #                                          end_key=range_tracker.stop_position(),
  #                                          filter_=self.beam_options['filter_']):
  #     if range_tracker.try_claim(row.row_key):
  #       self.row_count.inc()
  #       yield row
  #     else:
  #       # TODO: Modify the client ot be able to cancel read_row request
  #       break
  #
  # def display_data(self):
  #   return {'projectId': DisplayDataItem(self.beam_options['project_id'],
  #                                        label='Bigtable Project Id',
  #                                        key='projectId'),
  #           'instanceId': DisplayDataItem(self.beam_options['instance_id'],
  #                                         label='Bigtable Instance Id',
  #                                         key='instanceId'),
  #           'tableId': DisplayDataItem(self.beam_options['table_id'],
  #                                      label='Bigtable Table Id',
  #                                      key='tableId'),
  #           'filter_': DisplayDataItem(self.beam_options['filter_'],
  #                                      label='Bigtable Filter',
  #                                      key='filter_')
  #           }
