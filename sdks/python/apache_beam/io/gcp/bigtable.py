
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

"""BigTable sources.
This module implements reading from BigTable tables. It relies
on several classes exposed by the BigTable API. The default mode is to return
table rows read from a BIgTable source as dictionaries. This is done for more
convenient programming. If desired, the native Table objects can be used
throughout to represent rows.

The syntax supported is described here:
https://cloud.google.com/bigtable/docs/quickstart-cbt
BigTable sources can be used as main inputs or side inputs.
A main input (common case) is expected to be massive and will be split into
manageable chunks and processed in parallel. Side inputs are expected to be small
and will be read completely every time a ParDo DoFn gets executed.
In the example below the
lambda function implementing the DoFn for the Map transform will get on each
call *one* row of the main table and *all* rows of the side table. The runner
may use some caching techniques to share the side inputs between calls in order
to avoid excessive reading:::
  config = BigtableReadConfiguration(project_id, instance_id, table_id)
  main_table = pipeline | 'read' >> beam.io.Read(ReadFromBigtable(config))
  results = (
      main_table
      | 'ProcessData' >> beam.Map(
          lambda element, side_input: ..., AsList(side_table)))
There is no difference in how main and side inputs are read. What makes the
side_table a 'side input' is the AsList wrapper used when passing the table
as a parameter to the Map transform. AsList signals to the execution framework
that its input should be made available whole.
The main and side inputs are implemented differently. Reading a BigTable table
as main input entails exporting the table to a set of GCS files (currently in
JSON format) and then processing those files. Reading the same table as a side
input entails querying the table for all its rows. The coder argument on
BigQuerySource controls the reading of the lines in the export files (i.e.,
transform a JSON object into a PCollection element). The coder is not involved
when the same table is read as a side input since there is no intermediate
format involved. We get the table rows directly from the BigQuery service with
a query.
"""

import logging
import apache_beam as beam
from google.cloud import bigtable
from apache_beam.io import iobase
from apache_beam.metrics import Metrics
from apache_beam.io.iobase import SourceBundle
from apache_beam.transforms.display import DisplayDataItem
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.range_trackers import LexicographicKeyRangeTracker


class ReadBigtableOptions(PipelineOptions):
	""" Create the Pipeline Options to set ReadBigtable/WriteBigtable.
	You can create and use this class in the Template, with a certainly steps.
	"""
	@classmethod
	def _add_argparse_args(cls, parser):
		super(ReadBigtableOptions, cls)._add_argparse_args(parser)
		parser.add_argument('--instance', required=True )
		parser.add_argument('--table', required=True )


class ReadFromBigtable(iobase.BoundedSource):
	""" Bigtable apache beam read source
	:type split_keys: dict
						'~end_key'}`.
	:type beam_options:
	class:`~bigtable_configuration.BigtableReadConfiguration`
	:param beam_options: Class
	`~bigtable_configuration.BigtableReadConfiguration`.
	"""
	def __init__(self, beam_options):
		super(ReadFromBigtable, self).__init__()
		self.beam_options = beam_options
		self.table = None
		self.read_row = Metrics.counter(self.__class__, 'read')
	def _getTable(self):
		if self.table is None:
			options = self.beam_options
			client = bigtable.Client(
				project=options.project_id,
				credentials=self.beam_options.credentials)
			instance = client.instance(options.instance_id)
			self.table = instance.table(options.table_id)
		return self.table

	def __getstate__(self):
		return self.beam_options

	def __setstate__(self, options):
		self.beam_options = options
		self.table = None
		self.read_row = Metrics.counter(self.__class__, 'read')

	def estimate_size(self):
		size = [k.offset_bytes for k in self._getTable().sample_row_keys()][-1]
		return size

	def split(self, desired_bundle_size, start_position=None, stop_position=None):
		sample_row_keys = self._getTable().sample_row_keys()

		if self.beam_options.row_set is not None:
			for sample_row_key in self.beam_options.row_set.row_ranges:
				yield self.range_split_fraction(1, desired_bundle_size, sample_row_key.start_key, sample_row_key.end_key)
		else:
			suma = 0
			last_offset = 0
			current_size = 0

			start_key = b''
			end_key = b''
			
			for sample_row_key in sample_row_keys:
				current_size = sample_row_key.offset_bytes-last_offset
				if suma >= desired_bundle_size:
					end_key = sample_row_key.row_key
					yield self.range_split_fraction(suma, desired_bundle_size, start_key, end_key)
					start_key = sample_row_key.row_key

					suma = 0
				suma += current_size
				last_offset = sample_row_key.offset_bytes

	def split_key_range_into_bundle_sized_sub_ranges(self, sample_size_bytes, desired_bundle_size, ranges):
		last_key = copy.deepcopy(ranges.stop_position())
		s = ranges.start_position()
		e = ranges.stop_position()

		split_ = float(desired_bundle_size) / float(sample_size_bytes)
		split_count = int( math.ceil( sample_size_bytes / desired_bundle_size ) )

		for i in range(split_count):
			estimate_position = ((i+1) * split_)
			position = LexicographicKeyRangeTracker.fraction_to_position(estimate_position, ranges.start_position(), ranges.stop_position())
			e = position
			yield iobase.SourceBundle(sample_size_bytes * split_, self, s, e)
			s = position
		yield iobase.SourceBundle(sample_size_bytes * split_, self, s, last_key )

	def get_range_tracker(self, start_position, stop_position):
		return LexicographicKeyRangeTracker(start_position, stop_position)

	def read(self, range_tracker):
		if not (range_tracker.start_position() == None):
			if not range_tracker.try_claim(range_tracker.start_position()):
				# there needs to be a way to cancel the request.
				return
		read_rows = self._getTable().read_rows(start_key=range_tracker.start_position(),
			end_key=range_tracker.stop_position(),
			filter_=self.beam_options.filter_)

		for row in read_rows:
			self.read_row.inc()
			yield row

	def display_data(self):
		ret = {
			'projectId': DisplayDataItem(self.beam_options.project_id, label='Bigtable Project Id', key='projectId'),
			'instanceId': DisplayDataItem(self.beam_options.instance_id, label='Bigtable Instance Id',key='instanceId'),
			'tableId': DisplayDataItem(self.beam_options.table_id, label='Bigtable Table Id', key='tableId'),
		}
		if self.beam_options.row_set is not None:
			i = 0
			for value in self.beam_options.row_set.row_keys:
				ret['rowSet{}'.format(i)] = DisplayDataItem(str(value), label='Bigtable Row Set {}'.format(i), key='rowSet{}'.format(i))
				i = i+1
			for (i,value) in enumerate(self.beam_options.row_set.row_ranges):
				ret['rowSet{}'.format(i)] = DisplayDataItem(str(value.get_range_kwargs()), label='Bigtable Row Set {}'.format(i), key='rowSet{}'.format(i))
				i = i+1
		if self.beam_options.filter_ is not None:
			for (i,value) in enumerate(self.beam_options.filter_.filters):
				ret['rowFilter{}'.format(i)] = DisplayDataItem(str(value.to_pb()), label='Bigtable Row Filter {}'.format(i), key='rowFilter{}'.format(i))
		return ret


class BigtableConfiguration(object):
	""" Bigtable configuration variables.

	:type project_id: :class:`str` or :func:`unicode <unicode>`
	:param project_id: (Optional) The ID of the project which owns the
						instances, tables and data. If not provided, will
						attempt to determine from the environment.

	:type instance_id: str
	:param instance_id: The ID of the instance.

	:type table_id: str
	:param table_id: The ID of the table.
	"""

	def __init__(self, project_id, instance_id, table_id):
		self.project_id = project_id
		self.instance_id = instance_id
		self.table_id = table_id
		self.credentials = None


class BigtableReadConfiguration(BigtableConfiguration):
	""" Bigtable read configuration variables.

	:type configuration: :class:`~BigtableConfiguration`
	:param configuration: class:`~BigtableConfiguration`

	:type row_set: :class:`row_set.RowSet`
	:param row_set: (Optional) The row set containing multiple row keys and
					row_ranges.

	:type filter_: :class:`.RowFilter`
	:param filter_: (Optional) The filter to apply to the contents of the
					specified row(s). If unset, reads every column in
					each row.
	"""

	def __init__(self, project_id, instance_id, table_id, row_set=None, filter_=None):
		super(BigtableReadConfiguration, self).__init__(project_id, instance_id, table_id)
		self.row_set = row_set
		self.filter_ = filter_
	def __str__(self):
		import json
		row_set = []
		filters = ""
		if self.filter_ is not None:
			filters = str(self.filter_.to_pb())
		if self.row_set is not None:
			row_set = self.row_set.row_keys
			for r in self.row_set.row_ranges:
				row_set.append(r.get_range_kwargs())
		return json.dumps({
			'project_id': self.project_id,
			'instance_id': self.instance_id,
			'table_id': self.table_id,
			'row_set': row_set,
			'filter_': filters
		})
