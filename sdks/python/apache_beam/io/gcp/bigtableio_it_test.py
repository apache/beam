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


"""Unittest for GCP Bigtable testing."""
from __future__ import absolute_import

import datetime
import time
import logging
import random
import string
import unittest

import pytz

import apache_beam as beam
from apache_beam.io import Read
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.runners.runner import PipelineState
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.transforms.combiners import Count

try:
  from google.cloud._helpers import _datetime_from_microseconds, _microseconds_from_datetime, UTC
  from google.cloud.bigtable import enums, row, column_family, Client
except ImportError:
  Client = None
  UTC = pytz.utc
  _microseconds_from_datetime = lambda label_stamp: label_stamp
  _datetime_from_microseconds = lambda micro: micro

import bigtableio

LABEL_KEY = u'python-bigtable-beam'
LABEL_STAMP_MICROSECONDS = _microseconds_from_datetime(datetime.datetime.utcnow().replace(tzinfo=UTC))
LABELS = {LABEL_KEY: str(LABEL_STAMP_MICROSECONDS)}

EXPERIMENTS = 'beam_fn_api'
PROJECT_ID = 'grass-clump-479'
INSTANCE_ID = 'python-write-2'
INSTANCE_TYPE = enums.Instance.Type.DEVELOPMENT
STORAGE_TYPE = enums.StorageType.HDD
REGION = 'us-central1'
# RUNNER = 'dataflow'
RUNNER = 'direct'
LOCATION_ID = "us-east1-b"
REQUIREMENTS_FILE = 'C:\\git\\beam_bigtable\\beam_bigtable_package\\requirements.txt'
SETUP_FILE = 'C:\\git\\beam_bigtable\\beam_bigtable_package\\setup.py'
EXTRA_PACKAGE = 'C:\\git\\beam_bigtable\\beam_bigtable_package\\dist\\beam_bigtable-0.3.117.tar.gz'
STAGING_LOCATION = 'gs://mf2199/stage'
TEMP_LOCATION = 'gs://mf2199/temp'
AUTOSCALING_ALGORITHM = 'NONE'
DISK_SIZE_GB = 50
NUM_WORKERS = 300
COLUMN_COUNT = 10
CELL_SIZE = 100
COLUMN_FAMILY_ID = 'cf1'

TIME_STAMP = datetime.datetime.fromtimestamp(time.time()).strftime('%Y%m%d-%H%M%S')
# TABLE_INFO = ('testmillion1c1d2c39', 781000)  # "good" reading table
TABLE_INFO = ('sample-table-10k', 10000)  # "good" reading table
# TABLE_INFO = ('sample-table-10k-1555901788505000', 10000) # "bad" reading table
# TABLE_INFO = ('sample-table-10k-20190422-193002', 10000) # "bad" reading table
# TABLE_ID = TABLE_INFO[0]
# ROW_COUNT = TABLE_INFO[1]

ROW_COUNT = 10000
# TABLE_ID = 'sample-table-{}k-{}'.format(ROW_COUNT / 1000, str(LABEL_STAMP_MICROSECONDS))
TABLE_ID = 'sample-table-{}k-{}'.format(ROW_COUNT / 1000, TIME_STAMP)
# TABLE_ID = 'sample-table-{}k'.format(ROW_COUNT / 1000)

PIPELINE_PARAMETERS = [
		'--experiments={}'.format(EXPERIMENTS),
		'--project={}'.format(PROJECT_ID),
		# '--instance={}'.format(INSTANCE_ID),
		'--job_name={}'.format(TABLE_ID),
		# '--requirements_file={}'.format(REQUIREMENTS_FILE),
		'--disk_size_gb={}'.format(DISK_SIZE_GB),
		'--region={}'.format(REGION),
		'--runner={}'.format(RUNNER),
		'--autoscaling_algorithm={}'.format(AUTOSCALING_ALGORITHM),
		'--num_workers={}'.format(NUM_WORKERS),
		# '--setup_file={}'.format(SETUP_FILE),
		# '--extra_package={}'.format(EXTRA_PACKAGE),
		'--staging_location={}'.format(STAGING_LOCATION),
		'--temp_location={}'.format(TEMP_LOCATION),
	]


class GenerateTestRows(beam.PTransform):
  """ A PTransform to generate dummy rows to write to a Bigtable Table.

  A PTransform that generates a list of `DirectRow` and writes it to a Bigtable Table.
  """
  def __init__(self, **kwargs):
    super(self.__class__, self).__init__()
    self.beam_options = {'project_id': PROJECT_ID,
                         'instance_id': INSTANCE_ID,
                         'table_id': TABLE_ID}

  def _generate(self):

    for index in range(ROW_COUNT):
      key = "key_%s" % ('{0:012}'.format(index))
      test_row = row.DirectRow(row_key=key)
      value = ''.join(random.choice(string.ascii_letters + string.digits) for _ in range(CELL_SIZE))
      for i in range(COLUMN_COUNT):
        test_row.set_cell(column_family_id=COLUMN_FAMILY_ID,
                          column=('field%s' % i).encode('utf-8'),
                          value=value,
                          timestamp=datetime.datetime.now())
      yield test_row

  def expand(self, pvalue):
    return (pvalue
            | beam.Create(self._generate())
            | bigtableio.WriteToBigTable(project_id=self.beam_options['project_id'],
                                         instance_id=self.beam_options['instance_id'],
                                         table_id=self.beam_options['table_id']))


@unittest.skipIf(Client is None, 'GCP Bigtable dependencies are not installed')
class BigtableIOWTest(unittest.TestCase):
  """ Bigtable IO Connector Test

  """
  def setUp(self):
    self.result = None
    self.table = Client(project=PROJECT_ID, admin=True)\
                    .instance(instance_id=INSTANCE_ID,
                              instance_type=INSTANCE_TYPE,
                              labels=LABELS)\
                    .table(TABLE_ID)

    if not self.table.exists():
      logging.info('Table {} does not exist.'.format(TABLE_ID))
      column_families = {COLUMN_FAMILY_ID: column_family.MaxVersionsGCRule(2)}
      self.table.create(column_families=column_families)
      if self.table.exists():
        logging.info('Table {} has been created.'.format(TABLE_ID))
      else:
        logging.info('Error creating table {}!'.format(TABLE_ID))

  def test_bigtable_io(self):
    print 'Project ID: ', PROJECT_ID
    print 'Instance ID:', INSTANCE_ID
    print 'Table ID:   ', TABLE_ID

    pipeline_options = PipelineOptions(PIPELINE_PARAMETERS)
    # pipeline_options.view_as(SetupOptions).save_main_session = True

    p = beam.Pipeline(options=pipeline_options)
    count = (p
             | 'Write Test Rows' >> GenerateTestRows()
             | 'Count' >> Count.Globally()
             )

    # assert_that(count, equal_to([ROW_COUNT]))

    self.result = p.run()
    self.result.wait_until_finish()

    assert self.result.state == PipelineState.DONE
    assert len([_ for _ in self.table.read_rows()]) == ROW_COUNT

    if not hasattr(self.result, 'has_job') or self.result.has_job:
      # query_result = self.result.metrics().query(MetricsFilter().with_name('Written Row'))
      query_result = self.result.metrics().query()
      if query_result['counters']:
        logging.info('WRITE counters have been found!')
        read_counter = query_result['counters'][0]
        logging.info('Number of Rows written: %d', read_counter.committed)
        assert read_counter.committed == ROW_COUNT
      else:
        logging.info('No WRITE counters were found!')

    logging.info('Write sequence is complete.')

    pipeline_options = PipelineOptions(PIPELINE_PARAMETERS)
    # q = beam.Pipeline(options=pipeline_options)
    # _ = (q
    #       | 'Read from Bigtable' >> Read(bigtableio.BigtableSource(project_id=PROJECT_ID,
    #                                                                instance_id=INSTANCE_ID,
    #                                                                table_id=TABLE_ID))
    #       # | 'Count Rows' >> Count.Globally()
    #      )
    # # assert_that(count, equal_to([ROW_COUNT]))
    # # # assert count == ROW_COUNT
    # self.result = q.run()
    # self.result.wait_until_finish()
    #
    # if not hasattr(self.result, 'has_job') or self.result.has_job:
    #   query_result = self.result.metrics().query(MetricsFilter().with_name('Row count'))
    #   if query_result['counters']:
    #     read_counter = query_result['counters'][0]
    #     logging.info('Number of Rows read: %d', read_counter.committed)
    #     # assert read_counter.committed == ROW_COUNT
    # else:
    #   logging.info("not hasattr(self.result, 'has_job') or self.result.has_job: condition was not satisfied!")


    # source = bigtableio.BigtableSource(project_id=PROJECT_ID,
    #                                    instance_id=INSTANCE_ID,
    #                                    table_id=TABLE_ID)
    q = beam.Pipeline(options=pipeline_options)
    count = (q
             | 'Read from Bigtable' >> Read(bigtableio.BigtableSource(project_id=PROJECT_ID,
                                                                      instance_id=INSTANCE_ID,
                                                                      table_id=TABLE_ID))
             | 'Count Rows' >> Count.Globally()
             )
    # assert_that(count, equal_to([ROW_COUNT]))
    self.result = q.run()
    self.result.wait_until_finish()

    counter_name = 'Row count'
    if not hasattr(self.result, 'has_job') or self.result.has_job:
      # query_result = self.result.metrics().query(MetricsFilter().with_name(counter_name))
      query_result = self.result.metrics().query()
      if query_result['counters']:
        # logging.info('Counter {} has been found!'.format(counter_name))
        logging.info('READ counters have been found!')
        # read_counter = query_result['counters'][0]
        # logging.info('Number of Rows written: %d', read_counter.committed)
        # assert read_counter.committed == ROW_COUNT
      else:
        # logging.info('Counter {} was NOT found!'.format(counter_name))
        logging.info('No READ counters were found!')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()

