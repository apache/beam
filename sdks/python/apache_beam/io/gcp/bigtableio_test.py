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

"""Unit tests for BigTable service."""

# pytype: skip-file
import datetime
import string
import unittest
from random import random

import apache_beam as beam
from apache_beam.io.gcp.bigtableio import WriteToBigTable
from apache_beam.testing.test_pipeline import TestPipeline

try:
  from google.cloud.bigtable import row
except ImportError:
  row = None


class TestWriteBigTable(unittest.TestCase):
  def WriteBigTable(self):
    with TestPipeline() as p:
      result = (p | beam.Create([self.valid_pubsub_string]) | WriteToBigTable())
      self.assertEqual(result, True)


EXISTING_INSTANCES = []  # type: List[google.cloud.bigtable.instance.Instance]
LABEL_KEY = u'python-bigtable-beam'
LABELS = {LABEL_KEY: LABEL_KEY}


class GenerateTestRows(beam.PTransform):
  """ A transform test to run write to the Bigtable Table.

  A PTransform that generate a list of `DirectRow` to write it in
  Bigtable Table.

  """
  def __init__(self, number, project_id=None, instance_id=None, table_id=None):
    beam.PTransform.__init__(self)
    self.number = number
    self.rand = random.choice(string.ascii_letters + string.digits)
    self.column_family_id = 'cf1'
    self.beam_options = {
        'project_id': project_id,
        'instance_id': instance_id,
        'table_id': table_id
    }

  def _generate(self):
    value = ''.join(self.rand for i in range(100))

    for index in range(self.number):
      key = "beam_key%s" % ('{0:07}'.format(index))
      direct_row = row.DirectRow(row_key=key)
      for column_id in range(10):
        direct_row.set_cell(
            self.column_family_id, ('field%s' % column_id).encode('utf-8'),
            value,
            datetime.datetime.now())
      yield direct_row

  def expand(self, pvalue):
    beam_options = self.beam_options
    return (
        pvalue
        | beam.Create(self._generate())
        | WriteToBigTable(
            beam_options['project_id'],
            beam_options['instance_id'],
            beam_options['table_id']))


if __name__ == '__main__':
  unittest.main()
