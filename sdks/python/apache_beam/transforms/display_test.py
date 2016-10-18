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

"""Unit tests for the DisplayData API."""

from __future__ import absolute_import

from datetime import datetime
import unittest

import apache_beam as beam
from apache_beam.transforms.display import HasDisplayData, DisplayData, DisplayDataItem


class DisplayDataTest(unittest.TestCase):

  def test_inheritance_ptransform(self):
    class MyTransform(beam.PTransform):
      pass

    display_pt = MyTransform()
    # PTransform inherits from HasDisplayData.
    self.assertTrue(isinstance(display_pt, HasDisplayData))
    self.assertEqual(display_pt.display_data(), {})

  def test_inheritance_dofn(self):
    class MyDoFn(beam.DoFn):
      pass

    display_dofn = MyDoFn()
    self.assertTrue(isinstance(display_dofn, HasDisplayData))
    self.assertEqual(display_dofn.display_data(), {})

  def test_base_cases(self):
    """ Tests basic display data cases (key:value, key:dict)
    It does not test subcomponent inclusion
    """
    class MyDoFn(beam.DoFn):
      def __init__(self, *args, **kwargs):
        self.my_display_data = kwargs.get('display_data', None)

      def process(self, context):
        yield context.element + 1

      def display_data(self):
        return {'static_integer': 120,
                'static_string': 'static me!',
                'complex_url': DisplayDataItem('github.com',
                                               url='http://github.com',
                                               label='The URL'),
                'python_class': HasDisplayData,
                'my_dd': self.my_display_data}

    now = datetime.now()
    fn = MyDoFn(display_data=now)
    dd = DisplayData.create_from(fn)
    dd_dicts = sorted([item.get_dict() for item in dd.items],
                      key=lambda x: x['namespace']+x['key'])

    nspace = '{}.{}'.format(fn.__module__, fn.__class__.__name__)
    expected_items = [
        {'url': 'http://github.com', 'namespace': nspace,
         'value': 'github.com', 'label': 'The URL',
         'key': 'complex_url', 'type': 'STRING'},
        {'type': 'TIMESTAMP', 'namespace': nspace, 'key': 'my_dd',
         'value': DisplayDataItem._format_value(now, 'TIMESTAMP')},
        {'type': 'STRING', 'namespace': nspace,
         'shortValue': 'HasDisplayData', 'key': 'python_class',
         'value': 'apache_beam.transforms.display.HasDisplayData'},
        {'type': 'INTEGER', 'namespace': nspace,
         'value': 120, 'key': 'static_integer'},
        {'type': 'STRING', 'namespace': nspace,
         'value': 'static me!', 'key': 'static_string'}]
    expected_items = sorted(expected_items,
                            key=lambda x: x['namespace']+x['key'])

    self.assertEqual(dd_dicts, expected_items)

  def test_subcomponent(self):
    class SpecialParDo(beam.ParDo):
      def __init__(self, fn):
        self.fn = fn

      def display_data(self):
        return {'asubcomponent': self.fn}

    class SpecialDoFn(beam.DoFn):
      def display_data(self):
        return {'dofn_value': 42}

    dofn = SpecialDoFn()
    pardo = SpecialParDo(dofn)
    dd = DisplayData.create_from(pardo)
    nspace = '{}.{}'.format(dofn.__module__, dofn.__class__.__name__)
    self.assertEqual(dd.items[0].get_dict(),
                     {"type": "INTEGER",
                      "namespace": nspace,
                      "value": 42,
                      "key": "dofn_value"})


# TODO: Test __repr__ function
# TODO: Test PATH when added by swegner@
if __name__ == '__main__':
  unittest.main()
