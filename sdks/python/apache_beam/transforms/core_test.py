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

"""Unit tests for the core python file."""
# pytype: skip-file

import logging
import unittest

import apache_beam as beam


class TestDoFn1(beam.DoFn):
  def process(self, element):
    yield element


class TestDoFn2(beam.DoFn):
  def process(self, element):
    def inner_func(x):
      yield x

    return inner_func(element)


class TestDoFn3(beam.DoFn):
  """mixing return and yield is not allowed"""
  def process(self, element):
    if not element:
      return -1
    yield element


class CreateTest(unittest.TestCase):
  def test_dofn_with_yield_and_return(self):
    assert beam.ParDo(sum)
    assert beam.ParDo(TestDoFn1())
    assert beam.ParDo(TestDoFn2())
    with self.assertRaises(RuntimeError) as e:
      beam.ParDo(TestDoFn3())
    self.assertEqual(
        str(e.exception),
        'The yield and return statements in the process method '
        f'of {TestDoFn3().__class__} can not be mixed.')


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
