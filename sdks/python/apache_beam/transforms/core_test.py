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
import pytest
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


class TestDoFn4(beam.DoFn):
  """test the variable name containing return"""
  def process(self, element):
    my_return = element
    yield my_return


class TestDoFn5(beam.DoFn):
  """test the variable name containing yield"""
  def process(self, element):
    my_yield = element
    return my_yield


class CreateTest(unittest.TestCase):
  @pytest.fixture(autouse=True)
  def inject_fixtures(self, caplog):
    self._caplog = caplog

  def test_dofn_with_yield_and_return(self):
    assert beam.ParDo(sum)
    assert beam.ParDo(TestDoFn1())
    assert beam.ParDo(TestDoFn2())
    assert beam.ParDo(TestDoFn4())
    assert beam.ParDo(TestDoFn5())
    with self._caplog.at_level(logging.WARNING):
      beam.ParDo(TestDoFn3())
      assert 'The yield and return statements in' in self._caplog.text


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
