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

"""Tests corresponding to Dataflow's iobase module."""

from __future__ import absolute_import

import unittest

from apache_beam import Create
from apache_beam import error
from apache_beam import pvalue
from apache_beam.runners.dataflow.native_io.iobase import ConcatPosition
from apache_beam.runners.dataflow.native_io.iobase import DynamicSplitRequest
from apache_beam.runners.dataflow.native_io.iobase import DynamicSplitResultWithPosition
from apache_beam.runners.dataflow.native_io.iobase import NativeSink
from apache_beam.runners.dataflow.native_io.iobase import NativeSinkWriter
from apache_beam.runners.dataflow.native_io.iobase import NativeSource
from apache_beam.runners.dataflow.native_io.iobase import ReaderPosition
from apache_beam.runners.dataflow.native_io.iobase import ReaderProgress
from apache_beam.runners.dataflow.native_io.iobase import _dict_printable_fields
from apache_beam.runners.dataflow.native_io.iobase import _NativeWrite
from apache_beam.testing.test_pipeline import TestPipeline


class TestHelperFunctions(unittest.TestCase):

  def test_dict_printable_fields(self):
    dict_object = {
        'key_alpha': '1',
        'key_beta': None,
        'key_charlie': [],
        'key_delta': 2.0,
        'key_echo': 'skip_me',
        'key_fox': 0
    }
    skip_fields = [
        'key_echo',
    ]
    self.assertEqual(
        sorted(_dict_printable_fields(dict_object, skip_fields)),
        [
            "key_alpha='1'",
            'key_delta=2.0',
            'key_fox=0'
        ]
    )


class TestNativeSource(unittest.TestCase):

  def test_reader_method(self):
    native_source = NativeSource()
    self.assertRaises(NotImplementedError, native_source.reader)

  def test_repr_method(self):
    class FakeSource(NativeSource):
      """A fake source modeled after BigQuerySource, which inherits from
      NativeSource."""

      def __init__(self, table=None, dataset=None, project=None, query=None,
                   validate=False, coder=None, use_std_sql=False,
                   flatten_results=True):
        self.validate = validate

    fake_source = FakeSource()
    self.assertEqual(fake_source.__repr__(), '<FakeSource validate=False>')


class TestReaderProgress(unittest.TestCase):

  def test_out_of_bounds_percent_complete(self):
    with self.assertRaises(ValueError):
      ReaderProgress(percent_complete=-0.1)
    with self.assertRaises(ValueError):
      ReaderProgress(percent_complete=1.1)

  def test_position_property(self):
    reader_progress = ReaderProgress(position=ReaderPosition())
    self.assertEqual(type(reader_progress.position), ReaderPosition)

  def test_percent_complete_property(self):
    reader_progress = ReaderProgress(percent_complete=0.5)
    self.assertEqual(reader_progress.percent_complete, 0.5)


class TestReaderPosition(unittest.TestCase):

  def test_invalid_concat_position_type(self):
    with self.assertRaises(AssertionError):
      ReaderPosition(concat_position=1)

  def test_valid_concat_position_type(self):
    ReaderPosition(concat_position=ConcatPosition(None, None))


class TestConcatPosition(unittest.TestCase):

  def test_invalid_position_type(self):
    with self.assertRaises(AssertionError):
      ConcatPosition(None, position=1)

  def test_valid_position_type(self):
    ConcatPosition(None, position=ReaderPosition())


class TestDynamicSplitRequest(unittest.TestCase):

  def test_invalid_progress_type(self):
    with self.assertRaises(AssertionError):
      DynamicSplitRequest(progress=1)

  def test_valid_progress_type(self):
    DynamicSplitRequest(progress=ReaderProgress())


class TestDynamicSplitResultWithPosition(unittest.TestCase):

  def test_invalid_stop_position_type(self):
    with self.assertRaises(AssertionError):
      DynamicSplitResultWithPosition(stop_position=1)

  def test_valid_stop_position_type(self):
    DynamicSplitResultWithPosition(stop_position=ReaderPosition())


class TestNativeSink(unittest.TestCase):

  def test_writer_method(self):
    native_sink = NativeSink()
    self.assertRaises(NotImplementedError, native_sink.writer)

  def test_repr_method(self):
    class FakeSink(NativeSink):
      """A fake sink modeled after BigQuerySink, which inherits from
      NativeSink."""

      def __init__(self, validate=False, dataset=None, project=None,
                   schema=None, create_disposition='create',
                   write_disposition=None, coder=None):
        self.validate = validate

    fake_sink = FakeSink()
    self.assertEqual(fake_sink.__repr__(), "<FakeSink ['validate=False']>")

  def test_on_direct_runner(self):
    class FakeSink(NativeSink):
      """A fake sink outputing a number of elements."""

      def __init__(self):
        self.written_values = []
        self.writer_instance = FakeSinkWriter(self.written_values)

      def writer(self):
        return self.writer_instance

    class FakeSinkWriter(NativeSinkWriter):
      """A fake sink writer for testing."""

      def __init__(self, written_values):
        self.written_values = written_values

      def __enter__(self):
        return self

      def __exit__(self, *unused_args):
        pass

      def Write(self, value):
        self.written_values.append(value)

    p = TestPipeline()
    sink = FakeSink()
    p | Create(['a', 'b', 'c']) | _NativeWrite(sink)  # pylint: disable=expression-not-assigned
    p.run()

    self.assertEqual(['a', 'b', 'c'], sink.written_values)


class Test_NativeWrite(unittest.TestCase):

  def setUp(self):
    self.native_sink = NativeSink()
    self.native_write = _NativeWrite(self.native_sink)

  def test_expand_method_pcollection_errors(self):
    with self.assertRaises(error.TransformError):
      self.native_write.expand(None)
    with self.assertRaises(error.TransformError):
      pcoll = pvalue.PCollection(pipeline=None)
      self.native_write.expand(pcoll)


if __name__ == '__main__':
  unittest.main()
