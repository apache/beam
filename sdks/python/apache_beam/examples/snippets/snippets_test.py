# coding=utf-8
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

"""Tests for all code snippets used in public docs."""
from __future__ import absolute_import
from __future__ import division

import glob
import gzip
import logging
import os
import sys
import tempfile
import unittest
import uuid
from builtins import map
from builtins import object
from builtins import range
from builtins import zip

import mock

import apache_beam as beam
from apache_beam import WindowInto
from apache_beam import coders
from apache_beam import pvalue
from apache_beam import typehints
from apache_beam.coders.coders import ToStringCoder
from apache_beam.examples.snippets import snippets
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.test_stream import TestStream
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.transforms.trigger import AccumulationMode
from apache_beam.transforms.trigger import AfterAny
from apache_beam.transforms.trigger import AfterCount
from apache_beam.transforms.trigger import AfterProcessingTime
from apache_beam.transforms.trigger import AfterWatermark
from apache_beam.transforms.trigger import Repeatedly
from apache_beam.transforms.window import FixedWindows
from apache_beam.transforms.window import TimestampedValue
from apache_beam.utils.windowed_value import WindowedValue

# Protect against environments where apitools library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from apitools.base.py import base_api
except ImportError:
  base_api = None
# pylint: enable=wrong-import-order, wrong-import-position

# Protect against environments where datastore library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.cloud.proto.datastore.v1 import datastore_pb2
except ImportError:
  datastore_pb2 = None
# pylint: enable=wrong-import-order, wrong-import-position

# Protect against environments where the PubSub library is not available.
# pylint: disable=wrong-import-order, wrong-import-position
try:
  from google.cloud import pubsub
except ImportError:
  pubsub = None
# pylint: enable=wrong-import-order, wrong-import-position


class ParDoTest(unittest.TestCase):
  """Tests for model/par-do."""

  def test_pardo(self):
    # Note: "words" and "ComputeWordLengthFn" are referenced by name in
    # the text of the doc.

    words = ['aa', 'bbb', 'c']

    # [START model_pardo_pardo]
    class ComputeWordLengthFn(beam.DoFn):
      def process(self, element):
        return [len(element)]
    # [END model_pardo_pardo]

    # [START model_pardo_apply]
    # Apply a ParDo to the PCollection "words" to compute lengths for each word.
    word_lengths = words | beam.ParDo(ComputeWordLengthFn())
    # [END model_pardo_apply]
    self.assertEqual({2, 3, 1}, set(word_lengths))

  def test_pardo_yield(self):
    words = ['aa', 'bbb', 'c']

    # [START model_pardo_yield]
    class ComputeWordLengthFn(beam.DoFn):
      def process(self, element):
        yield len(element)
    # [END model_pardo_yield]

    word_lengths = words | beam.ParDo(ComputeWordLengthFn())
    self.assertEqual({2, 3, 1}, set(word_lengths))

  def test_pardo_using_map(self):
    words = ['aa', 'bbb', 'c']
    # [START model_pardo_using_map]
    word_lengths = words | beam.Map(len)
    # [END model_pardo_using_map]

    self.assertEqual({2, 3, 1}, set(word_lengths))

  def test_pardo_using_flatmap(self):
    words = ['aa', 'bbb', 'c']
    # [START model_pardo_using_flatmap]
    word_lengths = words | beam.FlatMap(lambda word: [len(word)])
    # [END model_pardo_using_flatmap]

    self.assertEqual({2, 3, 1}, set(word_lengths))

  def test_pardo_using_flatmap_yield(self):
    words = ['aA', 'bbb', 'C']

    # [START model_pardo_using_flatmap_yield]
    def capitals(word):
      for letter in word:
        if 'A' <= letter <= 'Z':
          yield letter
    all_capitals = words | beam.FlatMap(capitals)
    # [END model_pardo_using_flatmap_yield]

    self.assertEqual({'A', 'C'}, set(all_capitals))

  def test_pardo_with_label(self):
    words = ['aa', 'bbc', 'defg']
    # [START model_pardo_with_label]
    result = words | 'CountUniqueLetters' >> beam.Map(
        lambda word: len(set(word)))
    # [END model_pardo_with_label]

    self.assertEqual({1, 2, 4}, set(result))

  def test_pardo_side_input(self):
    # pylint: disable=line-too-long
    with TestPipeline() as p:
      words = p | 'start' >> beam.Create(['a', 'bb', 'ccc', 'dddd'])

      # [START model_pardo_side_input]
      # Callable takes additional arguments.
      def filter_using_length(word, lower_bound, upper_bound=float('inf')):
        if lower_bound <= len(word) <= upper_bound:
          yield word

      # Construct a deferred side input.
      avg_word_len = (words
                      | beam.Map(len)
                      | beam.CombineGlobally(beam.combiners.MeanCombineFn()))

      # Call with explicit side inputs.
      small_words = words | 'small' >> beam.FlatMap(filter_using_length, 0, 3)

      # A single deferred side input.
      larger_than_average = (words | 'large' >> beam.FlatMap(
          filter_using_length,
          lower_bound=pvalue.AsSingleton(avg_word_len)))

      # Mix and match.
      small_but_nontrivial = words | beam.FlatMap(
          filter_using_length,
          lower_bound=2,
          upper_bound=pvalue.AsSingleton(avg_word_len))
      # [END model_pardo_side_input]

      assert_that(small_words, equal_to(['a', 'bb', 'ccc']))
      assert_that(larger_than_average, equal_to(['ccc', 'dddd']),
                  label='larger_than_average')
      assert_that(small_but_nontrivial, equal_to(['bb']),
                  label='small_but_not_trivial')

  def test_pardo_side_input_dofn(self):
    words = ['a', 'bb', 'ccc', 'dddd']

    # [START model_pardo_side_input_dofn]
    class FilterUsingLength(beam.DoFn):
      def process(self, element, lower_bound, upper_bound=float('inf')):
        if lower_bound <= len(element) <= upper_bound:
          yield element

    small_words = words | beam.ParDo(FilterUsingLength(), 0, 3)
    # [END model_pardo_side_input_dofn]
    self.assertEqual({'a', 'bb', 'ccc'}, set(small_words))

  def test_pardo_with_tagged_outputs(self):
    # [START model_pardo_emitting_values_on_tagged_outputs]
    class ProcessWords(beam.DoFn):

      def process(self, element, cutoff_length, marker):
        if len(element) <= cutoff_length:
          # Emit this short word to the main output.
          yield element
        else:
          # Emit this word's long length to the 'above_cutoff_lengths' output.
          yield pvalue.TaggedOutput(
              'above_cutoff_lengths', len(element))
        if element.startswith(marker):
          # Emit this word to a different output with the 'marked strings' tag.
          yield pvalue.TaggedOutput('marked strings', element)
    # [END model_pardo_emitting_values_on_tagged_outputs]

    words = ['a', 'an', 'the', 'music', 'xyz']

    # [START model_pardo_with_tagged_outputs]
    results = (words | beam.ParDo(ProcessWords(), cutoff_length=2, marker='x')
               .with_outputs('above_cutoff_lengths', 'marked strings',
                             main='below_cutoff_strings'))
    below = results.below_cutoff_strings
    above = results.above_cutoff_lengths
    marked = results['marked strings']  # indexing works as well
    # [END model_pardo_with_tagged_outputs]

    self.assertEqual({'a', 'an'}, set(below))
    self.assertEqual({3, 5}, set(above))
    self.assertEqual({'xyz'}, set(marked))

    # [START model_pardo_with_tagged_outputs_iter]
    below, above, marked = (words
                            | beam.ParDo(
                                ProcessWords(), cutoff_length=2, marker='x')
                            .with_outputs('above_cutoff_lengths',
                                          'marked strings',
                                          main='below_cutoff_strings'))
    # [END model_pardo_with_tagged_outputs_iter]

    self.assertEqual({'a', 'an'}, set(below))
    self.assertEqual({3, 5}, set(above))
    self.assertEqual({'xyz'}, set(marked))

  def test_pardo_with_undeclared_outputs(self):
    # Note: the use of undeclared outputs is currently not supported in eager
    # execution mode.
    with TestPipeline() as p:
      numbers = p | beam.Create([1, 2, 3, 4, 5, 10, 20])

      # [START model_pardo_with_undeclared_outputs]
      def even_odd(x):
        yield pvalue.TaggedOutput('odd' if x % 2 else 'even', x)
        if x % 10 == 0:
          yield x

      results = numbers | beam.FlatMap(even_odd).with_outputs()

      evens = results.even
      odds = results.odd
      tens = results[None]  # the undeclared main output
      # [END model_pardo_with_undeclared_outputs]

      assert_that(evens, equal_to([2, 4, 10, 20]), label='assert_even')
      assert_that(odds, equal_to([1, 3, 5]), label='assert_odds')
      assert_that(tens, equal_to([10, 20]), label='assert_tens')


class TypeHintsTest(unittest.TestCase):

  def test_bad_types(self):
    # [START type_hints_missing_define_numbers]
    p = TestPipeline(options=PipelineOptions(pipeline_type_check=True))

    numbers = p | beam.Create(['1', '2', '3'])
    # [END type_hints_missing_define_numbers]

    # Consider the following code.
    # pylint: disable=expression-not-assigned
    # pylint: disable=unused-variable
    # [START type_hints_missing_apply]
    evens = numbers | beam.Filter(lambda x: x % 2 == 0)
    # [END type_hints_missing_apply]

    # Now suppose numbers was defined as [snippet above].
    # When running this pipeline, you'd get a runtime error,
    # possibly on a remote machine, possibly very late.

    with self.assertRaises(TypeError):
      p.run()

    # To catch this early, we can assert what types we expect.
    with self.assertRaises(typehints.TypeCheckError):
      # [START type_hints_takes]
      evens = numbers | beam.Filter(lambda x: x % 2 == 0).with_input_types(int)
      # [END type_hints_takes]

    # Type hints can be declared on DoFns and callables as well, rather
    # than where they're used, to be more self contained.
    with self.assertRaises(typehints.TypeCheckError):
      # [START type_hints_do_fn]
      @beam.typehints.with_input_types(int)
      class FilterEvensDoFn(beam.DoFn):
        def process(self, element):
          if element % 2 == 0:
            yield element
      evens = numbers | beam.ParDo(FilterEvensDoFn())
      # [END type_hints_do_fn]

    words = p | 'words' >> beam.Create(['a', 'bb', 'c'])
    # One can assert outputs and apply them to transforms as well.
    # Helps document the contract and checks it at pipeline construction time.
    # [START type_hints_transform]
    T = beam.typehints.TypeVariable('T')

    @beam.typehints.with_input_types(T)
    @beam.typehints.with_output_types(beam.typehints.Tuple[int, T])
    class MyTransform(beam.PTransform):
      def expand(self, pcoll):
        return pcoll | beam.Map(lambda x: (len(x), x))

    words_with_lens = words | MyTransform()
    # [END type_hints_transform]

    # pylint: disable=expression-not-assigned
    with self.assertRaises(typehints.TypeCheckError):
      words_with_lens | beam.Map(lambda x: x).with_input_types(
          beam.typehints.Tuple[int, int])

  def test_runtime_checks_off(self):
    # We do not run the following pipeline, as it has incorrect type
    # information, and may fail with obscure errors, depending on the runner
    # implementation.

    # pylint: disable=expression-not-assigned
    p = TestPipeline()
    # [START type_hints_runtime_off]
    p | beam.Create(['a']) | beam.Map(lambda x: 3).with_output_types(str)
    # [END type_hints_runtime_off]

  def test_runtime_checks_on(self):
    # pylint: disable=expression-not-assigned
    p = TestPipeline(options=PipelineOptions(runtime_type_check=True))
    with self.assertRaises(typehints.TypeCheckError):
      # [START type_hints_runtime_on]
      p | beam.Create(['a']) | beam.Map(lambda x: 3).with_output_types(str)
      p.run()
      # [END type_hints_runtime_on]

  def test_deterministic_key(self):
    with TestPipeline() as p:
      lines = (p | beam.Create(
          ['banana,fruit,3', 'kiwi,fruit,2', 'kiwi,fruit,2', 'zucchini,veg,3']))

      # For pickling.
      global Player  # pylint: disable=global-variable-not-assigned

      # [START type_hints_deterministic_key]
      class Player(object):
        def __init__(self, team, name):
          self.team = team
          self.name = name

      class PlayerCoder(beam.coders.Coder):
        def encode(self, player):
          return ('%s:%s' % (player.team, player.name)).encode('utf-8')

        def decode(self, s):
          return Player(*s.decode('utf-8').split(':'))

        def is_deterministic(self):
          return True

      beam.coders.registry.register_coder(Player, PlayerCoder)

      def parse_player_and_score(csv):
        name, team, score = csv.split(',')
        return Player(team, name), int(score)

      totals = (
          lines
          | beam.Map(parse_player_and_score)
          | beam.CombinePerKey(sum).with_input_types(
              beam.typehints.Tuple[Player, int]))
      # [END type_hints_deterministic_key]

      assert_that(
          totals | beam.Map(lambda k_v: (k_v[0].name, k_v[1])),
          equal_to([('banana', 3), ('kiwi', 4), ('zucchini', 3)]))


class SnippetsTest(unittest.TestCase):
  # Replacing text read/write transforms with dummy transforms for testing.

  class DummyReadTransform(beam.PTransform):
    """A transform that will replace iobase.ReadFromText.

    To be used for testing.
    """

    def __init__(self, file_to_read=None, compression_type=None):
      self.file_to_read = file_to_read
      self.compression_type = compression_type

    class ReadDoFn(beam.DoFn):

      def __init__(self, file_to_read, compression_type):
        self.file_to_read = file_to_read
        self.compression_type = compression_type
        self.coder = coders.StrUtf8Coder()

      def process(self, element):
        pass

      def finish_bundle(self):
        from apache_beam.transforms import window

        assert self.file_to_read
        for file_name in glob.glob(self.file_to_read):
          if self.compression_type is None:
            with open(file_name, 'rb') as file:
              for record in file:
                value = self.coder.decode(record.rstrip(b'\n'))
                yield WindowedValue(value, -1, [window.GlobalWindow()])
          else:
            with gzip.open(file_name, 'rb') as file:
              for record in file:
                value = self.coder.decode(record.rstrip(b'\n'))
                yield WindowedValue(value, -1, [window.GlobalWindow()])

    def expand(self, pcoll):
      return pcoll | beam.Create([None]) | 'DummyReadForTesting' >> beam.ParDo(
          SnippetsTest.DummyReadTransform.ReadDoFn(
              self.file_to_read, self.compression_type))

  class DummyWriteTransform(beam.PTransform):
    """A transform that will replace iobase.WriteToText.

    To be used for testing.
    """

    def __init__(self, file_to_write=None, file_name_suffix=''):
      self.file_to_write = file_to_write

    class WriteDoFn(beam.DoFn):
      def __init__(self, file_to_write):
        self.file_to_write = file_to_write
        self.file_obj = None
        self.coder = ToStringCoder()

      def start_bundle(self):
        assert self.file_to_write
        # Appending a UUID to create a unique file object per invocation.
        self.file_obj = open(self.file_to_write + str(uuid.uuid4()), 'wb')

      def process(self, element):
        assert self.file_obj
        self.file_obj.write(self.coder.encode(element) + b'\n')

      def finish_bundle(self):
        assert self.file_obj
        self.file_obj.close()

    def expand(self, pcoll):
      return pcoll | 'DummyWriteForTesting' >> beam.ParDo(
          SnippetsTest.DummyWriteTransform.WriteDoFn(self.file_to_write))

  @classmethod
  def setUpClass(cls):
    # Method has been renamed in Python 3
    if sys.version_info[0] < 3:
      cls.assertCountEqual = cls.assertItemsEqual

  def setUp(self):
    self.old_read_from_text = beam.io.ReadFromText
    self.old_write_to_text = beam.io.WriteToText

    # Monkey patching to allow testing pipelines defined in snippets.py using
    # real data.
    beam.io.ReadFromText = SnippetsTest.DummyReadTransform
    beam.io.WriteToText = SnippetsTest.DummyWriteTransform
    self.temp_files = []

  def tearDown(self):
    beam.io.ReadFromText = self.old_read_from_text
    beam.io.WriteToText = self.old_write_to_text
    # Cleanup all the temporary files created in the test.
    map(os.remove, self.temp_files)

  def create_temp_file(self, contents=''):
    with tempfile.NamedTemporaryFile(delete=False) as f:
      f.write(contents.encode('utf-8'))
      self.temp_files.append(f.name)
      return f.name

  def get_output(self, path, sorted_output=True, suffix=''):
    all_lines = []
    for file_name in glob.glob(path + '*'):
      with open(file_name) as f:
        lines = f.readlines()
        all_lines.extend([s.rstrip('\n') for s in lines])

    if sorted_output:
      return sorted(s.rstrip('\n') for s in all_lines)
    return all_lines

  def test_model_pipelines(self):
    temp_path = self.create_temp_file('aa bb cc\n bb cc\n cc')
    result_path = temp_path + '.result'
    snippets.model_pipelines([
        '--input=%s*' % temp_path,
        '--output=%s' % result_path])
    self.assertEqual(
        self.get_output(result_path),
        [str(s) for s in [(u'aa', 1), (u'bb', 2), (u'cc', 3)]])

  def test_model_pcollection(self):
    temp_path = self.create_temp_file()
    snippets.model_pcollection(['--output=%s' % temp_path])
    self.assertEqual(self.get_output(temp_path, sorted_output=False), [
        'To be, or not to be: that is the question: ',
        'Whether \'tis nobler in the mind to suffer ',
        'The slings and arrows of outrageous fortune, ',
        'Or to take arms against a sea of troubles, '])

  def test_construct_pipeline(self):
    temp_path = self.create_temp_file(
        'abc def ghi\n jkl mno pqr\n stu vwx yz')
    result_path = self.create_temp_file()
    snippets.construct_pipeline({'read': temp_path, 'write': result_path})
    self.assertEqual(
        self.get_output(result_path),
        ['cba', 'fed', 'ihg', 'lkj', 'onm', 'rqp', 'uts', 'xwv', 'zy'])

  def test_model_custom_source(self):
    snippets.model_custom_source(100)

  def test_model_custom_sink(self):
    tempdir_name = tempfile.mkdtemp()

    class SimpleKV(object):

      def __init__(self, tmp_dir):
        self._dummy_token = 'dummy_token'
        self._tmp_dir = tmp_dir

      def connect(self, url):
        return self._dummy_token

      def open_table(self, access_token, table_name):
        assert access_token == self._dummy_token
        file_name = self._tmp_dir + os.sep + table_name
        assert not os.path.exists(file_name)
        open(file_name, 'wb').close()
        return table_name

      def write_to_table(self, access_token, table_name, key, value):
        assert access_token == self._dummy_token
        file_name = self._tmp_dir + os.sep + table_name
        assert os.path.exists(file_name)
        with open(file_name, 'ab') as f:
          content = (key + ':' + value + os.linesep).encode('utf-8')
          f.write(content)

      def rename_table(self, access_token, old_name, new_name):
        assert access_token == self._dummy_token
        old_file_name = self._tmp_dir + os.sep + old_name
        new_file_name = self._tmp_dir + os.sep + new_name
        assert os.path.isfile(old_file_name)
        assert not os.path.exists(new_file_name)

        os.rename(old_file_name, new_file_name)

    snippets.model_custom_sink(
        SimpleKV(tempdir_name),
        [('key' + str(i), 'value' + str(i)) for i in range(100)],
        'final_table_no_ptransform', 'final_table_with_ptransform')

    expected_output = [
        'key' + str(i) + ':' + 'value' + str(i) for i in range(100)]

    glob_pattern = tempdir_name + os.sep + 'final_table_no_ptransform*'
    output_files = glob.glob(glob_pattern)
    assert output_files

    received_output = []
    for file_name in output_files:
      with open(file_name) as f:
        for line in f:
          received_output.append(line.rstrip(os.linesep))

    self.assertCountEqual(expected_output, received_output)

    glob_pattern = tempdir_name + os.sep + 'final_table_with_ptransform*'
    output_files = glob.glob(glob_pattern)
    assert output_files

    received_output = []
    for file_name in output_files:
      with open(file_name) as f:
        for line in f:
          received_output.append(line.rstrip(os.linesep))

    self.assertCountEqual(expected_output, received_output)

  def test_model_textio(self):
    temp_path = self.create_temp_file('aa bb cc\n bb cc\n cc')
    result_path = temp_path + '.result'
    snippets.model_textio({'read': temp_path, 'write': result_path})
    self.assertEqual(
        ['aa', 'bb', 'bb', 'cc', 'cc', 'cc'],
        self.get_output(result_path, suffix='.csv'))

  def test_model_textio_compressed(self):
    temp_path = self.create_temp_file('aa\nbb\ncc')
    gzip_file_name = temp_path + '.gz'
    with open(temp_path, 'rb') as src, gzip.open(gzip_file_name, 'wb') as dst:
      dst.writelines(src)
      # Add the temporary gzip file to be cleaned up as well.
      self.temp_files.append(gzip_file_name)
    snippets.model_textio_compressed(
        {'read': gzip_file_name}, ['aa', 'bb', 'cc'])

  @unittest.skipIf(sys.version_info[0] == 3 and
                   os.environ.get('RUN_SKIPPED_PY3_TESTS') != '1',
                   'This test still needs to be fixed on Python 3'
                   'TODO: BEAM-4543')
  @unittest.skipIf(datastore_pb2 is None, 'GCP dependencies are not installed')
  def test_model_datastoreio(self):
    # We cannot test DatastoreIO functionality in unit tests, therefore we limit
    # ourselves to making sure the pipeline containing Datastore read and write
    # transforms can be built.
    # TODO(vikasrk): Expore using Datastore Emulator.
    snippets.model_datastoreio()

  @unittest.skipIf(base_api is None, 'GCP dependencies are not installed')
  def test_model_bigqueryio(self):
    # We cannot test BigQueryIO functionality in unit tests, therefore we limit
    # ourselves to making sure the pipeline containing BigQuery sources and
    # sinks can be built.
    #
    # To run locally, set `run_locally` to `True`. You will also have to set
    # `project`, `dataset` and `table` to the BigQuery table the test will write
    # to.
    run_locally = False
    if run_locally:
      project = 'my-project'
      dataset = 'samples'  # this must already exist
      table = 'model_bigqueryio'  # this will be created if needed

      options = PipelineOptions().view_as(GoogleCloudOptions)
      options.project = project
      with beam.Pipeline(options=options) as p:
        snippets.model_bigqueryio(p, project, dataset, table)
    else:
      p = TestPipeline()
      snippets.model_bigqueryio(p)

  def _run_test_pipeline_for_options(self, fn):
    temp_path = self.create_temp_file('aa\nbb\ncc')
    result_path = temp_path + '.result'
    fn([
        '--input=%s*' % temp_path,
        '--output=%s' % result_path])
    self.assertEqual(
        ['aa', 'bb', 'cc'],
        self.get_output(result_path))

  def test_pipeline_options_local(self):
    self._run_test_pipeline_for_options(snippets.pipeline_options_local)

  def test_pipeline_options_remote(self):
    self._run_test_pipeline_for_options(snippets.pipeline_options_remote)

  def test_pipeline_options_command_line(self):
    self._run_test_pipeline_for_options(snippets.pipeline_options_command_line)

  def test_pipeline_logging(self):
    result_path = self.create_temp_file()
    lines = ['we found love right where we are',
             'we found love right from the start',
             'we found love in a hopeless place']
    snippets.pipeline_logging(lines, result_path)
    self.assertEqual(
        sorted(' '.join(lines).split(' ')),
        self.get_output(result_path))

  def test_examples_wordcount(self):
    pipelines = [snippets.examples_wordcount_minimal,
                 snippets.examples_wordcount_wordcount,
                 snippets.pipeline_monitoring,
                 snippets.examples_wordcount_templated]

    for pipeline in pipelines:
      temp_path = self.create_temp_file(
          'abc def ghi\n abc jkl')
      result_path = self.create_temp_file()
      pipeline({'read': temp_path, 'write': result_path})
      self.assertEqual(
          self.get_output(result_path),
          ['abc: 2', 'def: 1', 'ghi: 1', 'jkl: 1'])

  def test_examples_ptransforms_templated(self):
    pipelines = [snippets.examples_ptransforms_templated]

    for pipeline in pipelines:
      temp_path = self.create_temp_file('1\n 2\n 3')
      result_path = self.create_temp_file()
      pipeline({'read': temp_path, 'write': result_path})
      self.assertEqual(
          self.get_output(result_path),
          ['11', '12', '13'])

  def test_examples_wordcount_debugging(self):
    temp_path = self.create_temp_file(
        'Flourish Flourish Flourish stomach abc def')
    result_path = self.create_temp_file()
    snippets.examples_wordcount_debugging(
        {'read': temp_path, 'write': result_path})
    self.assertEqual(
        self.get_output(result_path),
        ['Flourish: 3', 'stomach: 1'])

  @unittest.skipIf(pubsub is None, 'GCP dependencies are not installed')
  @mock.patch('apache_beam.io.ReadFromPubSub')
  @mock.patch('apache_beam.io.WriteStringsToPubSub')
  def test_examples_wordcount_streaming(self, *unused_mocks):
    def FakeReadFromPubSub(topic=None, subscription=None, values=None):
      expected_topic = topic
      expected_subscription = subscription

      def _inner(topic=None, subscription=None):
        assert topic == expected_topic
        assert subscription == expected_subscription
        return TestStream().add_elements(values)
      return _inner

    class AssertTransform(beam.PTransform):
      def __init__(self, matcher):
        self.matcher = matcher

      def expand(self, pcoll):
        assert_that(pcoll, self.matcher)

    def FakeWriteStringsToPubSub(topic=None, values=None):
      expected_topic = topic

      def _inner(topic=None, subscription=None):
        assert topic == expected_topic
        return AssertTransform(equal_to(values))
      return _inner

    # Test basic execution.
    input_topic = 'projects/fake-beam-test-project/topic/intopic'
    input_values = [TimestampedValue(b'a a b', 1),
                    TimestampedValue(u'🤷 ¯\\_(ツ)_/¯ b b '.encode('utf-8'), 12),
                    TimestampedValue(b'a b c c c', 20)]
    output_topic = 'projects/fake-beam-test-project/topic/outtopic'
    output_values = ['a: 1', 'a: 2', 'b: 1', 'b: 3', 'c: 3']
    beam.io.ReadFromPubSub = (
        FakeReadFromPubSub(topic=input_topic, values=input_values))
    beam.io.WriteStringsToPubSub = (
        FakeWriteStringsToPubSub(topic=output_topic, values=output_values))
    snippets.examples_wordcount_streaming([
        '--input_topic', 'projects/fake-beam-test-project/topic/intopic',
        '--output_topic', 'projects/fake-beam-test-project/topic/outtopic'])

    # Test with custom subscription.
    input_sub = 'projects/fake-beam-test-project/subscriptions/insub'
    beam.io.ReadFromPubSub = FakeReadFromPubSub(subscription=input_sub,
                                                values=input_values)
    snippets.examples_wordcount_streaming([
        '--input_subscription',
        'projects/fake-beam-test-project/subscriptions/insub',
        '--output_topic', 'projects/fake-beam-test-project/topic/outtopic'])

  def test_model_composite_transform_example(self):
    contents = ['aa bb cc', 'bb cc', 'cc']
    result_path = self.create_temp_file()
    snippets.model_composite_transform_example(contents, result_path)
    self.assertEqual(['aa: 1', 'bb: 2', 'cc: 3'], self.get_output(result_path))

  def test_model_multiple_pcollections_flatten(self):
    contents = ['a', 'b', 'c', 'd', 'e', 'f']
    result_path = self.create_temp_file()
    snippets.model_multiple_pcollections_flatten(contents, result_path)
    self.assertEqual(contents, self.get_output(result_path))

  def test_model_multiple_pcollections_partition(self):
    contents = [17, 42, 64, 32, 0, 99, 53, 89]
    result_path = self.create_temp_file()
    snippets.model_multiple_pcollections_partition(contents, result_path)
    self.assertEqual(['0', '17', '32', '42', '53', '64', '89', '99'],
                     self.get_output(result_path))

  def test_model_group_by_key(self):
    contents = ['a bb ccc bb bb a']
    result_path = self.create_temp_file()
    snippets.model_group_by_key(contents, result_path)
    expected = [('a', 2), ('bb', 3), ('ccc', 1)]
    self.assertEqual([str(s) for s in expected], self.get_output(result_path))

  def test_model_co_group_by_key_tuple(self):
    with TestPipeline() as p:
      # [START model_group_by_key_cogroupbykey_tuple_inputs]
      emails_list = [
          ('amy', 'amy@example.com'),
          ('carl', 'carl@example.com'),
          ('julia', 'julia@example.com'),
          ('carl', 'carl@email.com'),
      ]
      phones_list = [
          ('amy', '111-222-3333'),
          ('james', '222-333-4444'),
          ('amy', '333-444-5555'),
          ('carl', '444-555-6666'),
      ]

      emails = p | 'CreateEmails' >> beam.Create(emails_list)
      phones = p | 'CreatePhones' >> beam.Create(phones_list)
      # [END model_group_by_key_cogroupbykey_tuple_inputs]

      result_path = self.create_temp_file()
      snippets.model_co_group_by_key_tuple(emails, phones, result_path)

    # [START model_group_by_key_cogroupbykey_tuple_outputs]
    results = [
        ('amy', {
            'emails': ['amy@example.com'],
            'phones': ['111-222-3333', '333-444-5555']}),
        ('carl', {
            'emails': ['carl@email.com', 'carl@example.com'],
            'phones': ['444-555-6666']}),
        ('james', {
            'emails': [],
            'phones': ['222-333-4444']}),
        ('julia', {
            'emails': ['julia@example.com'],
            'phones': []}),
    ]
    # [END model_group_by_key_cogroupbykey_tuple_outputs]
    # [START model_group_by_key_cogroupbykey_tuple_formatted_outputs]
    formatted_results = [
        "amy; ['amy@example.com']; ['111-222-3333', '333-444-5555']",
        "carl; ['carl@email.com', 'carl@example.com']; ['444-555-6666']",
        "james; []; ['222-333-4444']",
        "julia; ['julia@example.com']; []",
    ]
    # [END model_group_by_key_cogroupbykey_tuple_formatted_outputs]
    expected_results = ['%s; %s; %s' % (name, info['emails'], info['phones'])
                        for name, info in results]
    self.assertEqual(expected_results, formatted_results)
    self.assertEqual(formatted_results, self.get_output(result_path))

  def test_model_use_and_query_metrics(self):
    """DebuggingWordCount example snippets."""

    import re

    p = TestPipeline()  # Use TestPipeline for testing.
    words = p | beam.Create(['albert', 'sam', 'mark', 'sarah',
                             'swati', 'daniel', 'andrea'])

    # pylint: disable=unused-variable
    # [START metrics_usage_example]
    class FilterTextFn(beam.DoFn):
      """A DoFn that filters for a specific key based on a regex."""

      def __init__(self, pattern):
        self.pattern = pattern
        # A custom metric can track values in your pipeline as it runs. Create
        # custom metrics to count unmatched words, and know the distribution of
        # word lengths in the input PCollection.
        self.word_len_dist = Metrics.distribution(self.__class__,
                                                  'word_len_dist')
        self.unmatched_words = Metrics.counter(self.__class__,
                                               'unmatched_words')

      def process(self, element):
        word = element
        self.word_len_dist.update(len(word))
        if re.match(self.pattern, word):
          yield element
        else:
          self.unmatched_words.inc()

    filtered_words = (
        words | 'FilterText' >> beam.ParDo(FilterTextFn('s.*')))
    # [END metrics_usage_example]
    # pylint: enable=unused-variable

    # [START metrics_check_values_example]
    result = p.run()
    result.wait_until_finish()

    custom_distribution = result.metrics().query(
        MetricsFilter().with_name('word_len_dist'))['distributions']
    custom_counter = result.metrics().query(
        MetricsFilter().with_name('unmatched_words'))['counters']

    if custom_distribution:
      logging.info('The average word length was %d',
                   custom_distribution[0].committed.mean)
    if custom_counter:
      logging.info('There were %d words that did not match the filter.',
                   custom_counter[0].committed)
    # [END metrics_check_values_example]

    # There should be 4 words that did not match
    self.assertEqual(custom_counter[0].committed, 4)
    # The shortest word is 3 characters, the longest is 6
    self.assertEqual(custom_distribution[0].committed.min, 3)
    self.assertEqual(custom_distribution[0].committed.max, 6)

  def test_model_join_using_side_inputs(self):
    name_list = ['a', 'b']
    email_list = [['a', 'a@example.com'], ['b', 'b@example.com']]
    phone_list = [['a', 'x4312'], ['b', 'x8452']]
    result_path = self.create_temp_file()
    snippets.model_join_using_side_inputs(
        name_list, email_list, phone_list, result_path)
    expect = ['a; a@example.com; x4312', 'b; b@example.com; x8452']
    self.assertEqual(expect, self.get_output(result_path))

  def test_model_early_late_triggers(self):
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True

    with TestPipeline(options=pipeline_options) as p:
      test_stream = (TestStream()
                     .advance_watermark_to(10)
                     .add_elements(['a', 'a', 'a', 'b', 'b'])
                     .add_elements([TimestampedValue('a', 10)])
                     .advance_watermark_to(20)
                     .advance_processing_time(60)
                     .add_elements([TimestampedValue('a', 10)]))
      trigger = (
          # [START model_early_late_triggers]
          AfterWatermark(
              early=AfterProcessingTime(delay=1 * 60),
              late=AfterCount(1))
          # [END model_early_late_triggers]
          )
      counts = (p
                | test_stream
                | 'pair_with_one' >> beam.Map(lambda x: (x, 1))
                | WindowInto(FixedWindows(15),
                             trigger=trigger,
                             accumulation_mode=AccumulationMode.DISCARDING)
                | 'group' >> beam.GroupByKey()
                | 'count' >> beam.Map(
                    lambda word_ones: (word_ones[0], sum(word_ones[1]))))
      assert_that(counts, equal_to([('a', 4), ('b', 2), ('a', 1)]))

  def test_model_setting_trigger(self):
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True

    with TestPipeline(options=pipeline_options) as p:
      test_stream = (TestStream()
                     .advance_watermark_to(10)
                     .add_elements(['a', 'a', 'a', 'b', 'b'])
                     .advance_watermark_to(70)
                     .advance_processing_time(600))
      pcollection = (p
                     | test_stream
                     | 'pair_with_one' >> beam.Map(lambda x: (x, 1)))

      counts = (
          # [START model_setting_trigger]
          pcollection | WindowInto(
              FixedWindows(1 * 60),
              trigger=AfterProcessingTime(10 * 60),
              accumulation_mode=AccumulationMode.DISCARDING)
          # [END model_setting_trigger]
          | 'group' >> beam.GroupByKey()
          | 'count' >> beam.Map(
              lambda word_ones: (word_ones[0], sum(word_ones[1]))))
      assert_that(counts, equal_to([('a', 3), ('b', 2)]))

  def test_model_composite_triggers(self):
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True

    with TestPipeline(options=pipeline_options) as p:
      test_stream = (TestStream()
                     .advance_watermark_to(10)
                     .add_elements(['a', 'a', 'a', 'b', 'b'])
                     .advance_watermark_to(70)
                     .add_elements([TimestampedValue('a', 10),
                                    TimestampedValue('a', 10),
                                    TimestampedValue('c', 10),
                                    TimestampedValue('c', 10)])
                     .advance_processing_time(600))
      pcollection = (p
                     | test_stream
                     | 'pair_with_one' >> beam.Map(lambda x: (x, 1)))

      counts = (
          # [START model_composite_triggers]
          pcollection | WindowInto(
              FixedWindows(1 * 60),
              trigger=AfterWatermark(
                  late=AfterProcessingTime(10 * 60)),
              accumulation_mode=AccumulationMode.DISCARDING)
          # [END model_composite_triggers]
          | 'group' >> beam.GroupByKey()
          | 'count' >> beam.Map(
              lambda word_ones: (word_ones[0], sum(word_ones[1]))))
      assert_that(counts, equal_to([('a', 3), ('b', 2), ('a', 2), ('c', 2)]))

  def test_model_other_composite_triggers(self):
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True

    with TestPipeline(options=pipeline_options) as p:
      test_stream = (TestStream()
                     .advance_watermark_to(10)
                     .add_elements(['a', 'a'])
                     .add_elements(['a', 'b', 'b'])
                     .advance_processing_time(60)
                     .add_elements(['a'] * 100))
      pcollection = (p
                     | test_stream
                     | 'pair_with_one' >> beam.Map(lambda x: (x, 1)))

      counts = (
          # [START model_other_composite_triggers]
          pcollection | WindowInto(
              FixedWindows(1 * 60),
              trigger=Repeatedly(
                  AfterAny(
                      AfterCount(100),
                      AfterProcessingTime(1 * 60))),
              accumulation_mode=AccumulationMode.DISCARDING)
          # [END model_other_composite_triggers]
          | 'group' >> beam.GroupByKey()
          | 'count' >> beam.Map(
              lambda word_ones: (word_ones[0], sum(word_ones[1]))))
      assert_that(counts, equal_to([('a', 3), ('b', 2), ('a', 100)]))


class CombineTest(unittest.TestCase):
  """Tests for model/combine."""

  def test_global_sum(self):
    pc = [1, 2, 3]
    # [START global_sum]
    result = pc | beam.CombineGlobally(sum)
    # [END global_sum]
    self.assertEqual([6], result)

  def test_combine_values(self):
    occurences = [('cat', 1), ('cat', 5), ('cat', 9), ('dog', 5), ('dog', 2)]
    # [START combine_values]
    first_occurences = occurences | beam.GroupByKey() | beam.CombineValues(min)
    # [END combine_values]
    self.assertEqual({('cat', 1), ('dog', 2)}, set(first_occurences))

  def test_combine_per_key(self):
    player_accuracies = [
        ('cat', 1), ('cat', 5), ('cat', 9), ('cat', 1),
        ('dog', 5), ('dog', 2)]
    # [START combine_per_key]
    avg_accuracy_per_player = (player_accuracies
                               | beam.CombinePerKey(
                                   beam.combiners.MeanCombineFn()))
    # [END combine_per_key]
    self.assertEqual({('cat', 4.0), ('dog', 3.5)}, set(avg_accuracy_per_player))

  def test_combine_concat(self):
    pc = ['a', 'b']

    # [START combine_concat]
    def concat(values, separator=', '):
      return separator.join(values)
    with_commas = pc | beam.CombineGlobally(concat)
    with_dashes = pc | beam.CombineGlobally(concat, separator='-')
    # [END combine_concat]
    self.assertEqual(1, len(with_commas))
    self.assertTrue(with_commas[0] in {'a, b', 'b, a'})
    self.assertEqual(1, len(with_dashes))
    self.assertTrue(with_dashes[0] in {'a-b', 'b-a'})

  def test_bounded_sum(self):
    # [START combine_bounded_sum]
    pc = [1, 10, 100, 1000]

    def bounded_sum(values, bound=500):
      return min(sum(values), bound)
    small_sum = pc | beam.CombineGlobally(bounded_sum)              # [500]
    large_sum = pc | beam.CombineGlobally(bounded_sum, bound=5000)  # [1111]
    # [END combine_bounded_sum]
    self.assertEqual([500], small_sum)
    self.assertEqual([1111], large_sum)

  def test_combine_reduce(self):
    factors = [2, 3, 5, 7]
    # [START combine_reduce]
    import functools
    import operator
    product = factors | beam.CombineGlobally(
        functools.partial(functools.reduce, operator.mul), 1)
    # [END combine_reduce]
    self.assertEqual([210], product)

  def test_custom_average(self):
    pc = [2, 3, 5, 7]

    # [START combine_custom_average_define]
    class AverageFn(beam.CombineFn):
      def create_accumulator(self):
        return (0.0, 0)

      def add_input(self, sum_count, input):
        (sum, count) = sum_count
        return sum + input, count + 1

      def merge_accumulators(self, accumulators):
        sums, counts = zip(*accumulators)
        return sum(sums), sum(counts)

      def extract_output(self, sum_count):
        (sum, count) = sum_count
        return sum / count if count else float('NaN')
    # [END combine_custom_average_define]
    # [START combine_custom_average_execute]
    average = pc | beam.CombineGlobally(AverageFn())
    # [END combine_custom_average_execute]
    self.assertEqual([4.25], average)

  def test_keys(self):
    occurrences = [('cat', 1), ('cat', 5), ('dog', 5), ('cat', 9), ('dog', 2)]
    unique_keys = occurrences | snippets.Keys()
    self.assertEqual({'cat', 'dog'}, set(unique_keys))

  def test_count(self):
    occurrences = ['cat', 'dog', 'cat', 'cat', 'dog']
    perkey_counts = occurrences | snippets.Count()
    self.assertEqual({('cat', 3), ('dog', 2)}, set(perkey_counts))

  def test_setting_fixed_windows(self):
    with TestPipeline() as p:
      unkeyed_items = p | beam.Create([22, 33, 55, 100, 115, 120])
      items = (unkeyed_items
               | 'key' >> beam.Map(
                   lambda x: beam.window.TimestampedValue(('k', x), x)))
      # [START setting_fixed_windows]
      from apache_beam import window
      fixed_windowed_items = (
          items | 'window' >> beam.WindowInto(window.FixedWindows(60)))
      # [END setting_fixed_windows]
      summed = (fixed_windowed_items
                | 'group' >> beam.GroupByKey()
                | 'combine' >> beam.CombineValues(sum))
      unkeyed = summed | 'unkey' >> beam.Map(lambda x: x[1])
      assert_that(unkeyed, equal_to([110, 215, 120]))

  def test_setting_sliding_windows(self):
    with TestPipeline() as p:
      unkeyed_items = p | beam.Create([2, 16, 23])
      items = (unkeyed_items
               | 'key' >> beam.Map(
                   lambda x: beam.window.TimestampedValue(('k', x), x)))
      # [START setting_sliding_windows]
      from apache_beam import window
      sliding_windowed_items = (
          items | 'window' >> beam.WindowInto(window.SlidingWindows(30, 5)))
      # [END setting_sliding_windows]
      summed = (sliding_windowed_items
                | 'group' >> beam.GroupByKey()
                | 'combine' >> beam.CombineValues(sum))
      unkeyed = summed | 'unkey' >> beam.Map(lambda x: x[1])
      assert_that(unkeyed,
                  equal_to([2, 2, 2, 18, 23, 39, 39, 39, 41, 41]))

  def test_setting_session_windows(self):
    with TestPipeline() as p:
      unkeyed_items = p | beam.Create([2, 11, 16, 27])
      items = (unkeyed_items
               | 'key' >> beam.Map(
                   lambda x: beam.window.TimestampedValue(('k', x), x * 60)))
      # [START setting_session_windows]
      from apache_beam import window
      session_windowed_items = (
          items | 'window' >> beam.WindowInto(window.Sessions(10 * 60)))
      # [END setting_session_windows]
      summed = (session_windowed_items
                | 'group' >> beam.GroupByKey()
                | 'combine' >> beam.CombineValues(sum))
      unkeyed = summed | 'unkey' >> beam.Map(lambda x: x[1])
      assert_that(unkeyed, equal_to([29, 27]))

  def test_setting_global_window(self):
    with TestPipeline() as p:
      unkeyed_items = p | beam.Create([2, 11, 16, 27])
      items = (unkeyed_items
               | 'key' >> beam.Map(
                   lambda x: beam.window.TimestampedValue(('k', x), x)))
      # [START setting_global_window]
      from apache_beam import window
      session_windowed_items = (
          items | 'window' >> beam.WindowInto(window.GlobalWindows()))
      # [END setting_global_window]
      summed = (session_windowed_items
                | 'group' >> beam.GroupByKey()
                | 'combine' >> beam.CombineValues(sum))
      unkeyed = summed | 'unkey' >> beam.Map(lambda x: x[1])
      assert_that(unkeyed, equal_to([56]))

  def test_setting_timestamp(self):
    with TestPipeline() as p:
      unkeyed_items = p | beam.Create([12, 30, 60, 61, 66])
      items = (unkeyed_items | 'key' >> beam.Map(lambda x: ('k', x)))

      def extract_timestamp_from_log_entry(entry):
        return entry[1]

      # [START setting_timestamp]
      class AddTimestampDoFn(beam.DoFn):

        def process(self, element):
          # Extract the numeric Unix seconds-since-epoch timestamp to be
          # associated with the current log entry.
          unix_timestamp = extract_timestamp_from_log_entry(element)
          # Wrap and emit the current entry and new timestamp in a
          # TimestampedValue.
          yield beam.window.TimestampedValue(element, unix_timestamp)

      timestamped_items = items | 'timestamp' >> beam.ParDo(AddTimestampDoFn())
      # [END setting_timestamp]
      fixed_windowed_items = (
          timestamped_items | 'window' >> beam.WindowInto(
              beam.window.FixedWindows(60)))
      summed = (fixed_windowed_items
                | 'group' >> beam.GroupByKey()
                | 'combine' >> beam.CombineValues(sum))
      unkeyed = summed | 'unkey' >> beam.Map(lambda x: x[1])
      assert_that(unkeyed, equal_to([42, 187]))


class PTransformTest(unittest.TestCase):
  """Tests for PTransform."""

  def test_composite(self):

    # [START model_composite_transform]
    class ComputeWordLengths(beam.PTransform):
      def expand(self, pcoll):
        # Transform logic goes here.
        return pcoll | beam.Map(lambda x: len(x))
    # [END model_composite_transform]

    with TestPipeline() as p:
      lengths = p | beam.Create(["a", "ab", "abc"]) | ComputeWordLengths()
      assert_that(lengths, equal_to([1, 2, 3]))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
