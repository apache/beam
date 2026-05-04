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

import json
import os
import tempfile
import unittest

try:
  from apache_beam.examples.ml_transform import mltransform_generate_vocab
except ImportError:
  raise unittest.SkipTest('tensorflow_transform is not installed.')


class MLTransformGenerateVocabUnitTest(unittest.TestCase):
  def test_normalize_text(self):
    text = mltransform_generate_vocab.normalize_text('  Hello Beam  ', True)
    self.assertEqual(text, 'hello beam')

  def test_null_and_empty_handling_helpers(self):
    normalized_none = mltransform_generate_vocab.normalize_text(None, True)
    self.assertEqual(normalized_none, '')
    self.assertEqual(
        mltransform_generate_vocab._build_vocab_text(
            {
                'text': ['Beam', None, '  ', 'Flow']
            }, ['text'], lowercase=True),
        'beam flow')


class MLTransformGenerateVocabCliValidationTest(unittest.TestCase):
  def test_missing_required_args(self):
    args, _ = mltransform_generate_vocab.parse_known_args([])
    with self.assertRaisesRegex(ValueError, 'input_file or --input_table'):
      mltransform_generate_vocab.validate_args(args)

  def test_invalid_numeric_values(self):
    args, _ = mltransform_generate_vocab.parse_known_args([
        '--input_file=a.jsonl',
        '--output_vocab=/tmp/vocab',
        '--columns=text',
        '--vocab_size=0',
        '--min_frequency=0',
    ])
    with self.assertRaisesRegex(ValueError, 'vocab_size'):
      mltransform_generate_vocab.validate_args(args)

  def test_invalid_input_expand_factor(self):
    args, _ = mltransform_generate_vocab.parse_known_args([
        '--input_file=a.jsonl',
        '--output_vocab=/tmp/vocab',
        '--columns=text',
        '--input_expand_factor=0',
    ])
    with self.assertRaisesRegex(ValueError, 'input_expand_factor'):
      mltransform_generate_vocab.validate_args(args)


class MLTransformGenerateVocabIntegrationTest(unittest.TestCase):
  def test_batch_pipeline_exact_output_order(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      input_path = os.path.join(tmpdir, 'input.jsonl')
      output_prefix = os.path.join(tmpdir, 'vocab.txt')

      rows = [
          {
              'id': '1', 'text': 'Beam beam ML pipeline'
          },
          {
              'id': '2', 'text': 'Beam pipeline dataflow'
          },
          {
              'id': '3', 'text': 'ML transform beam'
          },
          {
              'id': '4', 'text': 'vocab vocab vocab test'
          },
          {
              'id': '5', 'text': 'rare_token_once'
          },
          {
              'id': '6', 'text': ''
          },
          {
              'id': '7', 'text': None
          },
      ]
      with open(input_path, 'w', encoding='utf-8') as f:
        for row in rows:
          f.write(json.dumps(row) + '\n')

      mltransform_generate_vocab.run([
          f'--input_file={input_path}',
          f'--output_vocab={output_prefix}',
          '--columns=text',
          '--vocab_size=3',
          '--min_frequency=2',
          '--lowercase=true',
          '--runner=DirectRunner',
      ])

      output_path = output_prefix
      with open(output_path, 'r', encoding='utf-8') as f:
        output_tokens = [line.rstrip('\n') for line in f]

      self.assertEqual(set(output_tokens), {'beam', 'vocab', 'ml'})
      self.assertEqual(len(output_tokens), 3)

  def test_output_is_stable_across_runs(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      input_path = os.path.join(tmpdir, 'input.jsonl')
      output_prefix = os.path.join(tmpdir, 'vocab.txt')
      rows = [
          {
              'text': 'apple banana'
          },
          {
              'text': 'banana apple'
          },
          {
              'text': 'cat dog'
          },
          {
              'text': 'dog cat'
          },
      ]
      with open(input_path, 'w', encoding='utf-8') as f:
        for row in rows:
          f.write(json.dumps(row) + '\n')

      common_args = [
          f'--input_file={input_path}',
          '--columns=text',
          '--vocab_size=4',
          '--min_frequency=2',
          '--lowercase=true',
          '--runner=DirectRunner',
      ]

      mltransform_generate_vocab.run(
          common_args + [f'--output_vocab={output_prefix}'])

      output_path = output_prefix
      with open(output_path, 'r', encoding='utf-8') as f:
        first_run_tokens = [line.rstrip('\n') for line in f]

      output_prefix_second = os.path.join(tmpdir, 'vocab_second.txt')
      mltransform_generate_vocab.run(
          common_args + [f'--output_vocab={output_prefix_second}'])

      with open(output_prefix_second, 'r', encoding='utf-8') as f:
        second_run_tokens = [line.rstrip('\n') for line in f]

      self.assertEqual(first_run_tokens, second_run_tokens)

  def test_empty_filtered_result_writes_empty_vocab(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      input_path = os.path.join(tmpdir, 'input.jsonl')
      output_prefix = os.path.join(tmpdir, 'vocab.txt')
      with open(input_path, 'w', encoding='utf-8') as f:
        f.write(json.dumps({'text': 'beam'}) + '\n')

      mltransform_generate_vocab.run([
          f'--input_file={input_path}',
          f'--output_vocab={output_prefix}',
          '--columns=text',
          '--vocab_size=10',
          '--min_frequency=2',
          '--runner=DirectRunner',
      ])

      output_path = output_prefix
      with open(output_path, 'r', encoding='utf-8') as f:
        output_tokens = [line.rstrip('\n') for line in f]
      self.assertEqual(output_tokens, [])


if __name__ == '__main__':
  unittest.main()
