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

import hashlib
import json
import unittest

import apache_beam as beam

from apache_beam.examples.ml_transform import mltransform_text_embedding


class FakeArray:
  def __init__(self, value):
    self.value = value

  def tolist(self):
    return self.value


class MLTransformTextEmbeddingTest(unittest.TestCase):
  def test_text_to_record_skips_empty_lines(self):
    self.assertEqual(list(mltransform_text_embedding.text_to_record('   ')), [])

  def test_text_to_record_preserves_text_and_adds_stable_id(self):
    records = list(
        mltransform_text_embedding.text_to_record('  Apache Beam is fun  '))

    self.assertEqual(len(records), 1)
    expected_id = hashlib.sha256(b'Apache Beam is fun').hexdigest()
    self.assertEqual(
        records[0],
        {
            'id': expected_id,
            'raw_text': 'Apache Beam is fun',
            'text': 'Apache Beam is fun',
        })

  def test_embedding_to_list(self):
    self.assertEqual(
        mltransform_text_embedding.embedding_to_list(FakeArray([1, 2.5])),
        [1, 2.5])

  def test_format_embedding_output_with_dict(self):
    row = {
        'id': 'abc',
        'raw_text': 'hello',
        'text': FakeArray([0.1, 0.2, 0.3]),
    }

    output = next(
        mltransform_text_embedding.FormatEmbeddingOutput('test-model').process(
            row))

    self.assertEqual(
        json.loads(output),
        {
            'id': 'abc',
            'raw_text': 'hello',
            'model_name': 'test-model',
            'embedding': [0.1, 0.2, 0.3],
            'embedding_dim': 3,
        })

  def test_format_embedding_output_with_beam_row(self):
    row = beam.Row(
        id='abc',
        raw_text='hello',
        text=FakeArray([0.1, 0.2]),
    )

    output = next(
        mltransform_text_embedding.FormatEmbeddingOutput('test-model').process(
            row))

    self.assertEqual(json.loads(output)['embedding_dim'], 2)


if __name__ == '__main__':
  unittest.main()
