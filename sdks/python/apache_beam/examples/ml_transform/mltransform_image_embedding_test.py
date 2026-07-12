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

import io
import unittest

import apache_beam as beam
from apache_beam.io.gcp import bigquery_tools
from PIL import Image

from apache_beam.examples.ml_transform import mltransform_image_embedding


class FakeArray:
  def __init__(self, value):
    self.value = value

  def tolist(self):
    return self.value


class MLTransformImageEmbeddingTest(unittest.TestCase):
  def test_filter_empty_uri(self):
    self.assertEqual(
        list(mltransform_image_embedding.filter_empty_uri('  ')), [])
    self.assertEqual(
        list(mltransform_image_embedding.filter_empty_uri(' gs://bucket/img ')),
        ['gs://bucket/img'])

  def test_sha1_hex(self):
    self.assertEqual(
        mltransform_image_embedding.sha1_hex('gs://bucket/img.jpg'),
        'f3557ebc47344e0189d39dcad3642c8e8afbc10a')

  def test_decode_pil_returns_rgb_image(self):
    image = Image.new('RGBA', (2, 2), color=(255, 0, 0, 255))
    image_bytes = io.BytesIO()
    image.save(image_bytes, format='PNG')

    decoded = mltransform_image_embedding.decode_pil(image_bytes.getvalue())

    self.assertEqual(decoded.mode, 'RGB')
    self.assertEqual(decoded.size, (2, 2))

  def test_embedding_to_list(self):
    self.assertEqual(
        mltransform_image_embedding.embedding_to_list(FakeArray([1, 2.5])),
        [1, 2.5])

  def test_format_output_with_dict(self):
    row = {
        'image_id': 'abc',
        'image_uri': 'gs://bucket/img.jpg',
        'image': FakeArray([0.1, 0.2, 0.3]),
    }

    output = next(
        mltransform_image_embedding.FormatImageEmbeddingOutput(
            'clip-ViT-B-32').process(row))

    self.assertEqual(output['image_id'], 'abc')
    self.assertEqual(output['image_uri'], 'gs://bucket/img.jpg')
    self.assertEqual(output['model_name'], 'clip-ViT-B-32')
    self.assertEqual(output['embedding'], [0.1, 0.2, 0.3])
    self.assertEqual(output['embedding_dim'], 3)
    self.assertIn('infer_ms', output)

  def test_output_table_schema_marks_embedding_as_repeated(self):
    schema = bigquery_tools.get_dict_table_schema(
        mltransform_image_embedding.OUTPUT_TABLE_SCHEMA)
    embedding_field = next(
        field for field in schema['fields'] if field['name'] == 'embedding')
    self.assertEqual(embedding_field['type'], 'FLOAT64')
    self.assertEqual(embedding_field['mode'], 'REPEATED')

  def test_format_output_with_beam_row(self):
    row = beam.Row(
        image_id='abc',
        image_uri='gs://bucket/img.jpg',
        image=FakeArray([0.1, 0.2]),
    )

    output = next(
        mltransform_image_embedding.FormatImageEmbeddingOutput(
            'clip-ViT-B-32').process(row))

    self.assertEqual(output['embedding_dim'], 2)


if __name__ == '__main__':
  unittest.main()
