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

import datetime
import io
import json
import logging
import os
import tempfile
import unittest

import fastavro
import mock

import apache_beam as beam
from apache_beam.coders.row_coder import RowCoder
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.testing.util import AssertThat
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.typehints import schemas as schema_utils
from apache_beam.utils import subprocess_server
from apache_beam.utils.timestamp import Timestamp
from apache_beam.yaml.yaml_transform import YamlTransform
from apache_beam.yaml.yaml_transform import expand_pipeline

try:
  import jsonschema
except ImportError:
  jsonschema = None


class FakeReadFromPubSub:
  def __init__(
      self,
      topic,
      messages,
      subscription=None,
      id_attribute=None,
      timestamp_attribute=None):
    self._topic = topic
    self._subscription = subscription
    self._messages = messages
    self._id_attribute = id_attribute
    self._timestamp_attribute = timestamp_attribute

  def __call__(
      self,
      *,
      topic,
      subscription,
      with_attributes,
      id_label,
      timestamp_attribute):
    assert topic == self._topic
    assert id_label == self._id_attribute
    assert timestamp_attribute == self._timestamp_attribute
    assert subscription == self._subscription
    if with_attributes:
      data = self._messages
    else:
      data = [x.data for x in self._messages]
    return beam.Create(data)


class FakeWriteToPubSub:
  def __init__(
      self, topic, messages, id_attribute=None, timestamp_attribute=None):
    self._topic = topic
    self._messages = messages
    self._id_attribute = id_attribute
    self._timestamp_attribute = timestamp_attribute

  def __call__(self, topic, *, with_attributes, id_label, timestamp_attribute):
    assert topic == self._topic
    assert with_attributes is True
    assert id_label == self._id_attribute
    assert timestamp_attribute == self._timestamp_attribute
    return AssertThat(equal_to(self._messages))


@unittest.skipIf(jsonschema is None, "Yaml dependencies not installed")
class YamlPubSubTest(unittest.TestCase):
  def test_simple_read(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      with mock.patch('apache_beam.io.ReadFromPubSub',
                      FakeReadFromPubSub(
                          topic='my_topic',
                          messages=[PubsubMessage(b'msg1', {'attr': 'value1'}),
                                    PubsubMessage(b'msg2',
                                                  {'attr': 'value2'})])):
        result = p | YamlTransform(
            '''
            type: ReadFromPubSub
            config:
              topic: my_topic
              format: RAW
            ''')
        assert_that(
            result,
            equal_to([beam.Row(payload=b'msg1'), beam.Row(payload=b'msg2')]))

  def test_simple_read_string(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      with mock.patch('apache_beam.io.ReadFromPubSub',
                      FakeReadFromPubSub(
                          topic='my_topic',
                          messages=[PubsubMessage('äter'.encode('utf-8'),
                                                  {'attr': 'value1'}),
                                    PubsubMessage('köttbullar'.encode('utf-8'),
                                                  {'attr': 'value2'})])):
        result = p | YamlTransform(
            '''
            type: ReadFromPubSub
            config:
              topic: my_topic
              format: STRING
            ''')
        assert_that(
            result,
            equal_to([beam.Row(payload='äter'),
                      beam.Row(payload='köttbullar')]))

  def test_read_with_attribute(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      with mock.patch('apache_beam.io.ReadFromPubSub',
                      FakeReadFromPubSub(
                          topic='my_topic',
                          messages=[PubsubMessage(b'msg1', {'attr': 'value1'}),
                                    PubsubMessage(b'msg2',
                                                  {'attr': 'value2'})])):
        result = p | YamlTransform(
            '''
            type: ReadFromPubSub
            config:
              topic: my_topic
              format: RAW
              attributes: [attr]
            ''')
        assert_that(
            result,
            equal_to([
                beam.Row(payload=b'msg1', attr='value1'),
                beam.Row(payload=b'msg2', attr='value2')
            ]))

  def test_read_with_attribute_map(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      with mock.patch('apache_beam.io.ReadFromPubSub',
                      FakeReadFromPubSub(
                          topic='my_topic',
                          messages=[PubsubMessage(b'msg1', {'attr': 'value1'}),
                                    PubsubMessage(b'msg2',
                                                  {'attr': 'value2'})])):
        result = p | YamlTransform(
            '''
            type: ReadFromPubSub
            config:
              topic: my_topic
              format: RAW
              attributes_map: attrMap
            ''')
        assert_that(
            result,
            equal_to([
                beam.Row(payload=b'msg1', attrMap={'attr': 'value1'}),
                beam.Row(payload=b'msg2', attrMap={'attr': 'value2'})
            ]))

  def test_read_with_publish_time_field(self):
    publish_time_1 = datetime.datetime(
        2018, 3, 12, 13, 37, 1, 234567, tzinfo=datetime.timezone.utc)
    publish_time_2 = datetime.datetime(
        2018, 3, 12, 13, 38, 2, 345678, tzinfo=datetime.timezone.utc)
    publish_time_3 = Timestamp.from_utc_datetime(
        datetime.datetime(
            2018, 3, 12, 13, 39, 3, 456789, tzinfo=datetime.timezone.utc))
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      with mock.patch('apache_beam.io.ReadFromPubSub',
                      FakeReadFromPubSub(
                          topic='my_topic',
                          messages=[PubsubMessage(b'msg1', {'attr': 'value1'},
                                                  publish_time=publish_time_1),
                                    PubsubMessage(b'msg2', {'attr': 'value2'},
                                                  publish_time=publish_time_2),
                                    PubsubMessage(b'msg3', {'attr': 'value3'},
                                                  publish_time=publish_time_3),
                                    PubsubMessage(b'msg4',
                                                  {'attr': 'value4'})])):
        result = p | YamlTransform(
            '''
            type: ReadFromPubSub
            config:
              topic: my_topic
              format: RAW
              publish_time_field: publish_time
            ''')
        assert_that(
            result,
            equal_to([
                beam.Row(
                    payload=b'msg1',
                    publish_time=Timestamp.from_utc_datetime(publish_time_1)),
                beam.Row(
                    payload=b'msg2',
                    publish_time=Timestamp.from_utc_datetime(publish_time_2)),
                beam.Row(payload=b'msg3', publish_time=publish_time_3),
                beam.Row(payload=b'msg4', publish_time=None)
            ]))

  def test_read_with_attributes_and_publish_time_field(self):
    publish_time_1 = Timestamp.from_utc_datetime(
        datetime.datetime(
            2018, 3, 12, 13, 37, 1, 234567, tzinfo=datetime.timezone.utc))
    publish_time_2 = Timestamp.from_utc_datetime(
        datetime.datetime(
            2018, 3, 12, 13, 38, 2, 345678, tzinfo=datetime.timezone.utc))
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      with mock.patch('apache_beam.io.ReadFromPubSub',
                      FakeReadFromPubSub(
                          topic='my_topic',
                          messages=[PubsubMessage(b'msg1', {'attr': 'value1'},
                                                  publish_time=publish_time_1),
                                    PubsubMessage(b'msg2', {'attr': 'value2'},
                                                  publish_time=publish_time_2)
                                    ])):
        result = p | YamlTransform(
            '''
            type: ReadFromPubSub
            config:
              topic: my_topic
              format: RAW
              attributes: [attr]
              attributes_map: attrMap
              publish_time_field: publish_time
            ''')
        assert_that(
            result,
            equal_to([
                beam.Row(
                    payload=b'msg1',
                    attr='value1',
                    attrMap={'attr': 'value1'},
                    publish_time=publish_time_1),
                beam.Row(
                    payload=b'msg2',
                    attr='value2',
                    attrMap={'attr': 'value2'},
                    publish_time=publish_time_2)
            ]))

  def test_read_with_empty_publish_time_field(self):
    for publish_time_field in ('', '   '):
      with self.subTest(publish_time_field=publish_time_field):
        with self.assertRaisesRegex(
            ValueError, 'publish_time_field must be a non-empty field name'):
          with beam.Pipeline(
              options=beam.options.pipeline_options.PipelineOptions(
                  pickle_library='cloudpickle')) as p:
            _ = p | YamlTransform(
                '''
                type: ReadFromPubSub
                config:
                  topic: my_topic
                  format: RAW
                  publish_time_field: "%s"
                ''' % publish_time_field)

  def test_read_with_id_attribute(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      with mock.patch('apache_beam.io.ReadFromPubSub',
                      FakeReadFromPubSub(
                          topic='my_topic',
                          messages=[PubsubMessage(b'msg1', {'attr': 'value1'}),
                                    PubsubMessage(b'msg2', {'attr': 'value2'})],
                          id_attribute='some_attr')):
        result = p | YamlTransform(
            '''
            type: ReadFromPubSub
            config:
              topic: my_topic
              format: RAW
              id_attribute: some_attr
            ''')
        assert_that(
            result,
            equal_to([beam.Row(payload=b'msg1'), beam.Row(payload=b'msg2')]))

  _avro_schema = {
      'type': 'record',
      'name': 'ec',
      'fields': [{
          'name': 'label', 'type': 'string'
      }, {
          'name': 'rank', 'type': 'int'
      }]
  }

  def _encode_avro(self, data):
    buffer = io.BytesIO()
    fastavro.schemaless_writer(buffer, self._avro_schema, data)
    buffer.seek(0)
    return buffer.read()

  def test_read_avro(self):

    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      with mock.patch(
          'apache_beam.io.ReadFromPubSub',
          FakeReadFromPubSub(
              topic='my_topic',
              messages=[PubsubMessage(self._encode_avro({'label': '37a',
                                                         'rank': 1}), {}),
                        PubsubMessage(self._encode_avro({'label': '389a',
                                                         'rank': 2}), {})])):
        result = p | YamlTransform(
            '''
            type: ReadFromPubSub
            config:
              topic: my_topic
              format: AVRO
              schema: %s
            ''' % json.dumps(self._avro_schema))
        assert_that(
            result,
            equal_to([
                beam.Row(label='37a', rank=1),  # linebreak
                beam.Row(label='389a', rank=2)
            ]))

  def test_read_json(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      with mock.patch('apache_beam.io.ReadFromPubSub',
                      FakeReadFromPubSub(
                          topic='my_topic',
                          messages=[PubsubMessage(
                              b'{"generator": {"x": 0, "y": 0}, "rank": 1}',
                              {'weierstrass': 'y^2+y=x^3-x', 'label': '37a'})
                                    ])):
        result = p | YamlTransform(
            '''
            type: ReadFromPubSub
            config:
              topic: my_topic
              format: JSON
              schema:
                type: object
                properties:
                  generator:
                    type: object
                    properties:
                      x: {type: integer}
                      y: {type: integer}
                  rank: {type: integer}
              attributes: [label]
              attributes_map: other
            ''')
        assert_that(
            result,
            equal_to([
                beam.Row(
                    generator=beam.Row(x=0, y=0),
                    rank=1,
                    label='37a',
                    other={
                        'label': '37a', 'weierstrass': 'y^2+y=x^3-x'
                    })
            ]))

  def test_read_json_with_error_handling(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      with mock.patch(
          'apache_beam.io.ReadFromPubSub',
          FakeReadFromPubSub(topic='my_topic',
                             messages=[PubsubMessage('{"some_int": 123}',
                                                     attributes={}),
                                       PubsubMessage('unparsable',
                                                     attributes={})])):
        result = p | YamlTransform(
            '''
            type: ReadFromPubSub
            config:
              topic: my_topic
              format: JSON
              schema:
                type: object
                properties:
                  some_int: {type: integer}
              error_handling:
                output: errors
            ''')
        assert_that(
            result['good'],
            equal_to([beam.Row(some_int=123)]),
            label='CheckGood')
        assert_that(
            result['errors'] | beam.Map(lambda error: error.element),
            equal_to(['unparsable']),
            label='CheckErrors')

  def test_read_json_without_error_handling(self):
    with self.assertRaises(Exception):
      with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle')) as p:
        with mock.patch(
            'apache_beam.io.ReadFromPubSub',
            FakeReadFromPubSub(topic='my_topic',
                               messages=[PubsubMessage('{"some_int": 123}',
                                                       attributes={}),
                                         PubsubMessage('unparsable',
                                                       attributes={})])):
          _ = p | YamlTransform(
              '''
              type: ReadFromPubSub
              config:
                topic: my_topic
                format: JSON
                schema:
                  type: object
                  properties:
                    some_int: {type: integer}
              ''')

  def test_read_json_with_bad_schema(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      with mock.patch('apache_beam.io.ReadFromPubSub',
                      FakeReadFromPubSub(
                          topic='my_topic',
                          messages=[PubsubMessage('{"some_int": 123}',
                                                  attributes={}),
                                    PubsubMessage('{"some_int": "NOT"}',
                                                  attributes={})])):
        result = p | YamlTransform(
            '''
            type: ReadFromPubSub
            config:
              topic: my_topic
              format: JSON
              schema:
                type: object
                properties:
                  some_int: {type: integer}
              error_handling:
                output: errors
            ''')
        assert_that(
            result['good'],
            equal_to([beam.Row(some_int=123)]),
            label='CheckGood')
        assert_that(
            result['errors'] | beam.Map(lambda error: error.element),
            equal_to(['{"some_int": "NOT"}']),
            label='CheckErrors')

  def test_simple_write(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      with mock.patch('apache_beam.io.WriteToPubSub',
                      FakeWriteToPubSub(topic='my_topic',
                                        messages=[PubsubMessage(b'msg1', {}),
                                                  PubsubMessage(b'msg2', {})])):
        _ = (
            p | beam.Create([beam.Row(a=b'msg1'), beam.Row(a=b'msg2')])
            | YamlTransform(
                '''
            type: WriteToPubSub
            config:
              topic: my_topic
              format: RAW
            '''))

  def test_write_with_attribute(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      with mock.patch('apache_beam.io.WriteToPubSub',
                      FakeWriteToPubSub(
                          topic='my_topic',
                          messages=[PubsubMessage(b'msg1', {'attr': 'value1'}),
                                    PubsubMessage(b'msg2',
                                                  {'attr': 'value2'})])):
        _ = (
            p | beam.Create([
                beam.Row(a=b'msg1', attr='value1'),
                beam.Row(a=b'msg2', attr='value2')
            ]) | YamlTransform(
                '''
            type: WriteToPubSub
            config:
              topic: my_topic
              format: RAW
              attributes: [attr]
            '''))

  def test_write_with_attribute_map(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      with mock.patch('apache_beam.io.WriteToPubSub',
                      FakeWriteToPubSub(topic='my_topic',
                                        messages=[PubsubMessage(b'msg1',
                                                                {'a': 'b'}),
                                                  PubsubMessage(b'msg2',
                                                                {'c': 'd'})])):
        _ = (
            p | beam.Create([
                beam.Row(a=b'msg1', attrMap={'a': 'b'}),
                beam.Row(a=b'msg2', attrMap={'c': 'd'})
            ]) | YamlTransform(
                '''
            type: WriteToPubSub
            config:
              topic: my_topic
              format: RAW
              attributes_map: attrMap
            '''))

  def test_write_with_id_attribute(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      with mock.patch('apache_beam.io.WriteToPubSub',
                      FakeWriteToPubSub(topic='my_topic',
                                        messages=[PubsubMessage(b'msg1', {}),
                                                  PubsubMessage(b'msg2', {})],
                                        id_attribute='some_attr')):
        _ = (
            p | beam.Create([beam.Row(a=b'msg1'), beam.Row(a=b'msg2')])
            | YamlTransform(
                '''
            type: WriteToPubSub
            config:
              topic: my_topic
              format: RAW
              id_attribute: some_attr
            '''))

  def test_write_avro(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      with mock.patch(
          'apache_beam.io.WriteToPubSub',
          FakeWriteToPubSub(
              topic='my_topic',
              messages=[PubsubMessage(self._encode_avro({'label': '37a',
                                                         'rank': 1}), {}),
                        PubsubMessage(self._encode_avro({'label': '389a',
                                                         'rank': 2}), {})])):
        _ = (
            p | beam.Create(
                [beam.Row(label='37a', rank=1), beam.Row(label='389a', rank=2)])
            | YamlTransform(
                '''
            type: WriteToPubSub
            config:
              topic: my_topic
              format: AVRO
            '''))

  def test_write_json(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      with mock.patch('apache_beam.io.WriteToPubSub',
                      FakeWriteToPubSub(
                          topic='my_topic',
                          messages=[PubsubMessage(
                              b'{"generator": {"x": 0, "y": 0}, "rank": 1}',
                              {'weierstrass': 'y^2+y=x^3-x', 'label': '37a'})
                                    ])):
        _ = (
            p | beam.Create([
                beam.Row(
                    label='37a',
                    generator=beam.Row(x=0, y=0),
                    rank=1,
                    other={'weierstrass': 'y^2+y=x^3-x'})
            ]) | YamlTransform(
                '''
            type: WriteToPubSub
            config:
              topic: my_topic
              format: JSON
              attributes: [label]
              attributes_map: other
            '''))

  def test_write_proto(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      data = [beam.Row(label='37a', rank=1), beam.Row(label='389a', rank=2)]
      coder = RowCoder(
          schema_utils.named_fields_to_schema([('label', str), ('rank', int)]))
      expected_messages = [PubsubMessage(coder.encode(r), {}) for r in data]
      with mock.patch('apache_beam.io.WriteToPubSub',
                      FakeWriteToPubSub(topic='my_topic',
                                        messages=expected_messages)):
        _ = (
            p | beam.Create(data) | YamlTransform(
                '''
            type: WriteToPubSub
            config:
              topic: my_topic
              format: PROTO
            '''))

  def test_read_proto(self):
    with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
        pickle_library='cloudpickle')) as p:
      data = [beam.Row(label='37a', rank=1), beam.Row(label='389a', rank=2)]
      coder = RowCoder(
          schema_utils.named_fields_to_schema([('label', str), ('rank', int)]))
      expected_messages = [PubsubMessage(coder.encode(r), {}) for r in data]
      with mock.patch('apache_beam.io.ReadFromPubSub',
                      FakeReadFromPubSub(topic='my_topic',
                                         messages=expected_messages)):
        result = p | YamlTransform(
            '''
            type: ReadFromPubSub
            config:
              topic: my_topic
              format: PROTO
              schema:
                type: object
                properties:
                  label: {type: string}
                  rank: {type: integer}
            ''')
        assert_that(result, equal_to(data))


class YamlMatchAllTest(unittest.TestCase):
  def test_match_all_simple(self):
    with tempfile.TemporaryDirectory() as temp_dir:
      file1 = os.path.join(temp_dir, 'file1.txt')
      file2 = os.path.join(temp_dir, 'file2.txt')
      for f in [file1, file2]:
        with open(f, 'w') as fout:
          fout.write('data')

      with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle')) as p:
        result = (
            p
            | beam.Create(
                [beam.Row(pattern=os.path.join(temp_dir, 'file*.txt'))])
            | YamlTransform(
                '''
                type: MatchAll
                config:
                  file_pattern: pattern
                '''))
        paths = result | beam.Map(lambda row: row.path)
        assert_that(paths, equal_to([file1, file2]))

  def test_match_all_single_field_default(self):
    with tempfile.TemporaryDirectory() as temp_dir:
      file1 = os.path.join(temp_dir, 'file1.txt')
      with open(file1, 'w') as fout:
        fout.write('data')

      with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle')) as p:
        result = (
            p
            | beam.Create([beam.Row(my_sole_pattern=file1)])
            | YamlTransform(
                '''
                type: MatchAll
                '''))
        paths = result | beam.Map(lambda row: row.path)
        assert_that(paths, equal_to([file1]))

  def test_match_all_multiple_fields_error(self):
    with self.assertRaises(Exception):
      with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle')) as p:
        _ = (
            p
            | beam.Create([beam.Row(pattern='foo', other_field='bar')])
            | YamlTransform(
                '''
                type: MatchAll
                '''))

  def test_match_all_empty_match_disallow_error(self):
    with self.assertRaises(Exception):
      with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle')) as p:
        _ = (
            p
            | beam.Create([beam.Row(pattern='does_not_exist*.txt')])
            | YamlTransform(
                '''
                type: MatchAll
                config:
                  empty_match_treatment: DISALLOW
                '''))

  def test_match_all_invalid_field_error(self):
    with self.assertRaisesRegex(
        ValueError, "Field 'invalid_field' not found in input schema fields"):
      with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle')) as p:
        _ = (
            p
            | beam.Create([beam.Row(pattern='foo')])
            | YamlTransform(
                '''
                type: MatchAll
                config:
                  file_pattern: invalid_field
                '''))

  def test_match_all_none_timestamp(self):
    from apache_beam.io.filesystem import FileMetadata

    class MockMatchAll(beam.PTransform):
      def expand(self, pcoll):
        return pcoll.pipeline | beam.Create([
            FileMetadata(
                path='file.txt',
                size_in_bytes=100,
                last_updated_in_seconds=None)
        ])

    with mock.patch('apache_beam.io.fileio.MatchAll',
                    return_value=MockMatchAll()):
      with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle')) as p:
        result = (
            p
            | beam.Create([beam.Row(pattern='file.txt')])
            | YamlTransform(
                '''
                type: MatchAll
                '''))
        assert_that(
            result,
            equal_to([
                beam.Row(
                    path='file.txt',
                    size_in_bytes=100,
                    last_updated_in_seconds=None)
            ]))


class ReadFromBigQueryTest(unittest.TestCase):
  def test_query_without_schema_raises(self):
    from apache_beam.yaml.yaml_io import read_from_bigquery
    with self.assertRaisesRegex(ValueError, 'schema'):
      read_from_bigquery(query='SELECT id FROM dataset.table')

  def test_table_without_schema_ok(self):
    import unittest.mock as mock

    from apache_beam.yaml.yaml_io import read_from_bigquery
    with mock.patch('apache_beam.yaml.yaml_io.ReadFromBigQuery') as mock_rfbq:
      mock_rfbq.return_value = mock.MagicMock()
      read_from_bigquery(table='project:dataset.table')
      mock_rfbq.assert_called_once()
      call_kwargs = mock_rfbq.call_args[1]
      self.assertIsNone(call_kwargs.get('query_output_schema'))

  def test_query_with_schema_passes_through(self):
    import unittest.mock as mock

    from apache_beam.yaml.yaml_io import read_from_bigquery
    schema = {
        'fields': [
            {
                'name': 'id', 'type': 'INTEGER', 'mode': 'NULLABLE'
            },
        ]
    }
    with mock.patch('apache_beam.yaml.yaml_io.ReadFromBigQuery') as mock_rfbq:
      mock_rfbq.return_value = mock.MagicMock()
      read_from_bigquery(query='SELECT id FROM dataset.table', schema=schema)
      call_kwargs = mock_rfbq.call_args[1]
      self.assertEqual(call_kwargs['query_output_schema'], schema)

  def test_query_and_table_both_raises(self):
    from apache_beam.yaml.yaml_io import read_from_bigquery
    with self.assertRaises(AssertionError):
      read_from_bigquery(
          table='project:dataset.table',
          query='SELECT id FROM dataset.table',
          schema={'fields': []})


class FakeDicomSearch(beam.PTransform):
  def __init__(
      self, buffer_size=8, max_workers=5, client=None, credential=None):
    self.buffer_size = buffer_size
    self.max_workers = max_workers

  def expand(self, pcoll):
    def do_search(element):
      required = [
          'project_id',
          'region',
          'dataset_id',
          'dicom_store_id',
          'search_type',
      ]
      for key in required:
        if key not in element:
          return {
              'result': [],
              'status': 'Must have %s in the dict.' % key,
              'input': element,
              'success': False,
          }
      if element['search_type'] not in ('instances', 'studies', 'series'):
        return {
            'result': [],
            'status': (
                'Search type can only be "studies", '
                '"instances" or "series"'),
            'input': element,
            'success': False,
        }
      if element.get('project_id') == 'bad_project':
        return {
            'result': [],
            'status': 500,
            'input': element,
            'success': False,
        }
      params = element.get('params') or {}
      result = [{'PatientName': 'Alice', 'params': params}]
      return {
          'result': result,
          'status': 200,
          'input': element,
          'success': True,
      }

    return pcoll | beam.Map(do_search)


def _patch_dicom_search():
  """Install a fake dicomio module so tests do not require GCP extras."""
  import sys
  import types

  dicomio_mod = types.ModuleType('apache_beam.io.gcp.healthcare.dicomio')
  dicomio_mod.DicomSearch = FakeDicomSearch
  return mock.patch.dict(
      sys.modules, {'apache_beam.io.gcp.healthcare.dicomio': dicomio_mod})


class YamlDicomSearchTest(unittest.TestCase):
  def test_dicom_search_success(self):
    with _patch_dicom_search():
      with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle')) as p:
        result = (
            p
            | beam.Create([
                beam.Row(
                    project_id='proj',
                    region='us-central1',
                    dataset_id='dataset',
                    dicom_store_id='store',
                    search_type='instances',
                    params={'PatientName': 'Alice'})
            ])
            | YamlTransform(
                '''
                type: DicomSearch
                '''))
        assert_that(
            result
            | beam.Map(
                lambda row:
                (row.status, json.loads(row.result)[0]['PatientName'])),
            equal_to([('200', 'Alice')]))

  def test_dicom_search_coerces_string_limit_offset(self):
    with _patch_dicom_search():
      with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle')) as p:
        result = (
            p
            | beam.Create([
                beam.Row(
                    project_id='proj',
                    region='us-central1',
                    dataset_id='dataset',
                    dicom_store_id='store',
                    search_type='instances',
                    params={
                        'PatientName': 'Alice', 'limit': '500', 'offset': '0'
                    })
            ])
            | YamlTransform(
                '''
                type: DicomSearch
                '''))
        assert_that(
            result
            | beam.Map(
                lambda row: (
                    row.status, json.loads(row.result)[0]['params']['limit'],
                    json.loads(row.result)[0]['params']['offset'])),
            equal_to([('200', 500, 0)]))

  def test_dicom_search_with_error_handling(self):
    with _patch_dicom_search():
      with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle')) as p:
        result = (
            p
            | beam.Create([
                beam.Row(
                    project_id='proj',
                    region='us-central1',
                    dataset_id='dataset',
                    dicom_store_id='store',
                    search_type='instances'),
                beam.Row(
                    project_id='bad_project',
                    region='us-central1',
                    dataset_id='dataset',
                    dicom_store_id='store',
                    search_type='instances'),
            ])
            | YamlTransform(
                '''
                type: DicomSearch
                config:
                  error_handling:
                    output: errors
                '''))
        assert_that(
            result['good'] | beam.Map(lambda row: row.status),
            equal_to(['200']),
            label='CheckGood')
        assert_that(
            result['errors'] | beam.Map(lambda error: error.msg),
            equal_to(['DicomSearch failed with status: 500']),
            label='CheckErrors')

  def test_dicom_search_without_error_handling_raises(self):
    with self.assertRaises(Exception):
      with _patch_dicom_search():
        with beam.Pipeline(
            options=beam.options.pipeline_options.PipelineOptions(
                pickle_library='cloudpickle')) as p:
          _ = (
              p
              | beam.Create([
                  beam.Row(
                      project_id='bad_project',
                      region='us-central1',
                      dataset_id='dataset',
                      dicom_store_id='store',
                      search_type='instances')
              ])
              | YamlTransform(
                  '''
                  type: DicomSearch
                  '''))


class YamlDeltaTest(unittest.TestCase):
  def test_read_from_delta(self):
    from apache_beam.yaml.yaml_io import read_from_delta
    transform = read_from_delta(
        table="my_table", version=5, timestamp="2026-01-01T00:00:00Z")
    self.assertIsInstance(transform, beam.managed.Read)
    self.assertEqual(transform._source, "delta")

  def test_read_from_delta_cdc(self):
    from apache_beam.yaml.yaml_io import read_from_delta_cdc
    transform = read_from_delta_cdc(
        table="my_table",
        start_version=0,
        end_version=10,
        include_metadata_columns=["_change_type"])
    self.assertIsInstance(transform, beam.managed.Read)
    self.assertEqual(transform._source, "delta_cdc")


@unittest.skipIf(
    not os.path.exists(
        subprocess_server.JavaJarServer.path_to_dev_beam_jar(
            'sdks:java:io:expansion-service:shadowJar')),
    "Requires expansion service jars.")
class YamlKafkaTest(unittest.TestCase):
  def test_read_from_kafka_json_schema_expansion(self):
    # Regression test for https://github.com/apache/beam/issues/35186.
    # Verifies that ReadFromKafka expands with a nested JSON schema map without
    # UnsupportedOperationException.
    p = beam.Pipeline(
        options=beam.options.pipeline_options.PipelineOptions(
            pickle_library='cloudpickle'))
    expand_pipeline(
        p,
        '''
        pipeline:
          type: chain
          transforms:
            - type: ReadFromKafka
              name: ReadFromMyTopic
              config:
                format: JSON
                schema:
                  type: "object"
                  properties:
                    value: { type: "string" }
                topic: test
                bootstrap_servers: kafka:9092
                auto_offset_reset_config: earliest

            - type: LogForTesting
        ''')

  def test_read_from_kafka_avro_schema_expansion(self):
    # Verifies that ReadFromKafka expands with a nested AVRO schema map without
    # UnsupportedOperationException.
    p = beam.Pipeline(
        options=beam.options.pipeline_options.PipelineOptions(
            pickle_library='cloudpickle'))
    _ = p | YamlTransform(
        '''
        type: ReadFromKafka
        config:
          topic: my-topic
          bootstrap_servers: kafka:9092
          format: AVRO
          schema:
            type: record
            name: my_record
            fields:
              - name: bool
                type: boolean
        ''')


class WriteToBigQueryTest(unittest.TestCase):
  def test_write_file_loads(self):
    from apache_beam.io import WriteToBigQuery
    from apache_beam.yaml.yaml_io import write_to_bigquery

    captured = {}

    class FakeWriteToBigQuery(beam.PTransform):
      Method = WriteToBigQuery.Method

      def __init__(self, table, **kwargs):
        captured['table'] = table
        captured['kwargs'] = kwargs

      def expand(self, pcoll):
        res = mock.MagicMock()
        res._method = WriteToBigQuery.Method.FILE_LOADS
        res.destination_load_jobid_pairs = pcoll | beam.Map(
            lambda _: ('dest', 'job_id'))
        return res

    with mock.patch('apache_beam.yaml.yaml_io.WriteToBigQuery',
                    FakeWriteToBigQuery):
      with beam.Pipeline() as p:
        out = p | beam.Create([beam.Row(id=1)
                               ]) | write_to_bigquery(table='proj:ds.tbl')
        assert_that(out['post_write'], equal_to([]))

    self.assertEqual(captured['table'], 'proj:ds.tbl')
    self.assertIsNone(captured['kwargs']['method'])

  def test_write_with_error_handling_uses_storage_write_api(self):
    from apache_beam.io import WriteToBigQuery
    from apache_beam.yaml.yaml_io import write_to_bigquery

    captured = {}

    class FakeWriteToBigQuery(beam.PTransform):
      Method = WriteToBigQuery.Method

      def __init__(self, table, **kwargs):
        captured['kwargs'] = kwargs

      def expand(self, pcoll):
        res = mock.MagicMock()
        res._method = WriteToBigQuery.Method.STORAGE_WRITE_API
        res.failed_rows_with_errors = pcoll | beam.Map(
            lambda r: beam.Row(row=r.id, error_message='bad row'))
        return res

    with mock.patch('apache_beam.yaml.yaml_io.WriteToBigQuery',
                    FakeWriteToBigQuery):
      with beam.Pipeline() as p:
        out = (
            p
            | beam.Create([beam.Row(id=42)])
            | write_to_bigquery(
                table='proj:ds.tbl', error_handling={'output': 'bq_errors'}))
        assert_that(out['post_write'], equal_to([]), label='CheckPostWrite')
        assert_that(
            out['bq_errors'],
            equal_to([beam.Row(row=42, error_message='bad row')]),
            label='CheckErrors')

    self.assertEqual(
        captured['kwargs']['method'], WriteToBigQuery.Method.STORAGE_WRITE_API)

  def test_write_streaming_without_error_handling_raises_on_failed_row(self):
    from apache_beam.io import WriteToBigQuery
    from apache_beam.yaml.yaml_io import write_to_bigquery

    class FakeWriteToBigQuery(beam.PTransform):
      Method = WriteToBigQuery.Method

      def __init__(self, table, **kwargs):
        pass

      def expand(self, pcoll):
        res = mock.MagicMock()
        res._method = WriteToBigQuery.Method.STORAGE_WRITE_API
        res.failed_rows_with_errors = pcoll | beam.Map(
            lambda _: beam.Row(error_message='permanent bq failure'))
        return res

    with mock.patch('apache_beam.yaml.yaml_io.WriteToBigQuery',
                    FakeWriteToBigQuery):
      with self.assertRaisesRegex(RuntimeError, 'permanent bq failure'):
        with beam.Pipeline() as p:
          _ = p | beam.Create([beam.Row(id=1)
                               ]) | write_to_bigquery(table='proj:ds.tbl')


@unittest.skipIf(jsonschema is None, "Yaml dependencies not installed")
class YamlTFRecordTest(unittest.TestCase):
  def test_write_and_read_tfrecord(self):
    with tempfile.TemporaryDirectory() as tmpdir:
      prefix = os.path.join(tmpdir, 'data')
      with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle')) as p:
        _ = (
            p
            | beam.Create(
                [beam.Row(payload=b'first'), beam.Row(payload=b'second')])
            | YamlTransform(
                f'''
                type: WriteToTFRecord
                config:
                  file_path_prefix: "{prefix}"
                  file_name_suffix: ".tfrecord"
                  num_shards: 1
                '''))

      with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle')) as p:
        result = p | YamlTransform(
            f'''
            type: ReadFromTFRecord
            config:
              file_pattern: "{prefix}*.tfrecord"
            ''')
        assert_that(
            result,
            equal_to([beam.Row(record=b'first'), beam.Row(record=b'second')]))

  def test_write_to_tfrecord_schema_validation(self):
    from apache_beam.yaml.yaml_io import write_to_tfrecord

    with self.assertRaisesRegex(ValueError, 'exactly one field'):
      with beam.Pipeline() as p:
        _ = (
            p
            | beam.Create([beam.Row(a=b'1', b=b'2')])
            | write_to_tfrecord(file_path_prefix='/tmp/unused'))

    with self.assertRaisesRegex(ValueError, 'exactly one field'):
      with beam.Pipeline() as p:
        _ = (
            p
            | beam.Create([b'raw_bytes'])
            | write_to_tfrecord(file_path_prefix='/tmp/unused'))


class _AsDictObj:
  def __init__(self, data):
    self._data = data

  def as_dict(self):
    return self._data


@unittest.skipIf(jsonschema is None, "Yaml dependencies not installed")
class YamlMongoDBTest(unittest.TestCase):
  def test_read_from_mongodb_string_and_dict_configs(self):
    captured = {}

    def fake_read(uri=None, db=None, coll=None, filter=None):
      captured['args'] = (uri, db, coll, filter)
      return beam.Create([
          {
              'name': 'Alice', 'score': 10
          },
          {
              'name': 'Bob', 'score': 20
          },
      ])

    schema_str = json.dumps({
        'type': 'object',
        'properties': {
            'name': {
                'type': 'string'
            },
            'score': {
                'type': 'integer'
            },
        },
        'required': ['name', 'score'],
    })

    with mock.patch('apache_beam.io.mongodbio.ReadFromMongoDB',
                    side_effect=fake_read):
      with beam.Pipeline(options=beam.options.pipeline_options.PipelineOptions(
          pickle_library='cloudpickle')) as p:
        result = p | YamlTransform(
            f'''
            type: ReadFromMongoDB
            config:
              uri: "mongodb://localhost:27017"
              database: test_db
              collection: users
              schema: '{schema_str}'
              filter: '{{"score": {{"$gte": 10}}}}'
            ''')
        assert_that(
            result,
            equal_to([
                beam.Row(name='Alice', score=10),
                beam.Row(name='Bob', score=20),
            ]))

    self.assertEqual(
        captured['args'],
        (
            'mongodb://localhost:27017',
            'test_db',
            'users', {
                'score': {
                    '$gte': 10
                }
            }))

  def test_write_to_mongodb_converts_nested_rows(self):
    from apache_beam.yaml.yaml_io import write_to_mongodb

    expected_docs = [
        {
            'name': 'Alice',
            'nothing': None,
            'nested': {
                'k': 'v'
            },
            'custom': {
                'x': 1
            },
            'items': [{
                'item_id': 2
            }],
            'tags': {
                'env': 'prod'
            },
        },
        {
            'name': 'Bob',
            'nothing': 'present',
            'nested': {
                'k': 'v2'
            },
            'custom': {
                'x': 2
            },
            'items': [],
            'tags': {},
        },
    ]

    def fake_write(uri=None, db=None, coll=None, batch_size=1024):
      self.assertEqual(uri, 'mongodb://localhost:27017')
      self.assertEqual(db, 'test_db')
      self.assertEqual(coll, 'events')
      self.assertEqual(batch_size, 64)
      return AssertThat(equal_to(expected_docs))

    with mock.patch('apache_beam.io.mongodbio.WriteToMongoDB',
                    side_effect=fake_write):
      with beam.Pipeline() as p:
        _ = (
            p
            | beam.Create([
                beam.Row(
                    name='Alice',
                    nothing=None,
                    nested=beam.Row(k='v'),
                    custom=_AsDictObj({'x': 1}),
                    items=[beam.Row(item_id=2)],
                    tags={'env': 'prod'}),
                beam.Row(
                    name='Bob',
                    nothing='present',
                    nested=beam.Row(k='v2'),
                    custom=_AsDictObj({'x': 2}),
                    items=[],
                    tags={}),
            ])
            | write_to_mongodb(
                uri='mongodb://localhost:27017',
                database='test_db',
                collection='events',
                batch_size=64))


class YamlIcebergTest(unittest.TestCase):
  def test_read_from_iceberg(self):
    from apache_beam.yaml.yaml_io import read_from_iceberg
    transform = read_from_iceberg(
        table='db.tbl', catalog_name='local', filter='id > 0', keep=['id'])
    self.assertIsInstance(transform, beam.managed.Read)
    self.assertEqual(transform._source, 'iceberg')

  def test_write_to_iceberg(self):
    from apache_beam.yaml.yaml_io import write_to_iceberg
    transform = write_to_iceberg(
        table='db.tbl', catalog_name='local', partition_fields=['dt'])
    self.assertIsInstance(transform, beam.managed.Write)
    self.assertEqual(transform._sink, 'iceberg')


class YamlIoValidationTest(unittest.TestCase):
  def test_write_to_text_schema_validation(self):
    from apache_beam.yaml.yaml_io import write_to_text

    with self.assertRaisesRegex(ValueError, 'exactly one field'):
      with beam.Pipeline() as p:
        _ = p | beam.Create([beam.Row(a=1, b=2)]) | write_to_text(path='/tmp/x')

    with self.assertRaisesRegex(ValueError, 'exactly one field'):
      with beam.Pipeline() as p:
        _ = p | beam.Create(['untyped']) | write_to_text(path='/tmp/x')

  def test_parser_and_formatter_validation(self):
    from apache_beam.portability.api import schema_pb2
    from apache_beam.yaml.yaml_io import _create_formatter
    from apache_beam.yaml.yaml_io import _create_parser

    with self.assertRaisesRegex(ValueError, 'requires valid JSON schema'):
      _create_parser('JSON', None)
    with self.assertRaisesRegex(ValueError, 'RAW format does not take'):
      _create_parser('RAW', {'type': 'object'})
    with self.assertRaisesRegex(ValueError, 'STRING format does not take'):
      _create_parser('STRING', {'type': 'object'})
    with self.assertRaisesRegex(ValueError, 'Unknown format: BOGUS'):
      _create_parser('BOGUS', None)

    one_field_schema = schema_pb2.Schema(
        fields=[schema_utils.schema_field('payload', str)])
    two_field_schema = schema_pb2.Schema(
        fields=[
            schema_utils.schema_field('a', str),
            schema_utils.schema_field('b', str),
        ])

    with self.assertRaisesRegex(ValueError, 'RAW format does not take'):
      _create_formatter('raw', {'type': 'object'}, one_field_schema)
    with self.assertRaisesRegex(ValueError, 'Expecting exactly one field'):
      _create_formatter('raw', None, two_field_schema)
    with self.assertRaisesRegex(ValueError, 'Unknown format: BOGUS'):
      _create_formatter('BOGUS', None, one_field_schema)

    raw_formatter = _create_formatter('raw', None, one_field_schema)
    self.assertEqual(raw_formatter(beam.Row(payload='hello')), b'hello')
    with self.assertRaisesRegex(ValueError, 'Cannot encode payload'):
      raw_formatter(beam.Row(payload=12345))

  def test_pubsub_argument_validation(self):
    from apache_beam.yaml.yaml_io import read_from_pubsub
    from apache_beam.yaml.yaml_io import write_to_pubsub

    with self.assertRaisesRegex(TypeError,
                                'Only one of topic and subscription'):
      with beam.Pipeline() as p:
        _ = p | read_from_pubsub(topic='t', subscription='s', format='RAW')

    with self.assertRaisesRegex(TypeError, 'One of topic or subscription'):
      with beam.Pipeline() as p:
        _ = p | read_from_pubsub(format='RAW')

    with self.assertRaisesRegex(ValueError, 'Attribute fields.*not found'):
      with beam.Pipeline() as p:
        _ = (
            p
            | beam.Create([beam.Row(payload=b'hi')])
            | write_to_pubsub(
                topic='t', format='RAW', attributes='missing_attr'))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
