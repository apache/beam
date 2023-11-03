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
import json
import logging
import unittest

import fastavro
import mock

import apache_beam as beam
from apache_beam.io.gcp.pubsub import PubsubMessage
from apache_beam.testing.util import AssertThat
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
from apache_beam.yaml.yaml_transform import YamlTransform


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
              format: raw
            ''')
        assert_that(
            result,
            equal_to([beam.Row(payload=b'msg1'), beam.Row(payload=b'msg2')]))

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
              format: raw
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
              format: raw
              attributes_map: attrMap
            ''')
        assert_that(
            result,
            equal_to([
                beam.Row(payload=b'msg1', attrMap={'attr': 'value1'}),
                beam.Row(payload=b'msg2', attrMap={'attr': 'value2'})
            ]))

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
              format: raw
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
              format: avro
              schema: %s
            ''' % json.dumps(self._avro_schema))
        assert_that(
            result,
            equal_to(
                [beam.Row(label='37a', rank=1),  # linebreak
                 beam.Row(label='389a', rank=2)]))

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
              format: json
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
              format: json
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
                format: json
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
              format: json
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
              format: raw
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
              format: raw
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
              format: raw
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
              format: raw
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
              format: avro
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
              format: json
              attributes: [label]
              attributes_map: other
            '''))


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
