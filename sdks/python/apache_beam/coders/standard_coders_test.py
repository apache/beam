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

"""Unit tests for coders that must be consistent across all Beam SDKs.
"""
# pytype: skip-file

from __future__ import absolute_import
from __future__ import print_function

import json
import logging
import math
import os.path
import sys
import unittest
from builtins import map
from copy import deepcopy
from typing import Dict
from typing import Tuple

import yaml

from apache_beam.coders import coder_impl
from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability.api import schema_pb2
from apache_beam.runners import pipeline_context
from apache_beam.transforms import userstate
from apache_beam.transforms import window
from apache_beam.transforms.window import IntervalWindow
from apache_beam.typehints import schemas
from apache_beam.utils import windowed_value
from apache_beam.utils.timestamp import Timestamp
from apache_beam.utils.windowed_value import PaneInfo
from apache_beam.utils.windowed_value import PaneInfoTiming

STANDARD_CODERS_YAML = os.path.normpath(
    os.path.join(
        os.path.dirname(__file__), '../portability/api/standard_coders.yaml'))


def _load_test_cases(test_yaml):
  """Load test data from yaml file and return an iterable of test cases.

  See ``standard_coders.yaml`` for more details.
  """
  if not os.path.exists(test_yaml):
    raise ValueError('Could not find the test spec: %s' % test_yaml)
  with open(test_yaml, 'rb') as coder_spec:
    for ix, spec in enumerate(
        yaml.load_all(coder_spec, Loader=yaml.SafeLoader)):
      spec['index'] = ix
      name = spec.get('name', spec['coder']['urn'].split(':')[-2])
      yield [name, spec]


def parse_float(s):
  x = float(s)
  if math.isnan(x):
    # In Windows, float('NaN') has opposite sign from other platforms.
    # For the purpose of this test, we just need consistency.
    x = abs(x)
  return x


def value_parser_from_schema(schema):
  def attribute_parser_from_type(type_):
    parser = nonnull_attribute_parser_from_type(type_)
    if type_.nullable:
      return lambda x: None if x is None else parser(x)
    else:
      return parser

  def nonnull_attribute_parser_from_type(type_):
    # TODO: This should be exhaustive
    type_info = type_.WhichOneof("type_info")
    if type_info == "atomic_type":
      if type_.atomic_type == schema_pb2.BYTES:
        return lambda x: x.encode("utf-8")
      else:
        return schemas.ATOMIC_TYPE_TO_PRIMITIVE[type_.atomic_type]
    elif type_info == "array_type":
      element_parser = attribute_parser_from_type(type_.array_type.element_type)
      return lambda x: list(map(element_parser, x))
    elif type_info == "map_type":
      key_parser = attribute_parser_from_type(type_.map_type.key_type)
      value_parser = attribute_parser_from_type(type_.map_type.value_type)
      return lambda x: dict(
          (key_parser(k), value_parser(v)) for k, v in x.items())
    elif type_info == "logical_type":
      # In YAML logical types are represented with their representation types.
      to_language_type = schemas.LogicalType.from_runner_api(
          type_.logical_type).to_language_type
      parse_representation = attribute_parser_from_type(
          type_.logical_type.representation)
      return lambda x: to_language_type(parse_representation(x))

  parsers = [(field.name, attribute_parser_from_type(field.type))
             for field in schema.fields]

  constructor = schemas.named_tuple_from_schema(schema)

  def value_parser(x):
    result = []
    x = deepcopy(x)
    for name, parser in parsers:
      value = x.pop(name)
      result.append(None if value is None else parser(value))

    if len(x):
      raise ValueError(
          "Test data contains attributes that don't exist in the schema: {}".
          format(', '.join(x.keys())))

    return constructor(*result)

  return value_parser


class StandardCodersTest(unittest.TestCase):

  _urn_to_json_value_parser = {
      'beam:coder:bytes:v1': lambda x: x.encode('utf-8'),
      'beam:coder:bool:v1': lambda x: x,
      'beam:coder:string_utf8:v1': lambda x: x,
      'beam:coder:varint:v1': lambda x: x,
      'beam:coder:kv:v1': lambda x,
      key_parser,
      value_parser: (key_parser(x['key']), value_parser(x['value'])),
      'beam:coder:interval_window:v1': lambda x: IntervalWindow(
          start=Timestamp(micros=(x['end'] - x['span']) * 1000),
          end=Timestamp(micros=x['end'] * 1000)),
      'beam:coder:iterable:v1': lambda x,
      parser: list(map(parser, x)),
      'beam:coder:global_window:v1': lambda x: window.GlobalWindow(),
      'beam:coder:windowed_value:v1': lambda x,
      value_parser,
      window_parser: windowed_value.create(
          value_parser(x['value']),
          x['timestamp'] * 1000,
          tuple([window_parser(w) for w in x['windows']])),
      'beam:coder:param_windowed_value:v1': lambda x,
      value_parser,
      window_parser: windowed_value.create(
          value_parser(x['value']),
          x['timestamp'] * 1000,
          tuple([window_parser(w) for w in x['windows']]),
          PaneInfo(
              x['pane']['is_first'],
              x['pane']['is_last'],
              PaneInfoTiming.from_string(x['pane']['timing']),
              x['pane']['index'],
              x['pane']['on_time_index'])),
      'beam:coder:timer:v1': lambda x,
      value_parser,
      window_parser: userstate.Timer(
          user_key=value_parser(x['userKey']),
          dynamic_timer_tag=x['dynamicTimerTag'],
          clear_bit=x['clearBit'],
          windows=tuple([window_parser(w) for w in x['windows']]),
          fire_timestamp=None,
          hold_timestamp=None,
          paneinfo=None) if x['clearBit'] else userstate.Timer(
              user_key=value_parser(x['userKey']),
              dynamic_timer_tag=x['dynamicTimerTag'],
              clear_bit=x['clearBit'],
              fire_timestamp=Timestamp(micros=x['fireTimestamp'] * 1000),
              hold_timestamp=Timestamp(micros=x['holdTimestamp'] * 1000),
              windows=tuple([window_parser(w) for w in x['windows']]),
              paneinfo=PaneInfo(
                  x['pane']['is_first'],
                  x['pane']['is_last'],
                  PaneInfoTiming.from_string(x['pane']['timing']),
                  x['pane']['index'],
                  x['pane']['on_time_index'])),
      'beam:coder:double:v1': parse_float,
  }

  def test_standard_coders(self):
    for name, spec in _load_test_cases(STANDARD_CODERS_YAML):
      logging.info('Executing %s test.', name)
      self._run_standard_coder(name, spec)

  def _run_standard_coder(self, name, spec):
    def assert_equal(actual, expected):
      """Handle nan values which self.assertEqual fails on."""
      if (isinstance(actual, float) and isinstance(expected, float) and
          math.isnan(actual) and math.isnan(expected)):
        return
      self.assertEqual(actual, expected)

    coder = self.parse_coder(spec['coder'])
    parse_value = self.json_value_parser(spec['coder'])
    nested_list = [spec['nested']] if 'nested' in spec else [True, False]
    for nested in nested_list:
      for expected_encoded, json_value in spec['examples'].items():
        value = parse_value(json_value)
        expected_encoded = expected_encoded.encode('latin1')
        if not spec['coder'].get('non_deterministic', False):
          actual_encoded = encode_nested(coder, value, nested)
          if self.fix and actual_encoded != expected_encoded:
            self.to_fix[spec['index'], expected_encoded] = actual_encoded
          else:
            self.assertEqual(expected_encoded, actual_encoded)
            decoded = decode_nested(coder, expected_encoded, nested)
            assert_equal(decoded, value)
        else:
          # Only verify decoding for a non-deterministic coder
          self.assertEqual(
              decode_nested(coder, expected_encoded, nested), value)

  def parse_coder(self, spec):
    context = pipeline_context.PipelineContext()
    coder_id = str(hash(str(spec)))
    component_ids = [
        context.coders.get_id(self.parse_coder(c))
        for c in spec.get('components', ())
    ]
    context.coders.put_proto(
        coder_id,
        beam_runner_api_pb2.Coder(
            spec=beam_runner_api_pb2.FunctionSpec(
                urn=spec['urn'],
                payload=spec.get('payload', '').encode('latin1')),
            component_coder_ids=component_ids))
    return context.coders.get_by_id(coder_id)

  def json_value_parser(self, coder_spec):
    # TODO: integrate this with the logic for the other parsers
    if coder_spec['urn'] == 'beam:coder:row:v1':
      schema = schema_pb2.Schema.FromString(
          coder_spec['payload'].encode('latin1'))
      return value_parser_from_schema(schema)

    component_parsers = [
        self.json_value_parser(c) for c in coder_spec.get('components', ())
    ]
    return lambda x: self._urn_to_json_value_parser[coder_spec['urn']](
        x, *component_parsers)

  # Used when --fix is passed.

  fix = False
  to_fix = {}  # type: Dict[Tuple[int, bytes], bytes]

  @classmethod
  def tearDownClass(cls):
    if cls.fix and cls.to_fix:
      print("FIXING", len(cls.to_fix), "TESTS")
      doc_sep = '\n---\n'
      docs = open(STANDARD_CODERS_YAML).read().split(doc_sep)

      def quote(s):
        return json.dumps(s.decode('latin1')).replace(r'\u0000', r'\0')

      for (doc_ix, expected_encoded), actual_encoded in cls.to_fix.items():
        print(quote(expected_encoded), "->", quote(actual_encoded))
        docs[doc_ix] = docs[doc_ix].replace(
            quote(expected_encoded) + ':', quote(actual_encoded) + ':')
      open(STANDARD_CODERS_YAML, 'w').write(doc_sep.join(docs))


def encode_nested(coder, value, nested=True):
  out = coder_impl.create_OutputStream()
  coder.get_impl().encode_to_stream(value, out, nested)
  return out.get()


def decode_nested(coder, encoded, nested=True):
  return coder.get_impl().decode_from_stream(
      coder_impl.create_InputStream(encoded), nested)


if __name__ == '__main__':
  if '--fix' in sys.argv:
    StandardCodersTest.fix = True
    sys.argv.remove('--fix')
  unittest.main()
