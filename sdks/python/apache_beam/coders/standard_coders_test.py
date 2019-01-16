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
from __future__ import absolute_import
from __future__ import print_function

import json
import logging
import os.path
import sys
import unittest
from builtins import map

import yaml

from apache_beam.coders import coder_impl
from apache_beam.coders import coders
from apache_beam.transforms import window
from apache_beam.transforms.window import IntervalWindow
from apache_beam.utils import windowed_value
from apache_beam.utils.timestamp import Timestamp

STANDARD_CODERS_YAML = os.path.join(
    os.path.dirname(__file__), '..', 'testing', 'data', 'standard_coders.yaml')


def _load_test_cases(test_yaml):
  """Load test data from yaml file and return an iterable of test cases.

  See ``standard_coders.yaml`` for more details.
  """
  if not os.path.exists(test_yaml):
    raise ValueError('Could not find the test spec: %s' % test_yaml)
  for ix, spec in enumerate(yaml.load_all(open(test_yaml))):
    spec['index'] = ix
    name = spec.get('name', spec['coder']['urn'].split(':')[-2])
    yield [name, spec]


class StandardCodersTest(unittest.TestCase):

  _urn_to_coder_class = {
      'beam:coder:bytes:v1': coders.BytesCoder,
      'beam:coder:varint:v1': coders.VarIntCoder,
      'beam:coder:kv:v1': lambda k, v: coders.TupleCoder((k, v)),
      'beam:coder:interval_window:v1': coders.IntervalWindowCoder,
      'beam:coder:iterable:v1': lambda t: coders.IterableCoder(t),
      'beam:coder:global_window:v1': coders.GlobalWindowCoder,
      'beam:coder:windowed_value:v1':
          lambda v, w: coders.WindowedValueCoder(v, w),
      'beam:coder:timer:v1': coders._TimerCoder,
  }

  _urn_to_json_value_parser = {
      'beam:coder:bytes:v1': lambda x: x.encode('utf-8'),
      'beam:coder:varint:v1': lambda x: x,
      'beam:coder:kv:v1':
          lambda x, key_parser, value_parser: (key_parser(x['key']),
                                               value_parser(x['value'])),
      'beam:coder:interval_window:v1':
          lambda x: IntervalWindow(
              start=Timestamp(micros=(x['end'] - x['span']) * 1000),
              end=Timestamp(micros=x['end'] * 1000)),
      'beam:coder:iterable:v1': lambda x, parser: list(map(parser, x)),
      'beam:coder:global_window:v1': lambda x: window.GlobalWindow(),
      'beam:coder:windowed_value:v1':
          lambda x, value_parser, window_parser: windowed_value.create(
              value_parser(x['value']), x['timestamp'] * 1000,
              tuple([window_parser(w) for w in x['windows']])),
      'beam:coder:timer:v1':
          lambda x, payload_parser: dict(
              payload=payload_parser(x['payload']),
              timestamp=Timestamp(micros=x['timestamp'])),
  }

  def test_standard_coders(self):
    for name, spec in _load_test_cases(STANDARD_CODERS_YAML):
      logging.info('Executing %s test.', name)
      self._run_standard_coder(name, spec)

  def _run_standard_coder(self, name, spec):
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
            self.assertEqual(decode_nested(coder, expected_encoded, nested),
                             value)
        else:
          # Only verify decoding for a non-deterministic coder
          self.assertEqual(decode_nested(coder, expected_encoded, nested),
                           value)

  def parse_coder(self, spec):
    return self._urn_to_coder_class[spec['urn']](
        *[self.parse_coder(c) for c in spec.get('components', ())])

  def json_value_parser(self, coder_spec):
    component_parsers = [
        self.json_value_parser(c) for c in coder_spec.get('components', ())]
    return lambda x: self._urn_to_json_value_parser[coder_spec['urn']](
        x, *component_parsers)

  # Used when --fix is passed.

  fix = False
  to_fix = {}

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
