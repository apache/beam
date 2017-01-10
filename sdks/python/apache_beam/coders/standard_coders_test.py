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

"""Unit tests for coders that must be consistent across all beam SDKs.
"""

import collections
import os.path
import pickle
import unittest

import yaml

import apache_beam as beam
from apache_beam import coders
from apache_beam.coders import coder_impl


class StandardCodersTest(unittest.TestCase):

  _urn_to_coder_class = {
    'beam:coders:bytes:0.1': coders.BytesCoder,
    'beam:coders:varint:0.1': coders.VarIntCoder,
    'beam:coders:kv:0.1': lambda k, v: coders.TupleCoder((k, v))
  }

  _urn_to_json_value_parser = {
    'beam:coders:bytes:0.1': lambda x: x,
    'beam:coders:varint:0.1': lambda x: x,
    'beam:coders:kv:0.1':
        lambda x, key_parser, value_parser: (key_parser(x['key']),
                                             value_parser(x['value']))
  }

  # We must prepend an underscore to this name so that the open-source unittest
  # runner does not execute this method directly as a test.
  @classmethod
  def _create_test(cls, spec):
    counter = 0
    name = spec.get('name', spec['coder']['urn'].split(':')[-2])
    unique_name = 'test_' + name
    while hasattr(cls, unique_name):
      counter += 1
      unique_name = 'test_%s_%d' % (name, counter)
    setattr(cls, unique_name, lambda self: self._run_coder_test(spec))

  # We must prepend an underscore to this name so that the open-source unittest
  # runner does not execute this method directly as a test.
  @classmethod
  def _create_tests(cls, coder_test_specs):
    for spec in yaml.load_all(open(coder_test_specs)):
      cls._create_test(spec)

  def _run_coder_test(self, spec):
    coder = self.parse_coder(spec['coder'])
    parse_value = self.json_value_parser(spec['coder'])
    for encoded, json_value in spec['examples'].items():
      value = parse_value(json_value)
      if spec.get('nested', True):
        self.assertEqual(decode_nested(coder, encoded), value)
        self.assertEqual(encoded, encode_nested(coder, value))
      if not spec.get('nested', False):
        self.assertEqual(coder.decode(encoded), value)
        self.assertEqual(encoded, coder.encode(value))

  def parse_coder(self, spec):
    return self._urn_to_coder_class[spec['urn']](
        *[self.parse_coder(c) for c in spec.get('components', ())])

  def json_value_parser(self, coder_spec):
    component_parsers = [
        self.json_value_parser(c) for c in coder_spec.get('components', ())]
    return lambda x: self._urn_to_json_value_parser[coder_spec['urn']](
        x, *component_parsers)


STANDARD_CODERS_YAML = os.path.join(
    os.path.dirname(__file__), 'standard_coders.yaml')
if os.path.exists(STANDARD_CODERS_YAML):
  StandardCodersTest._create_tests(STANDARD_CODERS_YAML)


def encode_nested(coder, value):
  out = coder_impl.create_OutputStream()
  coder.get_impl().encode_to_stream(value, out, True)
  return out.get()

def decode_nested(coder, encoded):
  return coder.get_impl().decode_from_stream(
      coder_impl.create_InputStream(encoded), True)


if __name__ == '__main__':
  unittest.main()
