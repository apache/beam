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

import json
import os.path
import sys
import unittest

import yaml

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
    for ix, spec in enumerate(yaml.load_all(open(coder_test_specs))):
      spec['index'] = ix
      cls._create_test(spec)

  def _run_coder_test(self, spec):
    coder = self.parse_coder(spec['coder'])
    parse_value = self.json_value_parser(spec['coder'])
    nested_list = [spec['nested']] if 'nested' in spec else [True, False]
    for nested in nested_list:
      for expected_encoded, json_value in spec['examples'].items():
        value = parse_value(json_value)
        expected_encoded = expected_encoded.encode('latin1')
        actual_encoded = encode_nested(coder, value, nested)
        if self.fix and actual_encoded != expected_encoded:
          self.to_fix[spec['index'], expected_encoded] = actual_encoded
        else:
          self.assertEqual(decode_nested(coder, expected_encoded, nested),
                           value)
          self.assertEqual(expected_encoded, actual_encoded)

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
      print "FIXING", len(cls.to_fix), "TESTS"
      doc_sep = '\n---\n'
      docs = open(STANDARD_CODERS_YAML).read().split(doc_sep)

      def quote(s):
        return json.dumps(s.decode('latin1')).replace(r'\u0000', r'\0')
      for (doc_ix, expected_encoded), actual_encoded in cls.to_fix.items():
        print quote(expected_encoded), "->", quote(actual_encoded)
        docs[doc_ix] = docs[doc_ix].replace(
            quote(expected_encoded) + ':', quote(actual_encoded) + ':')
      open(STANDARD_CODERS_YAML, 'w').write(doc_sep.join(docs))


STANDARD_CODERS_YAML = os.path.join(
    os.path.dirname(__file__), 'standard_coders.yaml')
if os.path.exists(STANDARD_CODERS_YAML):
  StandardCodersTest._create_tests(STANDARD_CODERS_YAML)


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
