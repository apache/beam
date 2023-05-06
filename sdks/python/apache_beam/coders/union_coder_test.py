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
# pytype: skip-file

import logging
import pytest
import unittest

from apache_beam.coders import coders
from apache_beam.coders.union_coder import UnionCoder
from apache_beam.coders.avro_record import AvroRecord


class AvroTestCoder(coders.AvroGenericCoder):
  SCHEMA = """
  {
    "type": "record", "name": "test",
    "fields": [
      {"name": "name", "type": "string"},
      {"name": "age", "type": "int"}
    ]
  }
  """

  def __init__(self):
    super().__init__(self.SCHEMA)


class AvroTestCoder1(coders.AvroGenericCoder):
  SCHEMA = """
  {
    "type": "record", "name": "test1",
    "fields": [
      {"name": "name", "type": "string"}
    ]
  }
  """

  def __init__(self):
    super().__init__(self.SCHEMA)


class UnionCoderTest(unittest.TestCase):
  def test_basics(self):
    coder_0 = UnionCoder([
        coders.StrUtf8Coder(),
        coders.VarIntCoder(),
    ])
    coder = UnionCoder([
        coders.StrUtf8Coder(),
        coders.VarIntCoder(),
        coders.FloatCoder(),
    ])
    assert coder != coder_0

    encoded_size = [2, 2, 9]
    for v, es in zip(["8", 8, 8.0], encoded_size):
      self.assertEqual(v, coder.decode(coder.encode(v)))
      self.assertEqual(coder.estimate_size(v), es)

    assert hash(coder)

    with pytest.raises(ValueError):
      coder.encode(True)

    with pytest.raises(ValueError):
      coder.decode(0)

  def test_custom_coder(self):

    coder = UnionCoder([AvroTestCoder(), AvroTestCoder1()])

    self.assertEqual(coder.is_deterministic(), False)

    assert coder.to_type_hint()
    assert str(coder) == 'UnionCoder[AvroTestCoder, AvroTestCoder1]'

    ar = AvroRecord({"name": "Daenerys targaryen", "age": 23})
    self.assertEqual(coder.decode(coder.encode(ar)).record, ar.record)

    ar1 = AvroRecord({"name": "Daenerys targaryen"})
    self.assertEqual(coder.decode(coder.encode(ar1)).record, ar1.record)


if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  unittest.main()
