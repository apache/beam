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

"""Property tests for coders in the Python SDK.

The tests in this file utilize the hypothesis library to generate random test
cases and run them against Beam's coder implementations.

These tests are similar to fuzzing, except they test invariant properties
of code.
"""

import keyword
import math
import typing
import unittest
# TODO(pabloem): Include other categories of characters
from datetime import datetime
from string import ascii_letters
from string import digits

import numpy as np
from hypothesis import strategies as st
from hypothesis import assume
from hypothesis import given
from hypothesis import settings
from pytz import utc

from apache_beam.coders import FloatCoder
from apache_beam.coders import RowCoder
from apache_beam.coders import StrUtf8Coder
from apache_beam.coders.typecoders import registry as coders_registry
from apache_beam.typehints.schemas import PRIMITIVE_TO_ATOMIC_TYPE
from apache_beam.typehints.schemas import typing_to_runner_api
from apache_beam.utils.timestamp import Timestamp

SCHEMA_TYPES_TO_STRATEGY = {
    str: st.text(),
    bytes: st.binary(),
    typing.ByteString: st.binary(),
    # Maximum datetime on year 3000 to conform to Windows OS limits.
    Timestamp: st.datetimes(
        min_value=datetime(1970, 1, 1, 1, 1),
        max_value=datetime(
            3000, 1, 1, 0,
            0)).map(lambda dt: Timestamp.from_utc_datetime(dt.astimezone(utc))),
    int: st.integers(min_value=-(1 << 63 - 1), max_value=1 << 63 - 1),
    # INT8/BYTE not yet supported by RowCoder.
    # np.int8: st.binary(min_size=1, max_size=1),
    # INT16 not yet supported by RowCoder.
    # np.int16: st.integers(min_value=-(1 << 15 - 1), max_value=1 << 15 - 1),
    np.int32: st.integers(min_value=-(1 << 31 - 1), max_value=1 << 31 - 1),
    np.int64: st.integers(min_value=-(1 << 63 - 1), max_value=1 << 63 - 1),
    np.uint32: st.integers(min_value=0, max_value=1 << 32 - 1),
    np.uint64: st.integers(min_value=0, max_value=1 << 64 - 1),
    np.float32: st.floats(width=32, allow_nan=False),
    np.float64: st.floats(width=64, allow_nan=False),
    float: st.floats(width=64, allow_nan=False),
    bool: st.booleans()
}

# TODO(https://github.com/apache/beam/issues/23003): Support logical types.
SCHEMA_TYPES = list(SCHEMA_TYPES_TO_STRATEGY.keys())

# A hypothesis strategy that generates schemas.
# A schema is a list containing tuples of strings (field names), types (field
# types) and boolean (nullable or not).
# This strategy currently generates rows with simple types (i.e. non-list, and
# non-map fields).
SCHEMA_GENERATOR_STRATEGY = st.lists(
    st.tuples(
        st.text(ascii_letters + digits + '_', min_size=1),
        st.sampled_from(SCHEMA_TYPES),
        st.booleans()))

TYPES_UNSUPPORTED_BY_ROW_CODER = {np.int8, np.int16}


class TypesAreAllTested(unittest.TestCase):
  def test_all_types_are_tested(self):
    # Verify that all types among Beam's defined types are being tested
    self.assertEqual(
        set(SCHEMA_TYPES).intersection(PRIMITIVE_TO_ATOMIC_TYPE.keys()),
        set(PRIMITIVE_TO_ATOMIC_TYPE.keys()).difference(
            TYPES_UNSUPPORTED_BY_ROW_CODER))


class ProperyTestingCoders(unittest.TestCase):
  @given(st.text())
  def test_string_coder(self, txt: str):
    coder = StrUtf8Coder()
    self.assertEqual(coder.decode(coder.encode(txt)), txt)

  @given(st.floats())
  def test_float_coder(self, num: float):
    coder = FloatCoder()
    test_num = coder.decode(coder.encode(num))
    if math.isnan(num):
      # nan != nan.
      self.assertTrue(math.isnan(test_num))
    else:
      self.assertEqual(coder.decode(coder.encode(num)), num)

  @settings(deadline=None, print_blob=True)
  @given(st.data())
  def test_row_coder(self, data: st.DataObject):
    """Generate rows and schemas, and test their encoding/decoding.

    The schemas are generated based on the SCHEMA_GENERATOR_STRATEGY.
    """
    schema = data.draw(SCHEMA_GENERATOR_STRATEGY)
    # Assume that the cardinality of the set of names is the same
    # as the length of the schema. This means there's no duplicate
    # names for fields.
    # If this condition does not hold, then we must not continue the
    # test.
    assume(len({name for name, _, _ in schema}) == len(schema))
    assume(all(not keyword.iskeyword(name) for name, _, _ in schema))
    assume(
        len({n[0]
             for n, _, _ in schema}.intersection(set(digits + '_'))) == 0)
    RowType = typing.NamedTuple(  # type: ignore
        'RandomRowType',
        [(name, type_ if not nullable else typing.Optional[type_]) for name,
         type_,
         nullable in schema])
    coders_registry.register_coder(RowType, RowCoder)

    # TODO(https://github.com/apache/beam/issues/23002): Apply nulls for these
    row = RowType(
        **{
            name: data.draw(SCHEMA_TYPES_TO_STRATEGY[type_])
            for name, type_, nullable in schema
        })

    coder = RowCoder(typing_to_runner_api(RowType).row_type.schema)
    self.assertEqual(coder.decode(coder.encode(row)), row)


if __name__ == "__main__":
  unittest.main()
