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

"""Result of Query3."""
from apache_beam.coders import coder_impl
from apache_beam.coders.coders import FastCoder
from apache_beam.coders.coders import StrUtf8Coder
from apache_beam.testing.benchmarks.nexmark import nexmark_util


class NameCiyStateIdCoder(FastCoder):
  def _create_impl(self):
    return NameCiyStateIdCoderImpl()

  def is_deterministic(self):
    return True


class NameCiyStateId(object):
  CODER = NameCiyStateIdCoder()

  def __init__(self, name, city, state, id):
    self.name = name
    self.city = city
    self.state = state
    self.auction_id = id

  def __repr__(self):
    return nexmark_util.model_to_json(self)


class NameCiyStateIdCoderImpl(coder_impl.StreamCoderImpl):
  _str_coder_impl = StrUtf8Coder().get_impl()
  _int_coder_impl = coder_impl.VarIntCoderImpl()

  def encode_to_stream(self, value, stream, nested):
    self._str_coder_impl.encode_to_stream(value.name, stream, True)
    self._str_coder_impl.encode_to_stream(value.city, stream, True)
    self._str_coder_impl.encode_to_stream(value.state, stream, True)
    self._int_coder_impl.encode_to_stream(value.auction_id, stream, True)

  def decode_from_stream(self, stream, nested):

    name = self._str_coder_impl.decode_from_stream(stream, True)
    city = self._str_coder_impl.decode_from_stream(stream, True)
    state = self._str_coder_impl.decode_from_stream(stream, True)
    auction_id = self._int_coder_impl.decode_from_stream(stream, True)
    return NameCiyStateId(name, city, state, auction_id)
