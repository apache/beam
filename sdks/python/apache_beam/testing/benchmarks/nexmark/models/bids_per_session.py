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

"""Result of Query11 and 12."""
from __future__ import absolute_import

from apache_beam.coders import coder_impl
from apache_beam.coders.coders import FastCoder
from apache_beam.testing.benchmarks.nexmark import nexmark_util


class BidsPerSessionCoder(FastCoder):
  def _create_impl(self):
    return BidsPerSessionCoderImpl()

  def is_deterministic(self):
    return True


class BidsPerSession(object):
  CODER = BidsPerSessionCoder()

  def __init__(self, bidder_id, bid_count):
    self.bidder_id = bidder_id
    self.bid_count = bid_count

  def __repr__(self):
    return nexmark_util.model_to_json(self)


class BidsPerSessionCoderImpl(coder_impl.StreamCoderImpl):
  _int_coder_impl = coder_impl.VarIntCoderImpl()

  def encode_to_stream(self, value, stream, nested):
    self._int_coder_impl.encode_to_stream(value.bidder_id, stream, True)
    self._int_coder_impl.encode_to_stream(value.bid_count, stream, True)

  def decode_from_stream(self, stream, nested):

    bidder_id = self._int_coder_impl.decode_from_stream(stream, True)
    bid_count = self._int_coder_impl.decode_from_stream(stream, True)
    return BidsPerSession(bidder_id, bid_count)
