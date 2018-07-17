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

"""Create transform for streaming."""

from __future__ import absolute_import

from builtins import map

from apache_beam import DoFn
from apache_beam import ParDo
from apache_beam import PTransform
from apache_beam import Windowing
from apache_beam import pvalue
from apache_beam.transforms.window import GlobalWindows


class StreamingCreate(PTransform):
  """A specialized implementation for ``Create`` transform in streaming mode.

  Note: There is no unbounded source API in python to wrap the Create source,
  so we map this to composite of Impulse primitive and an SDF.
  """

  def __init__(self, values, coder):
    self.coder = coder
    self.encoded_values = list(map(coder.encode, values))

  class DecodeAndEmitDoFn(DoFn):
    """A DoFn which stores encoded versions of elements.

    It also stores a Coder to decode and emit those elements.
    TODO: BEAM-2422 - Make this a SplittableDoFn.
    """

    def __init__(self, encoded_values, coder):
      self.encoded_values = encoded_values
      self.coder = coder

    def process(self, unused_element):
      for encoded_value in self.encoded_values:
        yield self.coder.decode(encoded_value)

  class Impulse(PTransform):
    """The Dataflow specific override for the impulse primitive."""

    def expand(self, pbegin):
      assert isinstance(pbegin, pvalue.PBegin), (
          'Input to Impulse transform must be a PBegin but found %s' % pbegin)
      return pvalue.PCollection(pbegin.pipeline)

    def get_windowing(self, inputs):
      return Windowing(GlobalWindows())

    def infer_output_type(self, unused_input_type):
      return bytes

  def expand(self, pbegin):
    return (pbegin
            | 'Impulse' >> self.Impulse()
            | 'Decode Values' >> ParDo(
                self.DecodeAndEmitDoFn(self.encoded_values, self.coder)))
