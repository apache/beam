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

"""
A PTransform that provides an unbounded, streaming source of empty byte arrays.

This can only be used with the flink runner.
"""
# pytype: skip-file

from __future__ import absolute_import

import json
from typing import Any
from typing import Dict

from apache_beam import PTransform
from apache_beam import Windowing
from apache_beam import pvalue
from apache_beam.transforms.window import GlobalWindows


class FlinkStreamingImpulseSource(PTransform):
  URN = "flink:transform:streaming_impulse:v1"

  config: Dict[str, Any] = {}

  def expand(self, pbegin):
    assert isinstance(pbegin, pvalue.PBegin), (
        'Input to transform must be a PBegin but found %s' % pbegin)
    return pvalue.PCollection(pbegin.pipeline, is_bounded=False)

  def get_windowing(self, inputs):
    return Windowing(GlobalWindows())

  def infer_output_type(self, unused_input_type):
    return bytes

  def to_runner_api_parameter(self, context):
    assert isinstance(self, FlinkStreamingImpulseSource), \
      "expected instance of StreamingImpulseSource, but got %s" % self.__class__
    return (self.URN, json.dumps(self.config))

  def set_interval_ms(self, interval_ms):
    """Sets the interval (in milliseconds) between messages in the stream.
    """
    self.config["interval_ms"] = interval_ms
    return self

  def set_message_count(self, message_count):
    """If non-zero, the stream will produce only this many total messages.
    Otherwise produces an unbounded number of messages.
    """
    self.config["message_count"] = message_count
    return self

  @staticmethod
  @PTransform.register_urn(URN, None)
  def from_runner_api_parameter(_ptransform, spec_parameter, _context):
    if isinstance(spec_parameter, bytes):
      spec_parameter = spec_parameter.decode('utf-8')
    config = json.loads(spec_parameter)
    instance = FlinkStreamingImpulseSource()
    if "interval_ms" in config:
      instance.set_interval_ms(config["interval_ms"])
    if "message_count" in config:
      instance.set_message_count(config["message_count"])

    return instance
