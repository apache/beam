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
A PTransform that provides a bounded or unbounded stream of integers.
"""
from __future__ import absolute_import

from apache_beam import ExternalTransform
from apache_beam.coders import VarIntCoder
from apache_beam.portability.api.external_transforms_pb2 import ConfigValue
from apache_beam.portability.api.external_transforms_pb2 import ExternalConfigurationPayload


class GenerateSequence(ExternalTransform):

  def __init__(self, start, stop=None,
               elements_per_period=None, max_read_time=None,
               expansion_service=None):
    coder = VarIntCoder()
    coder_urn = 'beam:coder:varint:v1'
    args = {
        'start':
            ConfigValue(
                coder_urn=coder_urn,
                payload=coder.encode(start))
    }
    if stop:
      args['stop'] = ConfigValue(
          coder_urn=coder_urn,
          payload=coder.encode(stop))
    if elements_per_period:
      args['elements_per_period'] = ConfigValue(
          coder_urn=coder_urn,
          payload=coder.encode(elements_per_period))
    if max_read_time:
      args['max_read_time'] = ConfigValue(
          coder_urn=coder_urn,
          payload=coder.encode(max_read_time))

    payload = ExternalConfigurationPayload(configuration=args)
    super(GenerateSequence, self).__init__(
        'beam:external:java:generate_sequence:v1',
        payload.SerializeToString(),
        expansion_service)
