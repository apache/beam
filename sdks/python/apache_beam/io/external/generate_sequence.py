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
from apache_beam.portability.api import external_transforms_pb2
from apache_beam.portability.common_urns import generate_sequence


class GenerateSequence(ExternalTransform):

  def __init__(self, start, stop=None,
               elements_per_period=None, max_read_time=None,
               expansion_service=None):
    payload = external_transforms_pb2.GenerateSequencePayload(start=start)
    if stop:
      payload.stop_provided = True
      payload.stop_value = stop
    if elements_per_period:
      payload.elements_per_period_provided = True
      payload.elements_per_period_value = elements_per_period
    if max_read_time:
      payload.max_read_time_provided = True
      payload.max_read_time_value = max_read_time

    super(GenerateSequence, self).__init__(
        generate_sequence.urn, payload.SerializeToString(), expansion_service)
