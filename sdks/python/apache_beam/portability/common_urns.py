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

""" Accessors for URNs of common Beam entities. """

from __future__ import absolute_import

from builtins import object

from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability.api import metrics_pb2
from apache_beam.portability.api import standard_window_fns_pb2


class PropertiesFromEnumValue(object):
  def __init__(self, value_descriptor):
    self.urn = (
        value_descriptor.GetOptions().Extensions[beam_runner_api_pb2.beam_urn])
    self.constant = (
        value_descriptor.GetOptions().Extensions[
            beam_runner_api_pb2.beam_constant])


class PropertiesFromEnumType(object):
  def __init__(self, enum_type):
    for v in enum_type.DESCRIPTOR.values:
      setattr(self, v.name, PropertiesFromEnumValue(v))


primitives = PropertiesFromEnumType(
    beam_runner_api_pb2.StandardPTransforms.Primitives)
deprecated_primitives = PropertiesFromEnumType(
    beam_runner_api_pb2.StandardPTransforms.DeprecatedPrimitives)
composites = PropertiesFromEnumType(
    beam_runner_api_pb2.StandardPTransforms.Composites)
combine_components = PropertiesFromEnumType(
    beam_runner_api_pb2.StandardPTransforms.CombineComponents)
sdf_components = PropertiesFromEnumType(
    beam_runner_api_pb2.StandardPTransforms.SplittableParDoComponents)

side_inputs = PropertiesFromEnumType(
    beam_runner_api_pb2.StandardSideInputTypes.Enum)

coders = PropertiesFromEnumType(beam_runner_api_pb2.StandardCoders.Enum)

constants = PropertiesFromEnumType(
    beam_runner_api_pb2.BeamConstants.Constants)

environments = PropertiesFromEnumType(
    beam_runner_api_pb2.StandardEnvironments.Environments)


def PropertiesFromPayloadType(payload_type):
  return PropertiesFromEnumType(payload_type.Enum).PROPERTIES


global_windows = PropertiesFromPayloadType(
    standard_window_fns_pb2.GlobalWindowsPayload)
fixed_windows = PropertiesFromPayloadType(
    standard_window_fns_pb2.FixedWindowsPayload)
sliding_windows = PropertiesFromPayloadType(
    standard_window_fns_pb2.SlidingWindowsPayload)
session_windows = PropertiesFromPayloadType(
    standard_window_fns_pb2.SessionsPayload)

monitoring_infos = PropertiesFromEnumType(
    metrics_pb2.MonitoringInfoUrns.Enum)
monitoring_info_types = PropertiesFromEnumType(
    metrics_pb2.MonitoringInfoTypeUrns.Enum)
