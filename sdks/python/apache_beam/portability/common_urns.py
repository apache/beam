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

from apache_beam.portability.api import beam_runner_api_pb2_urns
from apache_beam.portability.api import metrics_pb2_urns
from apache_beam.portability.api import standard_window_fns_pb2_urns

primitives = beam_runner_api_pb2_urns.StandardPTransforms.Primitives
deprecated_primitives = beam_runner_api_pb2_urns.StandardPTransforms.DeprecatedPrimitives
composites = beam_runner_api_pb2_urns.StandardPTransforms.Composites
combine_components = beam_runner_api_pb2_urns.StandardPTransforms.CombineComponents
sdf_components = beam_runner_api_pb2_urns.StandardPTransforms.SplittableParDoComponents

side_inputs = beam_runner_api_pb2_urns.StandardSideInputTypes.Enum
coders = beam_runner_api_pb2_urns.StandardCoders.Enum
constants = beam_runner_api_pb2_urns.BeamConstants.Constants

environments = beam_runner_api_pb2_urns.StandardEnvironments.Environments

global_windows = standard_window_fns_pb2_urns.GlobalWindowsPayload.Enum.PROPERTIES
fixed_windows = standard_window_fns_pb2_urns.FixedWindowsPayload.Enum.PROPERTIES
sliding_windows = standard_window_fns_pb2_urns.SlidingWindowsPayload.Enum.PROPERTIES
session_windows = standard_window_fns_pb2_urns.SessionsPayload.Enum.PROPERTIES

monitoring_info_specs = metrics_pb2_urns.MonitoringInfoSpecs.Enum
monitoring_info_types = metrics_pb2_urns.MonitoringInfoTypeUrns.Enum
monitoring_info_labels = metrics_pb2_urns.MonitoringInfo.MonitoringInfoLabels
