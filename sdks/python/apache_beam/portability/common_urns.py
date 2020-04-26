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

# pytype: skip-file

from __future__ import absolute_import

from apache_beam.portability.api.beam_runner_api_pb2_urns import BeamConstants
from apache_beam.portability.api.beam_runner_api_pb2_urns import StandardArtifacts
from apache_beam.portability.api.beam_runner_api_pb2_urns import StandardCoders
from apache_beam.portability.api.beam_runner_api_pb2_urns import StandardEnvironments
from apache_beam.portability.api.beam_runner_api_pb2_urns import StandardProtocols
from apache_beam.portability.api.beam_runner_api_pb2_urns import StandardPTransforms
from apache_beam.portability.api.beam_runner_api_pb2_urns import StandardRequirements
from apache_beam.portability.api.beam_runner_api_pb2_urns import StandardSideInputTypes
from apache_beam.portability.api.metrics_pb2_urns import MonitoringInfo
from apache_beam.portability.api.metrics_pb2_urns import MonitoringInfoSpecs
from apache_beam.portability.api.metrics_pb2_urns import MonitoringInfoTypeUrns
from apache_beam.portability.api.standard_window_fns_pb2_urns import FixedWindowsPayload
from apache_beam.portability.api.standard_window_fns_pb2_urns import GlobalWindowsPayload
from apache_beam.portability.api.standard_window_fns_pb2_urns import SessionWindowsPayload
from apache_beam.portability.api.standard_window_fns_pb2_urns import SlidingWindowsPayload

primitives = StandardPTransforms.Primitives
deprecated_primitives = StandardPTransforms.DeprecatedPrimitives
composites = StandardPTransforms.Composites
combine_components = StandardPTransforms.CombineComponents
sdf_components = StandardPTransforms.SplittableParDoComponents

side_inputs = StandardSideInputTypes.Enum
coders = StandardCoders.Enum
constants = BeamConstants.Constants

environments = StandardEnvironments.Environments
artifact_types = StandardArtifacts.Types
artifact_roles = StandardArtifacts.Roles

global_windows = GlobalWindowsPayload.Enum.PROPERTIES
fixed_windows = FixedWindowsPayload.Enum.PROPERTIES
sliding_windows = SlidingWindowsPayload.Enum.PROPERTIES
session_windows = SessionWindowsPayload.Enum.PROPERTIES

monitoring_info_specs = MonitoringInfoSpecs.Enum
monitoring_info_types = MonitoringInfoTypeUrns.Enum
monitoring_info_labels = MonitoringInfo.MonitoringInfoLabels

protocols = StandardProtocols.Enum
requirements = StandardRequirements.Enum
