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

from .api import beam_runner_api_pb2_urns
from .api import external_transforms_pb2_urns
from .api import metrics_pb2_urns
from .api import schema_pb2_urns
from .api import standard_window_fns_pb2_urns

BeamConstants = beam_runner_api_pb2_urns.BeamConstants
StandardArtifacts = beam_runner_api_pb2_urns.StandardArtifacts
StandardCoders = beam_runner_api_pb2_urns.StandardCoders
StandardDisplayData = beam_runner_api_pb2_urns.StandardDisplayData
StandardEnvironments = beam_runner_api_pb2_urns.StandardEnvironments
StandardProtocols = beam_runner_api_pb2_urns.StandardProtocols
StandardPTransforms = beam_runner_api_pb2_urns.StandardPTransforms
StandardRequirements = beam_runner_api_pb2_urns.StandardRequirements
StandardResourceHints = beam_runner_api_pb2_urns.StandardResourceHints
StandardSideInputTypes = beam_runner_api_pb2_urns.StandardSideInputTypes
StandardUserStateTypes = beam_runner_api_pb2_urns.StandardUserStateTypes
ExpansionMethods = external_transforms_pb2_urns.ExpansionMethods
MonitoringInfo = metrics_pb2_urns.MonitoringInfo
MonitoringInfoSpecs = metrics_pb2_urns.MonitoringInfoSpecs
MonitoringInfoTypeUrns = metrics_pb2_urns.MonitoringInfoTypeUrns
LogicalTypes = schema_pb2_urns.LogicalTypes
FixedWindowsPayload = standard_window_fns_pb2_urns.FixedWindowsPayload
GlobalWindowsPayload = standard_window_fns_pb2_urns.GlobalWindowsPayload
SessionWindowsPayload = standard_window_fns_pb2_urns.SessionWindowsPayload
SlidingWindowsPayload = standard_window_fns_pb2_urns.SlidingWindowsPayload

primitives = StandardPTransforms.Primitives
deprecated_primitives = StandardPTransforms.DeprecatedPrimitives
composites = StandardPTransforms.Composites
combine_components = StandardPTransforms.CombineComponents
sdf_components = StandardPTransforms.SplittableParDoComponents
group_into_batches_components = StandardPTransforms.GroupIntoBatchesComponents

user_state = StandardUserStateTypes.Enum
side_inputs = StandardSideInputTypes.Enum
coders = StandardCoders.Enum
constants = BeamConstants.Constants

environments = StandardEnvironments.Environments
artifact_types = StandardArtifacts.Types
artifact_roles = StandardArtifacts.Roles
resource_hints = StandardResourceHints.Enum

global_windows = GlobalWindowsPayload.Enum.PROPERTIES
fixed_windows = FixedWindowsPayload.Enum.PROPERTIES
sliding_windows = SlidingWindowsPayload.Enum.PROPERTIES
session_windows = SessionWindowsPayload.Enum.PROPERTIES

monitoring_info_specs = MonitoringInfoSpecs.Enum
monitoring_info_types = MonitoringInfoTypeUrns.Enum
monitoring_info_labels = MonitoringInfo.MonitoringInfoLabels

protocols = StandardProtocols.Enum
requirements = StandardRequirements.Enum

displayData = StandardDisplayData.DisplayData

java_class_lookup = ExpansionMethods.Enum.JAVA_CLASS_LOOKUP

decimal = LogicalTypes.Enum.DECIMAL
micros_instant = LogicalTypes.Enum.MICROS_INSTANT
millis_instant = LogicalTypes.Enum.MILLIS_INSTANT
python_callable = LogicalTypes.Enum.PYTHON_CALLABLE
