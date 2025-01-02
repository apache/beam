// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package urns handles extracting urns from all the protos.
package urns

import (
	pipepb "github.com/apache/beam/sdks/v2/go/pkg/beam/model/pipeline_v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type protoEnum interface {
	~int32
	Descriptor() protoreflect.EnumDescriptor
}

// toUrn returns a function that can get the urn string from the proto.
func toUrn[Enum protoEnum]() func(Enum) string {
	evd := (Enum)(0).Descriptor().Values()
	return func(v Enum) string {
		return proto.GetExtension(evd.ByNumber(protoreflect.EnumNumber(v)).Options(), pipepb.E_BeamUrn).(string)
	}
}

// quickUrn handles one off urns instead of retaining a helper function.
// Notably useful for the windowFns due to their older design.
func quickUrn[Enum protoEnum](v Enum) string {
	return toUrn[Enum]()(v)
}

var (
	ptUrn      = toUrn[pipepb.StandardPTransforms_Primitives]()
	ctUrn      = toUrn[pipepb.StandardPTransforms_Composites]()
	cmbtUrn    = toUrn[pipepb.StandardPTransforms_CombineComponents]()
	sdfUrn     = toUrn[pipepb.StandardPTransforms_SplittableParDoComponents]()
	siUrn      = toUrn[pipepb.StandardSideInputTypes_Enum]()
	cdrUrn     = toUrn[pipepb.StandardCoders_Enum]()
	reqUrn     = toUrn[pipepb.StandardRequirements_Enum]()
	runProcUrn = toUrn[pipepb.StandardRunnerProtocols_Enum]()
	envUrn     = toUrn[pipepb.StandardEnvironments_Environments]()
	usUrn      = toUrn[pipepb.StandardUserStateTypes_Enum]()
)

var (
	// SDK transforms.
	TransformParDo                = ptUrn(pipepb.StandardPTransforms_PAR_DO)
	TransformCombinePerKey        = ctUrn(pipepb.StandardPTransforms_COMBINE_PER_KEY)
	TransformCombineGlobally      = ctUrn(pipepb.StandardPTransforms_COMBINE_GLOBALLY)
	TransformReshuffle            = ctUrn(pipepb.StandardPTransforms_RESHUFFLE)
	TransformCombineGroupedValues = cmbtUrn(pipepb.StandardPTransforms_COMBINE_GROUPED_VALUES)
	TransformPreCombine           = cmbtUrn(pipepb.StandardPTransforms_COMBINE_PER_KEY_PRECOMBINE)
	TransformMerge                = cmbtUrn(pipepb.StandardPTransforms_COMBINE_PER_KEY_MERGE_ACCUMULATORS)
	TransformExtract              = cmbtUrn(pipepb.StandardPTransforms_COMBINE_PER_KEY_EXTRACT_OUTPUTS)
	TransformPairWithRestriction  = sdfUrn(pipepb.StandardPTransforms_PAIR_WITH_RESTRICTION)
	TransformSplitAndSize         = sdfUrn(pipepb.StandardPTransforms_SPLIT_AND_SIZE_RESTRICTIONS)
	TransformProcessSizedElements = sdfUrn(pipepb.StandardPTransforms_PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS)
	TransformTruncate             = sdfUrn(pipepb.StandardPTransforms_TRUNCATE_SIZED_RESTRICTION)

	// Window Manipulation
	TransformAssignWindows = ptUrn(pipepb.StandardPTransforms_ASSIGN_WINDOWS)
	TransformMapWindows    = ptUrn(pipepb.StandardPTransforms_MAP_WINDOWS)
	TransformMergeWindows  = ptUrn(pipepb.StandardPTransforms_MERGE_WINDOWS)

	// Testing
	TransformTestStream = ptUrn(pipepb.StandardPTransforms_TEST_STREAM)

	// Debugging
	TransformToString = ptUrn(pipepb.StandardPTransforms_TO_STRING)

	// Undocumented Urns
	GoDoFn          = "beam:go:transform:dofn:v1" // Only used for Go DoFn.
	TransformSource = "beam:runner:source:v1"     // The data source reading transform.
	TransformSink   = "beam:runner:sink:v1"       // The data sink writing transform.

	// Runner transforms.
	TransformImpulse = ptUrn(pipepb.StandardPTransforms_IMPULSE)
	TransformGBK     = ptUrn(pipepb.StandardPTransforms_GROUP_BY_KEY)
	TransformFlatten = ptUrn(pipepb.StandardPTransforms_FLATTEN)

	// Side Input access patterns
	SideInputIterable = siUrn(pipepb.StandardSideInputTypes_ITERABLE)
	SideInputMultiMap = siUrn(pipepb.StandardSideInputTypes_MULTIMAP)

	// UserState kinds
	UserStateBag         = usUrn(pipepb.StandardUserStateTypes_BAG)
	UserStateMultiMap    = usUrn(pipepb.StandardUserStateTypes_MULTIMAP)
	UserStateOrderedList = usUrn(pipepb.StandardUserStateTypes_ORDERED_LIST)

	// WindowsFns
	WindowFnGlobal  = quickUrn(pipepb.GlobalWindowsPayload_PROPERTIES)
	WindowFnFixed   = quickUrn(pipepb.FixedWindowsPayload_PROPERTIES)
	WindowFnSliding = quickUrn(pipepb.SlidingWindowsPayload_PROPERTIES)
	WindowFnSession = quickUrn(pipepb.SessionWindowsPayload_PROPERTIES)

	// Coders
	CoderBytes      = cdrUrn(pipepb.StandardCoders_BYTES)
	CoderBool       = cdrUrn(pipepb.StandardCoders_BOOL)
	CoderDouble     = cdrUrn(pipepb.StandardCoders_DOUBLE)
	CoderStringUTF8 = cdrUrn(pipepb.StandardCoders_STRING_UTF8)
	CoderRow        = cdrUrn(pipepb.StandardCoders_ROW)
	CoderVarInt     = cdrUrn(pipepb.StandardCoders_VARINT)

	CoderGlobalWindow   = cdrUrn(pipepb.StandardCoders_GLOBAL_WINDOW)
	CoderIntervalWindow = cdrUrn(pipepb.StandardCoders_INTERVAL_WINDOW)
	CoderCustomWindow   = cdrUrn(pipepb.StandardCoders_CUSTOM_WINDOW)

	CoderParamWindowedValue = cdrUrn(pipepb.StandardCoders_PARAM_WINDOWED_VALUE)
	CoderWindowedValue      = cdrUrn(pipepb.StandardCoders_WINDOWED_VALUE)
	CoderTimer              = cdrUrn(pipepb.StandardCoders_TIMER)

	CoderKV                  = cdrUrn(pipepb.StandardCoders_KV)
	CoderLengthPrefix        = cdrUrn(pipepb.StandardCoders_LENGTH_PREFIX)
	CoderNullable            = cdrUrn(pipepb.StandardCoders_NULLABLE)
	CoderIterable            = cdrUrn(pipepb.StandardCoders_ITERABLE)
	CoderStateBackedIterable = cdrUrn(pipepb.StandardCoders_STATE_BACKED_ITERABLE)
	CoderShardedKey          = cdrUrn(pipepb.StandardCoders_SHARDED_KEY)

	// Requirements
	RequirementSplittableDoFn     = reqUrn(pipepb.StandardRequirements_REQUIRES_SPLITTABLE_DOFN)
	RequirementBundleFinalization = reqUrn(pipepb.StandardRequirements_REQUIRES_BUNDLE_FINALIZATION)
	RequirementOnWindowExpiration = reqUrn(pipepb.StandardRequirements_REQUIRES_ON_WINDOW_EXPIRATION)
	RequirementStableInput        = reqUrn(pipepb.StandardRequirements_REQUIRES_STABLE_INPUT)
	RequirementStatefulProcessing = reqUrn(pipepb.StandardRequirements_REQUIRES_STATEFUL_PROCESSING)
	RequirementTimeSortedInput    = reqUrn(pipepb.StandardRequirements_REQUIRES_TIME_SORTED_INPUT)

	// Capabilities
	CapabilityMonitoringInfoShortIDs           = runProcUrn(pipepb.StandardRunnerProtocols_MONITORING_INFO_SHORT_IDS)
	CapabilityControlResponseElementsEmbedding = runProcUrn(pipepb.StandardRunnerProtocols_CONTROL_RESPONSE_ELEMENTS_EMBEDDING)

	// Environment types
	EnvDocker   = envUrn(pipepb.StandardEnvironments_DOCKER)
	EnvProcess  = envUrn(pipepb.StandardEnvironments_PROCESS)
	EnvExternal = envUrn(pipepb.StandardEnvironments_EXTERNAL)
	EnvDefault  = envUrn(pipepb.StandardEnvironments_DEFAULT)
)
