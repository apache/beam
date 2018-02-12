from apache_beam.portability.api import beam_runner_api_pb2
from apache_beam.portability.api import standard_window_fns_pb2

class PropertiesFromEnumValue(object):
  def __init__(self, value_descriptor):
    self.urn = (
        value_descriptor.GetOptions().Extensions[beam_runner_api_pb2.beam_urn])

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

PARDO_TRANSFORM = primitives.PAR_DO.urn
FLATTEN_TRANSFORM = primitives.FLATTEN.urn
GROUP_BY_KEY_TRANSFORM = primitives.GROUP_BY_KEY.urn
IMPULSE_TRANSFORM = primitives.IMPULSE.urn
ASSIGN_WINDOWS_TRANSFORM = primitives.ASSIGN_WINDOWS.urn
TEST_STREAM_TRANSFORM = primitives.TEST_STREAM.urn
MAP_WINDOWS_TRANSFORM = primitives.TEST_STREAM.urn

READ_TRANSFORM = deprecated_primitives.READ.urn
CREATE_VIEW_TRANSFORM = deprecated_primitives.CREATE_VIEW.urn

COMBINE_PER_KEY_TRANSFORM = composites.COMBINE_PER_KEY.urn
COMBINE_GLOBALLY_TRANSFORM = composites.COMBINE_GLOBALLY.urn
COMBINE_GROUPED_VALUES_TRANSFORM = composites.COMBINE_GROUPED_VALUES.urn
RESHUFFLE_TRANSFORM = composites.RESHUFFLE.urn
WRITE_FILES_TRANSFORM = composites.WRITE_FILES.urn

COMBINE_PGBKCV_TRANSFORM = combine_components.COMBINE_PGBKCV.urn
COMBINE_MERGE_ACCUMULATORS_TRANSFORM = (
    combine_components.COMBINE_MERGE_ACCUMULATORS.urn)
COMBINE_EXTRACT_OUTPUTS_TRANSFORM = (
    combine_components.COMBINE_EXTRACT_OUTPUTS.urn)

side_inputs = PropertiesFromEnumType(
    beam_runner_api_pb2.StandardSideInputTypes.Enum)

ITERABLE_SIDE_INPUT = side_inputs.ITERABLE.urn
MULTIMAP_SIDE_INPUT = side_inputs.MULTIMAP.urn

coders = PropertiesFromEnumType(beam_runner_api_pb2.StandardCoders.Enum)

BYTES_CODER = coders.BYTES.urn
KV_CODER = coders.KV.urn
VARINT_CODER = coders.VARINT.urn
ITERABLE_CODER = coders.ITERABLE.urn
INTERVAL_WINDOW_CODER = coders.INTERVAL_WINDOW.urn
LENGTH_PREFIX_CODER = coders.LENGTH_PREFIX.urn
GLOBAL_WINDOW_CODER = coders.GLOBAL_WINDOW.urn
WINDOWED_VALUE_CODER = coders.WINDOWED_VALUE.urn

def PropertiesFromPayloadType(payload_type):
  return PropertiesFromEnumType(payload_type.Enum).PROPERTIES

GLOBAL_WINDOWS_WINDOWFN = PropertiesFromPayloadType(
    standard_window_fns_pb2.GlobalWindowsPayload).urn
FIXED_WINDOWS_WINDOWFN = PropertiesFromPayloadType(
    standard_window_fns_pb2.FixedWindowsPayload).urn
SLIDING_WINDOWS_WINDOWFN = PropertiesFromPayloadType(
    standard_window_fns_pb2.SlidingWindowsPayload).urn
SESSION_WINDOWS_WINDOWFN = PropertiesFromPayloadType(
    standard_window_fns_pb2.SessionsPayload).urn
