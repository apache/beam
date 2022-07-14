from ........utils import PropertiesFromEnumValue
from . import metrics_pb2
EMPTY_MONITORING_INFO_LABEL_PROPS = metrics_pb2.MonitoringInfoLabelProps()
EMPTY_MONITORING_INFO_SPEC = metrics_pb2.MonitoringInfoSpec()

class BeamConstants(object):

  class Constants(object):
    MIN_TIMESTAMP_MILLIS = PropertiesFromEnumValue('', '-9223372036854775', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    MAX_TIMESTAMP_MILLIS = PropertiesFromEnumValue('', '9223372036854775', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    GLOBAL_WINDOW_MAX_TIMESTAMP_MILLIS = PropertiesFromEnumValue('', '9223371950454775', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


class StandardArtifacts(object):

  class Roles(object):
    STAGING_TO = PropertiesFromEnumValue('beam:artifact:role:staging_to:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    PIP_REQUIREMENTS_FILE = PropertiesFromEnumValue('beam:artifact:role:pip_requirements_file:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    GO_WORKER_BINARY = PropertiesFromEnumValue('beam:artifact:role:go_worker_binary:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


  class Types(object):
    FILE = PropertiesFromEnumValue('beam:artifact:type:file:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    URL = PropertiesFromEnumValue('beam:artifact:type:url:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    EMBEDDED = PropertiesFromEnumValue('beam:artifact:type:embedded:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    PYPI = PropertiesFromEnumValue('beam:artifact:type:pypi:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    MAVEN = PropertiesFromEnumValue('beam:artifact:type:maven:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    DEFERRED = PropertiesFromEnumValue('beam:artifact:type:deferred:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


class StandardCoders(object):

  class Enum(object):
    BYTES = PropertiesFromEnumValue('beam:coder:bytes:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    STRING_UTF8 = PropertiesFromEnumValue('beam:coder:string_utf8:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    KV = PropertiesFromEnumValue('beam:coder:kv:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    BOOL = PropertiesFromEnumValue('beam:coder:bool:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    VARINT = PropertiesFromEnumValue('beam:coder:varint:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    DOUBLE = PropertiesFromEnumValue('beam:coder:double:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    ITERABLE = PropertiesFromEnumValue('beam:coder:iterable:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    TIMER = PropertiesFromEnumValue('beam:coder:timer:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    INTERVAL_WINDOW = PropertiesFromEnumValue('beam:coder:interval_window:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    LENGTH_PREFIX = PropertiesFromEnumValue('beam:coder:length_prefix:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    GLOBAL_WINDOW = PropertiesFromEnumValue('beam:coder:global_window:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    WINDOWED_VALUE = PropertiesFromEnumValue('beam:coder:windowed_value:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    PARAM_WINDOWED_VALUE = PropertiesFromEnumValue('beam:coder:param_windowed_value:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    STATE_BACKED_ITERABLE = PropertiesFromEnumValue('beam:coder:state_backed_iterable:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    CUSTOM_WINDOW = PropertiesFromEnumValue('beam:coder:custom_window:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    ROW = PropertiesFromEnumValue('beam:coder:row:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    SHARDED_KEY = PropertiesFromEnumValue('beam:coder:sharded_key:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    NULLABLE = PropertiesFromEnumValue('beam:coder:nullable:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


class StandardDisplayData(object):

  class DisplayData(object):
    LABELLED = PropertiesFromEnumValue('beam:display_data:labelled:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


class StandardEnvironments(object):

  class Environments(object):
    DOCKER = PropertiesFromEnumValue('beam:env:docker:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    PROCESS = PropertiesFromEnumValue('beam:env:process:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    EXTERNAL = PropertiesFromEnumValue('beam:env:external:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    DEFAULT = PropertiesFromEnumValue('beam:env:default:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


class StandardPTransforms(object):

  class CombineComponents(object):
    COMBINE_PER_KEY_PRECOMBINE = PropertiesFromEnumValue('beam:transform:combine_per_key_precombine:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    COMBINE_PER_KEY_MERGE_ACCUMULATORS = PropertiesFromEnumValue('beam:transform:combine_per_key_merge_accumulators:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    COMBINE_PER_KEY_EXTRACT_OUTPUTS = PropertiesFromEnumValue('beam:transform:combine_per_key_extract_outputs:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    COMBINE_GROUPED_VALUES = PropertiesFromEnumValue('beam:transform:combine_grouped_values:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    COMBINE_PER_KEY_CONVERT_TO_ACCUMULATORS = PropertiesFromEnumValue('beam:transform:combine_per_key_convert_to_accumulators:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


  class Composites(object):
    COMBINE_PER_KEY = PropertiesFromEnumValue('beam:transform:combine_per_key:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    COMBINE_GLOBALLY = PropertiesFromEnumValue('beam:transform:combine_globally:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    RESHUFFLE = PropertiesFromEnumValue('beam:transform:reshuffle:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    WRITE_FILES = PropertiesFromEnumValue('beam:transform:write_files:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    PUBSUB_READ = PropertiesFromEnumValue('beam:transform:pubsub_read:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    PUBSUB_WRITE = PropertiesFromEnumValue('beam:transform:pubsub_write:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    PUBSUB_WRITE_V2 = PropertiesFromEnumValue('beam:transform:pubsub_write:v2', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    GROUP_INTO_BATCHES_WITH_SHARDED_KEY = PropertiesFromEnumValue('beam:transform:group_into_batches_with_sharded_key:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


  class DeprecatedPrimitives(object):
    READ = PropertiesFromEnumValue('beam:transform:read:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    CREATE_VIEW = PropertiesFromEnumValue('beam:transform:create_view:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


  class GroupIntoBatchesComponents(object):
    GROUP_INTO_BATCHES = PropertiesFromEnumValue('beam:transform:group_into_batches:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


  class Primitives(object):
    PAR_DO = PropertiesFromEnumValue('beam:transform:pardo:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    FLATTEN = PropertiesFromEnumValue('beam:transform:flatten:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    GROUP_BY_KEY = PropertiesFromEnumValue('beam:transform:group_by_key:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    IMPULSE = PropertiesFromEnumValue('beam:transform:impulse:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    ASSIGN_WINDOWS = PropertiesFromEnumValue('beam:transform:window_into:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    TEST_STREAM = PropertiesFromEnumValue('beam:transform:teststream:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    MAP_WINDOWS = PropertiesFromEnumValue('beam:transform:map_windows:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    MERGE_WINDOWS = PropertiesFromEnumValue('beam:transform:merge_windows:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    TO_STRING = PropertiesFromEnumValue('beam:transform:to_string:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


  class SplittableParDoComponents(object):
    PAIR_WITH_RESTRICTION = PropertiesFromEnumValue('beam:transform:sdf_pair_with_restriction:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    SPLIT_AND_SIZE_RESTRICTIONS = PropertiesFromEnumValue('beam:transform:sdf_split_and_size_restrictions:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    PROCESS_SIZED_ELEMENTS_AND_RESTRICTIONS = PropertiesFromEnumValue('beam:transform:sdf_process_sized_element_and_restrictions:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    TRUNCATE_SIZED_RESTRICTION = PropertiesFromEnumValue('beam:transform:sdf_truncate_sized_restrictions:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


class StandardProtocols(object):

  class Enum(object):
    LEGACY_PROGRESS_REPORTING = PropertiesFromEnumValue('beam:protocol:progress_reporting:v0', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    PROGRESS_REPORTING = PropertiesFromEnumValue('beam:protocol:progress_reporting:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    WORKER_STATUS = PropertiesFromEnumValue('beam:protocol:worker_status:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    MULTI_CORE_BUNDLE_PROCESSING = PropertiesFromEnumValue('beam:protocol:multi_core_bundle_processing:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    SIBLING_WORKERS = PropertiesFromEnumValue('beam:protocol:sibling_workers:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    HARNESS_MONITORING_INFOS = PropertiesFromEnumValue('beam:protocol:harness_monitoring_infos:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    CONTROL_REQUEST_ELEMENTS_EMBEDDING = PropertiesFromEnumValue('beam:protocol:control_request_elements_embedding:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    STATE_CACHING = PropertiesFromEnumValue('beam:protocol:state_caching:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


class StandardRequirements(object):

  class Enum(object):
    REQUIRES_STATEFUL_PROCESSING = PropertiesFromEnumValue('beam:requirement:pardo:stateful:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    REQUIRES_BUNDLE_FINALIZATION = PropertiesFromEnumValue('beam:requirement:pardo:finalization:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    REQUIRES_STABLE_INPUT = PropertiesFromEnumValue('beam:requirement:pardo:stable_input:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    REQUIRES_TIME_SORTED_INPUT = PropertiesFromEnumValue('beam:requirement:pardo:time_sorted_input:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    REQUIRES_SPLITTABLE_DOFN = PropertiesFromEnumValue('beam:requirement:pardo:splittable_dofn:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    REQUIRES_ON_WINDOW_EXPIRATION = PropertiesFromEnumValue('beam:requirement:pardo:on_window_expiration:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


class StandardResourceHints(object):

  class Enum(object):
    ACCELERATOR = PropertiesFromEnumValue('beam:resources:accelerator:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    MIN_RAM_BYTES = PropertiesFromEnumValue('beam:resources:min_ram_bytes:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


class StandardRunnerProtocols(object):

  class Enum(object):
    MONITORING_INFO_SHORT_IDS = PropertiesFromEnumValue('beam:protocol:monitoring_info_short_ids:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    CONTROL_RESPONSE_ELEMENTS_EMBEDDING = PropertiesFromEnumValue('beam:protocol:control_response_elements_embedding:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


class StandardSideInputTypes(object):

  class Enum(object):
    ITERABLE = PropertiesFromEnumValue('beam:side_input:iterable:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    MULTIMAP = PropertiesFromEnumValue('beam:side_input:multimap:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)


class StandardUserStateTypes(object):

  class Enum(object):
    BAG = PropertiesFromEnumValue('beam:user_state:bag:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)
    MULTIMAP = PropertiesFromEnumValue('beam:user_state:multimap:v1', '', EMPTY_MONITORING_INFO_SPEC, EMPTY_MONITORING_INFO_LABEL_PROPS)

