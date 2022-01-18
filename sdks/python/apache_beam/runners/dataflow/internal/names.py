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

"""Various names for properties, transforms, etc."""

# All constants are for internal use only; no backwards-compatibility
# guarantees.

# pytype: skip-file

# Referenced by Dataflow legacy worker.
from apache_beam.runners.internal.names import PICKLED_MAIN_SESSION_FILE  # pylint: disable=unused-import

# String constants related to sources framework
SOURCE_FORMAT = 'custom_source'
SOURCE_TYPE = 'CustomSourcesType'
SERIALIZED_SOURCE_KEY = 'serialized_source'

# In a released SDK, container tags are selected based on the SDK version.
# Unreleased versions use container versions based on values of
# BEAM_CONTAINER_VERSION and BEAM_FNAPI_CONTAINER_VERSION (see below).

# Update this version to the next version whenever there is a change that will
# require changes to legacy Dataflow worker execution environment.
BEAM_CONTAINER_VERSION = 'beam-master-20220113'
# Update this version to the next version whenever there is a change that
# requires changes to SDK harness container or SDK harness launcher.
BEAM_FNAPI_CONTAINER_VERSION = 'beam-master-20220117'

DATAFLOW_CONTAINER_IMAGE_REPOSITORY = 'gcr.io/cloud-dataflow/v1beta3'


class TransformNames(object):
  """For internal use only; no backwards-compatibility guarantees.

  Transform strings as they are expected in the CloudWorkflow protos.
  """
  COLLECTION_TO_SINGLETON = 'CollectionToSingleton'
  COMBINE = 'CombineValues'
  CREATE_PCOLLECTION = 'CreateCollection'
  DO = 'ParallelDo'
  FLATTEN = 'Flatten'
  GROUP = 'GroupByKey'
  READ = 'ParallelRead'
  WRITE = 'ParallelWrite'


class PropertyNames(object):
  """For internal use only; no backwards-compatibility guarantees.

  Property strings as they are expected in the CloudWorkflow protos.
  """
  # If uses_keyed_state, whether the state can be sharded.
  ALLOWS_SHARDABLE_STATE = 'allows_shardable_state'
  BIGQUERY_CREATE_DISPOSITION = 'create_disposition'
  BIGQUERY_DATASET = 'dataset'
  BIGQUERY_EXPORT_FORMAT = 'bigquery_export_format'
  BIGQUERY_FLATTEN_RESULTS = 'bigquery_flatten_results'
  BIGQUERY_KMS_KEY = 'bigquery_kms_key'
  BIGQUERY_PROJECT = 'project'
  BIGQUERY_QUERY = 'bigquery_query'
  BIGQUERY_SCHEMA = 'schema'
  BIGQUERY_TABLE = 'table'
  BIGQUERY_USE_LEGACY_SQL = 'bigquery_use_legacy_sql'
  BIGQUERY_WRITE_DISPOSITION = 'write_disposition'
  DISPLAY_DATA = 'display_data'
  ELEMENT = 'element'
  ELEMENTS = 'elements'
  ENCODING = 'encoding'
  FILE_PATTERN = 'filepattern'
  FILE_NAME_PREFIX = 'filename_prefix'
  FILE_NAME_SUFFIX = 'filename_suffix'
  FORMAT = 'format'
  INPUTS = 'inputs'
  IMPULSE_ELEMENT = 'impulse_element'
  NON_PARALLEL_INPUTS = 'non_parallel_inputs'
  NUM_SHARDS = 'num_shards'
  OUT = 'out'
  OUTPUT = 'output'
  OUTPUT_INFO = 'output_info'
  OUTPUT_NAME = 'output_name'
  PARALLEL_INPUT = 'parallel_input'
  PIPELINE_PROTO_TRANSFORM_ID = 'pipeline_proto_transform_id'
  # If the input element is a key/value pair, then the output element(s) all
  # have the same key as the input.
  PRESERVES_KEYS = 'preserves_keys'
  PUBSUB_ID_LABEL = 'pubsub_id_label'
  PUBSUB_SERIALIZED_ATTRIBUTES_FN = 'pubsub_serialized_attributes_fn'
  PUBSUB_SUBSCRIPTION = 'pubsub_subscription'
  PUBSUB_TIMESTAMP_ATTRIBUTE = 'pubsub_timestamp_label'
  PUBSUB_TOPIC = 'pubsub_topic'
  RESOURCE_HINTS = 'resource_hints'
  RESTRICTION_ENCODING = 'restriction_encoding'
  SERIALIZED_FN = 'serialized_fn'
  SHARD_NAME_TEMPLATE = 'shard_template'
  SOURCE_STEP_INPUT = 'custom_source_step_input'
  SERIALIZED_TEST_STREAM = 'serialized_test_stream'
  STEP_NAME = 'step_name'
  USE_INDEXED_FORMAT = 'use_indexed_format'
  USER_FN = 'user_fn'
  USER_NAME = 'user_name'
  USES_KEYED_STATE = 'uses_keyed_state'
  VALIDATE_SINK = 'validate_sink'
  VALIDATE_SOURCE = 'validate_source'
  VALUE = 'value'
  WINDOWING_STRATEGY = 'windowing_strategy'
