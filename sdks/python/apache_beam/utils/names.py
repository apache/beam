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


# Standard file names used for staging files.
PICKLED_MAIN_SESSION_FILE = 'pickled_main_session'
DATAFLOW_SDK_TARBALL_FILE = 'dataflow_python_sdk.tar'

# String constants related to sources framework
SOURCE_FORMAT = 'custom_source'
SOURCE_TYPE = 'CustomSourcesType'
SERIALIZED_SOURCE_KEY = 'serialized_source'


class TransformNames(object):
  """Transform strings as they are expected in the CloudWorkflow protos."""
  COLLECTION_TO_SINGLETON = 'CollectionToSingleton'
  COMBINE = 'CombineValues'
  CREATE_PCOLLECTION = 'CreateCollection'
  DO = 'ParallelDo'
  FLATTEN = 'Flatten'
  GROUP = 'GroupByKey'
  READ = 'ParallelRead'
  WRITE = 'ParallelWrite'


class PropertyNames(object):
  """Property strings as they are expected in the CloudWorkflow protos."""
  BIGQUERY_CREATE_DISPOSITION = 'create_disposition'
  BIGQUERY_DATASET = 'dataset'
  BIGQUERY_QUERY = 'bigquery_query'
  BIGQUERY_TABLE = 'table'
  BIGQUERY_PROJECT = 'project'
  BIGQUERY_SCHEMA = 'schema'
  BIGQUERY_WRITE_DISPOSITION = 'write_disposition'
  ELEMENT = 'element'
  ELEMENTS = 'elements'
  ENCODING = 'encoding'
  FILE_PATTERN = 'filepattern'
  FILE_NAME_PREFIX = 'filename_prefix'
  FILE_NAME_SUFFIX = 'filename_suffix'
  FORMAT = 'format'
  INPUTS = 'inputs'
  NON_PARALLEL_INPUTS = 'non_parallel_inputs'
  NUM_SHARDS = 'num_shards'
  OUT = 'out'
  OUTPUT = 'output'
  OUTPUT_INFO = 'output_info'
  OUTPUT_NAME = 'output_name'
  PARALLEL_INPUT = 'parallel_input'
  PUBSUB_TOPIC = 'pubsub_topic'
  PUBSUB_SUBSCRIPTION = 'pubsub_subscription'
  PUBSUB_ID_LABEL = 'pubsub_id_label'
  SERIALIZED_FN = 'serialized_fn'
  SHARD_NAME_TEMPLATE = 'shard_template'
  SOURCE_STEP_INPUT = 'custom_source_step_input'
  STEP_NAME = 'step_name'
  USER_FN = 'user_fn'
  USER_NAME = 'user_name'
  VALIDATE_SINK = 'validate_sink'
  VALIDATE_SOURCE = 'validate_source'
  VALUE = 'value'
