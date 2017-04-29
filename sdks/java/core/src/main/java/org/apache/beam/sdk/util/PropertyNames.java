/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.util;

/**
 * Constant property names used by the SDK in CloudWorkflow specifications.
 */
public class PropertyNames {
  public static final String ALLOWED_ENCODINGS = "allowed_encodings";
  public static final String APPEND_TRAILING_NEWLINES = "append_trailing_newlines";
  public static final String BIGQUERY_CREATE_DISPOSITION = "create_disposition";
  public static final String BIGQUERY_DATASET = "dataset";
  public static final String BIGQUERY_PROJECT = "project";
  public static final String BIGQUERY_SCHEMA = "schema";
  public static final String BIGQUERY_TABLE = "table";
  public static final String BIGQUERY_QUERY = "bigquery_query";
  public static final String BIGQUERY_FLATTEN_RESULTS = "bigquery_flatten_results";
  public static final String BIGQUERY_USE_LEGACY_SQL = "bigquery_use_legacy_sql";
  public static final String BIGQUERY_WRITE_DISPOSITION = "write_disposition";
  public static final String BIGQUERY_EXPORT_FORMAT = "bigquery_export_format";
  public static final String BIGQUERY_EXPORT_SCHEMA = "bigquery_export_schema";
  public static final String CO_GBK_RESULT_SCHEMA = "co_gbk_result_schema";
  public static final String COMBINE_FN = "combine_fn";
  public static final String COMPONENT_ENCODINGS = "component_encodings";
  public static final String COMPRESSION_TYPE = "compression_type";
  public static final String CUSTOM_SOURCE_FORMAT = "custom_source";
  public static final String CONCAT_SOURCE_SOURCES = "sources";
  public static final String CONCAT_SOURCE_BASE_SPECS = "base_specs";
  public static final String SOURCE_STEP_INPUT = "custom_source_step_input";
  public static final String SOURCE_SPEC = "spec";
  public static final String SOURCE_METADATA = "metadata";
  public static final String SOURCE_DOES_NOT_NEED_SPLITTING = "does_not_need_splitting";
  public static final String SOURCE_PRODUCES_SORTED_KEYS = "produces_sorted_keys";
  public static final String SOURCE_IS_INFINITE = "is_infinite";
  public static final String SOURCE_ESTIMATED_SIZE_BYTES = "estimated_size_bytes";
  public static final String ELEMENT = "element";
  public static final String ELEMENTS = "elements";
  public static final String ENCODING = "encoding";
  public static final String ENCODING_ID = "encoding_id";
  public static final String END_INDEX = "end_index";
  public static final String END_OFFSET = "end_offset";
  public static final String END_SHUFFLE_POSITION = "end_shuffle_position";
  public static final String ENVIRONMENT_VERSION_JOB_TYPE_KEY = "job_type";
  public static final String ENVIRONMENT_VERSION_MAJOR_KEY = "major";
  public static final String FILENAME = "filename";
  public static final String FILENAME_PREFIX = "filename_prefix";
  public static final String FILENAME_SUFFIX = "filename_suffix";
  public static final String FILEPATTERN = "filepattern";
  public static final String FOOTER = "footer";
  public static final String FORMAT = "format";
  public static final String HEADER = "header";
  public static final String INPUTS = "inputs";
  public static final String INPUT_CODER = "input_coder";
  public static final String IS_GENERATED = "is_generated";
  public static final String IS_MERGING_WINDOW_FN = "is_merging_window_fn";
  public static final String IS_PAIR_LIKE = "is_pair_like";
  public static final String IS_STREAM_LIKE = "is_stream_like";
  public static final String IS_WRAPPER = "is_wrapper";
  public static final String DISALLOW_COMBINER_LIFTING = "disallow_combiner_lifting";
  public static final String NON_PARALLEL_INPUTS = "non_parallel_inputs";
  public static final String NUM_SHARD_CODERS = "num_shard_coders";
  public static final String NUM_METADATA_SHARD_CODERS = "num_metadata_shard_coders";
  public static final String NUM_SHARDS = "num_shards";
  public static final String OBJECT_TYPE_NAME = "@type";
  public static final String OUTPUT = "output";
  public static final String OUTPUT_INFO = "output_info";
  public static final String OUTPUT_NAME = "output_name";
  public static final String PARALLEL_INPUT = "parallel_input";
  public static final String PHASE = "phase";
  public static final String PUBSUB_ID_ATTRIBUTE = "pubsub_id_label";
  public static final String PUBSUB_SERIALIZED_ATTRIBUTES_FN = "pubsub_serialized_attributes_fn";
  public static final String PUBSUB_SUBSCRIPTION = "pubsub_subscription";
  public static final String PUBSUB_SUBSCRIPTION_OVERRIDE = "pubsub_subscription_runtime_override";
  public static final String PUBSUB_TIMESTAMP_ATTRIBUTE = "pubsub_timestamp_label";
  public static final String PUBSUB_TOPIC = "pubsub_topic";
  public static final String PUBSUB_TOPIC_OVERRIDE = "pubsub_topic_runtime_override";
  public static final String SCALAR_FIELD_NAME = "value";
  public static final String SERIALIZED_FN = "serialized_fn";
  public static final String SHARD_NAME_TEMPLATE = "shard_template";
  public static final String SHUFFLE_KIND = "shuffle_kind";
  public static final String SHUFFLE_READER_CONFIG = "shuffle_reader_config";
  public static final String SHUFFLE_WRITER_CONFIG = "shuffle_writer_config";
  public static final String SORT_VALUES = "sort_values";
  public static final String START_INDEX = "start_index";
  public static final String START_OFFSET = "start_offset";
  public static final String START_SHUFFLE_POSITION = "start_shuffle_position";
  public static final String STRIP_TRAILING_NEWLINES = "strip_trailing_newlines";
  public static final String TUPLE_TAGS = "tuple_tags";
  public static final String USE_INDEXED_FORMAT = "use_indexed_format";
  public static final String USER_FN = "user_fn";
  public static final String USER_NAME = "user_name";
  public static final String USES_KEYED_STATE = "uses_keyed_state";
  public static final String VALIDATE_SINK = "validate_sink";
  public static final String VALIDATE_SOURCE = "validate_source";
  public static final String VALUE = "value";
  public static final String DISPLAY_DATA = "display_data";
}
