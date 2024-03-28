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
package org.apache.beam.runners.dataflow.util;

/** Constant property names used by the SDK in CloudWorkflow specifications. */
public class PropertyNames {
  public static final String CO_GBK_RESULT_SCHEMA = "co_gbk_result_schema";
  public static final String COMPONENT_ENCODINGS = "component_encodings";
  public static final String CUSTOM_SOURCE_FORMAT = "custom_source";
  public static final String SOURCE_STEP_INPUT = "custom_source_step_input";
  public static final String SOURCE_SPEC = "spec";
  public static final String SOURCE_METADATA = "metadata";
  public static final String SOURCE_DOES_NOT_NEED_SPLITTING = "does_not_need_splitting";
  public static final String SOURCE_IS_INFINITE = "is_infinite";
  public static final String SOURCE_ESTIMATED_SIZE_BYTES = "estimated_size_bytes";
  public static final String ENCODING = "encoding";
  public static final String ENVIRONMENT_VERSION_JOB_TYPE_KEY = "job_type";
  public static final String ENVIRONMENT_VERSION_MAJOR_KEY = "major";
  public static final String FORMAT = "format";
  public static final String INPUTS = "inputs";
  public static final String IS_MERGING_WINDOW_FN = "is_merging_window_fn";
  public static final String IS_PAIR_LIKE = "is_pair_like";
  public static final String IS_STREAM_LIKE = "is_stream_like";
  public static final String IS_WRAPPER = "is_wrapper";
  public static final String DISALLOW_COMBINER_LIFTING = "disallow_combiner_lifting";
  public static final String NON_PARALLEL_INPUTS = "non_parallel_inputs";
  public static final String OBJECT_TYPE_NAME = "@type";
  public static final String OUTPUT = "output";
  public static final String OUTPUT_INFO = "output_info";
  public static final String OUTPUT_NAME = "output_name";
  public static final String PARALLEL_INPUT = "parallel_input";
  public static final String PUBSUB_ID_ATTRIBUTE = "pubsub_id_label";
  public static final String PUBSUB_SERIALIZED_ATTRIBUTES_FN = "pubsub_serialized_attributes_fn";

  public static final String PUBSUB_SUBSCRIPTION = "pubsub_subscription";
  public static final String PUBSUB_SUBSCRIPTION_OVERRIDE = "pubsub_subscription_runtime_override";
  public static final String PUBSUB_TIMESTAMP_ATTRIBUTE = "pubsub_timestamp_label";
  public static final String PUBSUB_TOPIC = "pubsub_topic";
  public static final String PUBSUB_TOPIC_OVERRIDE = "pubsub_topic_runtime_override";

  public static final String PUBSUB_DYNAMIC_DESTINATIONS = "pubsub_with_dynamic_destinations";

  public static final String SCALAR_FIELD_NAME = "value";
  public static final String SERIALIZED_FN = "serialized_fn";
  public static final String SERIALIZED_TEST_STREAM = "serialized_test_stream";
  public static final String SORT_VALUES = "sort_values";
  public static final String TUPLE_TAGS = "tuple_tags";
  public static final String USE_INDEXED_FORMAT = "use_indexed_format";
  public static final String USER_FN = "user_fn";
  public static final String USER_NAME = "user_name";
  public static final String USES_KEYED_STATE = "uses_keyed_state";
  public static final String ALLOWS_SHARDABLE_STATE = "allows_shardable_state";
  public static final String VALUE = "value";
  public static final String WINDOWING_STRATEGY = "windowing_strategy";
  public static final String DISPLAY_DATA = "display_data";
  public static final String RESOURCE_HINTS = "resource_hints";
  public static final String PRESERVES_KEYS = "preserves_keys";
  public static final String ALLOW_DUPLICATES = "allow_duplicates";
  /**
   * @deprecated Uses the incorrect terminology. {@link #RESTRICTION_ENCODING}. Should be removed
   *     once non FnAPI SplittableDoFn expansion for Dataflow is removed.
   */
  @Deprecated public static final String RESTRICTION_CODER = "restriction_coder";

  public static final String IMPULSE_ELEMENT = "impulse_element";
  public static final String PIPELINE_PROTO_CODER_ID = "pipeline_proto_coder_id";
  public static final String RESTRICTION_ENCODING = "restriction_encoding";
}
