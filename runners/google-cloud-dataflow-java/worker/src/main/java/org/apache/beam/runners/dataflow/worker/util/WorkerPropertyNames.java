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
package org.apache.beam.runners.dataflow.worker.util;

/** Constants private to the exchange between the Dataflow service and the worker. */
public class WorkerPropertyNames {
  public static final String CAPABILITY_REMOTE_SOURCE = "remote_source";
  public static final String COMBINE_FN = "combine_fn";
  public static final String CONCAT_SOURCE_SOURCES = "sources";
  public static final String CONCAT_SOURCE_BASE_SPECS = "base_specs";
  public static final String ELEMENTS = "elements";
  public static final String ENCODED_KEY = "encoded_key";
  public static final String END_INDEX = "end_index";
  public static final String END_OFFSET = "end_offset";
  public static final String END_SHUFFLE_POSITION = "end_shuffle_position";
  public static final String FILENAME = "filename";
  public static final String INPUT_CODER = "input_coder";
  public static final String PHASE = "phase";
  public static final String SHUFFLE_KIND = "shuffle_kind";
  public static final String SHUFFLE_READER_CONFIG = "shuffle_reader_config";
  public static final String SHUFFLE_WRITER_CONFIG = "shuffle_writer_config";
  public static final String SIDE_INPUT_ID = "side_input_id";
  public static final String START_INDEX = "start_index";
  public static final String START_OFFSET = "start_offset";
  public static final String START_SHUFFLE_POSITION = "start_shuffle_position";
  public static final String RESTRICTION_CODER = "restriction_coder";
  public static final String WORK_ITEM_TYPE_MAP_TASK = "map_task";
  public static final String WORK_ITEM_TYPE_REMOTE_SOURCE_TASK = "remote_source_task";
  public static final String WORK_ITEM_TYPE_SEQ_MAP_TASK = "seq_map_task";
  public static final String WORK_ITEM_TYPE_STREAMING_CONFIG_TASK = "streaming_config_task:";
}
