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
package org.apache.beam.sdk.extensions.spd.profile.adapter;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.beam.sdk.extensions.spd.StructuredPipelineDescription;
import org.apache.beam.sdk.extensions.sql.meta.Table;

public interface ProfileAdapter {

  String getName();

  Table getSourceTable(JsonNode profile, Table table);

  Table getMaterializedTable(JsonNode profile, Table table);

  default void preLoadHook(StructuredPipelineDescription pipelineDescription) {}

  default void preLoadSourceHook(StructuredPipelineDescription pipelineDescription) {}

  default void preLoadMaterializationHook(StructuredPipelineDescription pipelineDescription) {}

  default void preExecuteHook(StructuredPipelineDescription pipelineDescription) {}

  default void preExecuteSourceHook(StructuredPipelineDescription pipelineDescription) {}

  default void preExecuteMaterializationHook(StructuredPipelineDescription pipelineDescription) {}

  default void postExecuteHook(StructuredPipelineDescription pipelineDescription) {}

  default void postExecuteSourceHook(StructuredPipelineDescription pipelineDescription) {}

  default void postExecuteMaterializationHook(StructuredPipelineDescription pipelineDescription) {}
}
