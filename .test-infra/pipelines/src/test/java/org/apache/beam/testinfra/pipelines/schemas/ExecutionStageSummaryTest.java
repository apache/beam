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
package org.apache.beam.testinfra.pipelines.schemas;

import com.google.dataflow.v1beta3.ExecutionStageSummary;
import com.google.protobuf.Descriptors.Descriptor;
import java.util.Set;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.NonNull;

/**
 * Tests for converting an {@link ExecutionStageSummary} to a {@link Row} using {@link
 * GeneratedMessageV3RowBuilder}.
 */
class ExecutionStageSummaryTest
    extends AbstractGeneratedMessageV3RowBuilderTest<ExecutionStageSummary> {

  @Override
  protected @NonNull Descriptor getDescriptorForType() {
    return ExecutionStageSummary.getDescriptor();
  }

  @Override
  protected @NonNull ExecutionStageSummary getDefaultInstance() {
    return ExecutionStageSummary.getDefaultInstance();
  }

  @Override
  protected @NonNull Class<ExecutionStageSummary> getDefaultInstanceClass() {
    return ExecutionStageSummary.class;
  }

  @Override
  protected @NonNull Set<@NonNull String> getStringFields() {
    return ImmutableSet.of("name", "id");
  }

  @Override
  protected @NonNull Set<@NonNull String> getStringArrayFields() {
    return ImmutableSet.of("prerequisite_stage");
  }

  @Override
  protected @NonNull Set<@NonNull String> getBooleanFields() {
    return ImmutableSet.of();
  }

  @Override
  protected @NonNull Set<@NonNull String> getStructFields() {
    return ImmutableSet.of();
  }

  @Override
  protected @NonNull Set<@NonNull String> getEnumFields() {
    return ImmutableSet.of("kind");
  }

  @Override
  protected @NonNull Set<@NonNull String> getDisplayDataFields() {
    return ImmutableSet.of();
  }

  @Override
  protected @NonNull Set<@NonNull String> getRepeatedMessageFields() {
    return ImmutableSet.of(
        "input_source", "output_source", "component_transform", "component_source");
  }
}
