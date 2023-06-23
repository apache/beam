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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.dataflow.v1beta3.DebugOptions;
import com.google.dataflow.v1beta3.Environment;
import com.google.dataflow.v1beta3.WorkerPool;
import com.google.protobuf.Descriptors.Descriptor;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.junit.jupiter.api.Test;

/**
 * Tests for converting an {@link Environment} to a {@link Row} using {@link
 * GeneratedMessageV3RowBuilder}.
 */
class EnvironmentRowBuilderTest extends AbstractGeneratedMessageV3RowBuilderTest<Environment> {

  @Override
  protected @NonNull Descriptor getDescriptorForType() {
    return Environment.getDescriptor();
  }

  @Override
  protected @NonNull Environment getDefaultInstance() {
    return Environment.getDefaultInstance();
  }

  @Override
  protected @NonNull Class<Environment> getDefaultInstanceClass() {
    return Environment.class;
  }

  @Override
  protected @NonNull Set<@NonNull String> getStringFields() {
    return ImmutableSet.of(
        "temp_storage_prefix",
        "cluster_manager_api_service",
        "service_kms_key_name",
        "dataset",
        "service_account_email",
        "worker_region",
        "worker_zone");
  }

  @Override
  protected @NonNull Set<@NonNull String> getStringArrayFields() {
    return ImmutableSet.of("experiments", "service_options");
  }

  @Override
  protected @NonNull Set<@NonNull String> getBooleanFields() {
    return Collections.emptySet();
  }

  @Override
  protected @NonNull Set<@NonNull String> getStructFields() {
    return ImmutableSet.of("user_agent", "version", "sdk_pipeline_options");
  }

  @Override
  protected @NonNull Set<@NonNull String> getEnumFields() {
    return ImmutableSet.of("flex_resource_scheduling_goal", "shuffle_mode");
  }

  @Override
  protected @NonNull Set<@NonNull String> getDisplayDataFields() {
    return ImmutableSet.of();
  }

  @Override
  protected @NonNull Set<@NonNull String> getRepeatedMessageFields() {
    return ImmutableSet.of();
  }

  @Test
  void workerPools() {
    String fieldName = "worker_pools";
    Environment defaultInstance = getDefaultInstance();
    Row defaultRow = builderOf(defaultInstance).build();
    assertNotNull(defaultRow);
    Collection<Row> defaultCollection = defaultRow.getArray(fieldName);
    assertNotNull(defaultCollection);
    assertTrue(defaultCollection.isEmpty());

    Environment instance =
        getDefaultInstance().toBuilder().addWorkerPools(WorkerPool.getDefaultInstance()).build();
    Row row = builderOf(instance).build();
    Collection<Row> collection = row.getArray(fieldName);
    assertNotNull(collection);
    assertEquals(1, collection.size());
  }

  @Test
  void debugOptions() {
    String fieldName = "debug_options";
    Environment defaultInstance = getDefaultInstance();
    Row defaultRow = builderOf(defaultInstance).build();
    assertNotNull(defaultRow);
    Row defaultDebugOptions = defaultRow.getRow(fieldName);
    assertNotNull(defaultDebugOptions);
    assertTrue(defaultDebugOptions.getSchema().hasField("enable_hot_key_logging"));
    assertEquals(false, defaultDebugOptions.getBoolean("enable_hot_key_logging"));

    Environment instance =
        getDefaultInstance()
            .toBuilder()
            .setDebugOptions(
                DebugOptions.getDefaultInstance().toBuilder().setEnableHotKeyLogging(true).build())
            .build();
    Row row = builderOf(instance).build();
    assertNotNull(row);
    Row debugOptions = row.getRow(fieldName);
    assertNotNull(debugOptions);
    assertEquals(true, debugOptions.getBoolean("enable_hot_key_logging"));
  }
}
