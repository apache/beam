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
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.dataflow.v1beta3.Job;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import java.util.stream.Collectors;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

class GeneratedMessageV3ReflectionTest {

  @Test
  void getNonRepeatedFieldsForType_Job() {
    GeneratedMessageV3Reflection<Job> reflection = new GeneratedMessageV3Reflection<>(Job.class);
    assertEquals(
        ImmutableSet.of(
            "id",
            "project_id",
            "name",
            "steps_location",
            "replace_job_id",
            "client_request_id",
            "replaced_by_job_id",
            "location",
            "created_from_snapshot_id"),
        reflection.getNonRepeatedFieldsForType(JavaType.STRING).stream()
            .map(FieldDescriptor::getName)
            .collect(Collectors.toSet()));

    assertEquals(
        ImmutableSet.of("satisfies_pzs"),
        reflection.getNonRepeatedFieldsForType(JavaType.BOOLEAN).stream()
            .map(FieldDescriptor::getName)
            .collect(Collectors.toSet()));

    assertEquals(
        ImmutableSet.of(
            "environment",
            "current_state_time",
            "execution_info",
            "create_time",
            "pipeline_description",
            "job_metadata",
            "start_time"),
        reflection.getNonRepeatedFieldsForType(JavaType.MESSAGE).stream()
            .map(FieldDescriptor::getName)
            .collect(Collectors.toSet()));
  }

  @Test
  void getRepeatedFieldsForType() {
    GeneratedMessageV3Reflection<Job> reflection = new GeneratedMessageV3Reflection<>(Job.class);
    assertEquals(
        ImmutableSet.of("temp_files"),
        reflection.getRepeatedFieldsForType(JavaType.STRING).stream()
            .map(FieldDescriptor::getName)
            .collect(Collectors.toSet()));
  }

  @Test
  void getNonRepeatedEnumTypes_Job() {
    GeneratedMessageV3Reflection<Job> reflection = new GeneratedMessageV3Reflection<>(Job.class);
    assertEquals(
        ImmutableSet.of("type", "current_state", "requested_state"),
        reflection.getNonRepeatedEnumTypes().stream()
            .map(FieldDescriptor::getName)
            .collect(Collectors.toSet()));
  }

  @Test
  void getRepeatedEnumTypes_Job() {
    GeneratedMessageV3Reflection<Job> reflection = new GeneratedMessageV3Reflection<>(Job.class);
    assertTrue(reflection.getRepeatedEnumTypes().isEmpty());
  }

  @Test
  void getNonRepeatedExtensions_Job() {
    GeneratedMessageV3Reflection<Job> reflection = new GeneratedMessageV3Reflection<>(Job.class);
    assertTrue(reflection.getNonRepeatedExtensions().isEmpty());
  }

  @Test
  void getRepeatedExtensions_Job() {
    GeneratedMessageV3Reflection<Job> reflection = new GeneratedMessageV3Reflection<>(Job.class);
    assertTrue(reflection.getRepeatedExtensions().isEmpty());
  }

  @Test
  void getNonRepeatedMapTypes_Job() {
    GeneratedMessageV3Reflection<Job> reflection = new GeneratedMessageV3Reflection<>(Job.class);
    assertEquals(
        ImmutableSet.of("transform_name_mapping", "labels"),
        reflection.getNonRepeatedMapTypes().stream()
            .map(FieldDescriptor::getName)
            .collect(Collectors.toSet()));
  }

  @Test
  void hasOptionalKeyword_Job_isEmpty() {
    GeneratedMessageV3Reflection<Job> reflection = new GeneratedMessageV3Reflection<>(Job.class);
    assertTrue(
        reflection.getFields().stream()
            .filter(FieldDescriptor::hasOptionalKeyword)
            .collect(Collectors.toSet())
            .isEmpty());
  }
}
