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

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;
import static org.apache.beam.testinfra.pipelines.schemas.GeneratedMessageV3SchemaBuilder.buildField;
import static org.apache.beam.testinfra.pipelines.schemas.GeneratedMessageV3SchemaBuilder.determineFieldType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.dataflow.v1beta3.Job;
import com.google.protobuf.Descriptors;
import java.util.HashSet;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

class GeneratedMessageV3SchemaBuilderTest {

  @Test
  void build_Job_allFieldNamesMatch() {
    GeneratedMessageV3Reflection<Job> reflection = new GeneratedMessageV3Reflection<>(Job.class);
    GeneratedMessageV3SchemaBuilder<Job> builder =
        new GeneratedMessageV3SchemaBuilder<>(Schema.builder(), reflection);
    Schema schema = builder.build();
    assertNotNull(schema);
    assertEquals(
        ImmutableSet.of(
            "id",
            "project_id",
            "name",
            "type",
            "environment",
            "steps",
            "steps_location",
            "current_state",
            "current_state_time",
            "requested_state",
            "execution_info",
            "create_time",
            "replace_job_id",
            "transform_name_mapping",
            "client_request_id",
            "replaced_by_job_id",
            "temp_files",
            "labels",
            "location",
            "pipeline_description",
            "stage_states",
            "job_metadata",
            "start_time",
            "created_from_snapshot_id",
            "satisfies_pzs"),
        new HashSet<>(schema.getFieldNames()));
  }

  @Test
  void build_Job_strings() {
    GeneratedMessageV3Reflection<Job> reflection = new GeneratedMessageV3Reflection<>(Job.class);
    GeneratedMessageV3SchemaBuilder<Job> builder =
        new GeneratedMessageV3SchemaBuilder<>(Schema.builder(), reflection);
    Schema schema = builder.build();
    assertNotNull(schema);
    assertEquals(
        ImmutableSet.of(
            "id",
            "project_id",
            "name",
            "type",
            "steps_location",
            "current_state",
            "requested_state",
            "replace_job_id",
            "client_request_id",
            "replaced_by_job_id",
            "location",
            "created_from_snapshot_id"),
        schema.getFields().stream()
            .filter(field -> field.getType().equals(Schema.FieldType.STRING))
            .map(Schema.Field::getName)
            .collect(Collectors.toSet()));
  }

  @Test
  void build_Job_booleans() {
    GeneratedMessageV3Reflection<Job> reflection = new GeneratedMessageV3Reflection<>(Job.class);
    GeneratedMessageV3SchemaBuilder<Job> builder =
        new GeneratedMessageV3SchemaBuilder<>(Schema.builder(), reflection);
    Schema schema = builder.build();
    assertNotNull(schema);
    assertEquals(
        ImmutableSet.of("satisfies_pzs"),
        schema.getFields().stream()
            .filter(field -> field.getType().equals(Schema.FieldType.BOOLEAN))
            .map(Schema.Field::getName)
            .collect(Collectors.toSet()));
  }

  @Test
  void build_Job_repeated_strings() {
    GeneratedMessageV3Reflection<Job> reflection = new GeneratedMessageV3Reflection<>(Job.class);
    GeneratedMessageV3SchemaBuilder<Job> builder =
        new GeneratedMessageV3SchemaBuilder<>(Schema.builder(), reflection);
    Schema schema = builder.build();
    assertNotNull(schema);
    assertEquals(
        ImmutableSet.of("temp_files"),
        schema.getFields().stream()
            .filter(
                field -> field.getType().equals(Schema.FieldType.array(Schema.FieldType.STRING)))
            .map(Schema.Field::getName)
            .collect(Collectors.toSet()));
  }

  @Test
  void build_Job_maps() {
    GeneratedMessageV3Reflection<Job> reflection = new GeneratedMessageV3Reflection<>(Job.class);
    GeneratedMessageV3SchemaBuilder<Job> builder =
        new GeneratedMessageV3SchemaBuilder<>(Schema.builder(), reflection);
    Schema schema = builder.build();
    assertNotNull(schema);
    assertEquals(
        ImmutableSet.of("transform_name_mapping", "labels"),
        schema.getFields().stream()
            .filter(
                field ->
                    field
                        .getType()
                        .equals(
                            Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.STRING)))
            .map(Schema.Field::getName)
            .collect(Collectors.toSet()));
  }

  @Test
  void build_Job_rows() {
    GeneratedMessageV3Reflection<Job> reflection = new GeneratedMessageV3Reflection<>(Job.class);
    GeneratedMessageV3SchemaBuilder<Job> builder =
        new GeneratedMessageV3SchemaBuilder<>(Schema.builder(), reflection);
    Schema schema = builder.build();
    assertNotNull(schema);
    assertEquals(
        ImmutableSet.of(
            "environment",
            "current_state_time",
            "execution_info",
            "create_time",
            "pipeline_description",
            "job_metadata",
            "start_time"),
        schema.getFields().stream()
            .filter(field -> field.getType().getTypeName().equals(Schema.TypeName.ROW))
            .map(Schema.Field::getName)
            .collect(Collectors.toSet()));
  }

  @Test
  void build_Job_repeated_rows() {
    GeneratedMessageV3Reflection<Job> reflection = new GeneratedMessageV3Reflection<>(Job.class);
    GeneratedMessageV3SchemaBuilder<Job> builder =
        new GeneratedMessageV3SchemaBuilder<>(Schema.builder(), reflection);
    Schema schema = builder.build();
    assertNotNull(schema);
    assertEquals(
        ImmutableSet.of("stage_states", "steps"),
        schema.getFields().stream()
            .filter(field -> field.getType().getTypeName().equals(Schema.TypeName.ARRAY))
            .filter(
                field ->
                    (checkStateNotNull(field.getType().getCollectionElementType())
                        .getTypeName()
                        .equals(Schema.TypeName.ROW)))
            .map(Schema.Field::getName)
            .collect(Collectors.toSet()));
  }

  @Test
  void buildField_Job_name() {
    Descriptors.FieldDescriptor descriptor = getField("name");
    assertFalse(descriptor.hasOptionalKeyword());
    Schema.Field field = buildField(descriptor);
    assertEquals("name", field.getName());
    assertFalse(field.getType().getNullable());
    assertEquals(Schema.FieldType.STRING, field.getType());
    assertNotNull(Job.getDefaultInstance().getName());
  }

  @Test
  void determineFieldType_Job_transform_name_mapping() {
    Descriptors.FieldDescriptor field = getField("transform_name_mapping");
    Schema.FieldType type = determineFieldType(field);
    assertEquals(Schema.TypeName.MAP, type.getTypeName());
    assertEquals(Schema.FieldType.STRING, type.getMapKeyType());
    assertEquals(Schema.FieldType.STRING, type.getMapValueType());
  }

  @Test
  void buildRowFieldType() {
    Descriptors.FieldDescriptor field = getField("current_state_time");
    Schema.FieldType type = determineFieldType(field);
    assertEquals(Schema.TypeName.ROW, type.getTypeName());
    Schema schema = checkStateNotNull(type.getRowSchema());
    assertEquals(ImmutableSet.of("seconds", "nanos"), new HashSet<>(schema.getFieldNames()));
  }

  @Test
  void determineFieldType_Job_labels() {
    Descriptors.FieldDescriptor field = getField("labels");
    Schema.FieldType type = determineFieldType(field);
    assertEquals(Schema.TypeName.MAP, type.getTypeName());
    assertEquals(Schema.FieldType.STRING, type.getMapKeyType());
    assertEquals(Schema.FieldType.STRING, type.getMapValueType());
    assertNotNull(Job.getDefaultInstance().getLabelsMap());
  }

  Descriptors.FieldDescriptor getField(String name) {
    return Job.getDefaultInstance().getDescriptorForType().findFieldByName(name);
  }
}
