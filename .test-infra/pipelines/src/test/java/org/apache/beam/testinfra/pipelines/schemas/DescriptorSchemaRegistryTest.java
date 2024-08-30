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

import com.google.dataflow.v1beta3.Job;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** Tests for {@link DescriptorSchemaRegistry}. */
class DescriptorSchemaRegistryTest {

  private static final DescriptorSchemaRegistry REGISTRY = DescriptorSchemaRegistry.INSTANCE;

  @BeforeAll
  public static void setup() {
    REGISTRY.build(Job.getDescriptor());
  }

  @Test
  void build_Job() {
    Schema mapSchema =
        Schema.of(
            Schema.Field.of("key", FieldType.STRING), Schema.Field.of("value", FieldType.STRING));
    Schema schema = REGISTRY.getOrBuild(Job.getDescriptor());
    assertEquals(Schema.Field.of("id", FieldType.STRING), schema.getField("id"));
    assertEquals(Schema.Field.of("type", FieldType.STRING), schema.getField("type"));
    assertEquals(
        Schema.Field.of("create_time", FieldType.DATETIME), schema.getField("create_time"));
    assertEquals(
        Schema.Field.of(
            "steps",
            FieldType.array(
                FieldType.row(
                    Schema.of(
                        Schema.Field.of("kind", FieldType.STRING),
                        Schema.Field.of("name", FieldType.STRING),
                        Schema.Field.of(
                            "properties",
                            FieldType.row(
                                Schema.of(
                                    Schema.Field.of(
                                        "fields", FieldType.array(FieldType.row(mapSchema)))))))))),
        schema.getField("steps"));
    assertEquals(
        Schema.Field.of("transform_name_mapping", FieldType.array(FieldType.row(mapSchema))),
        schema.getField("transform_name_mapping"));
    assertEquals(
        Schema.Field.of(
            "stage_states",
            FieldType.array(
                FieldType.row(
                    Schema.of(
                        Schema.Field.of("execution_stage_name", FieldType.STRING),
                        Schema.Field.of("execution_stage_state", FieldType.STRING),
                        Schema.Field.of("current_state_time", FieldType.DATETIME))))),
        schema.getField("stage_states"));
    assertEquals(
        Schema.Field.of("satisfies_pzs", FieldType.BOOLEAN), schema.getField("satisfies_pzs"));
  }
}
