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
package org.apache.beam.sdk.schemas;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.utils.JsonUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class JsonSchemaConversionTest {

  @Test
  public void testBasicJsonSchemaToBeamSchema() throws IOException {
    try (InputStream inputStream =
        getClass().getResourceAsStream("/schemas/json/basic_json_schema.json")) {
      String stringJsonSchema = new String(ByteStreams.toByteArray(inputStream), "UTF-8");
      Schema parsedSchema = JsonUtils.beamSchemaFromJsonSchema(stringJsonSchema);

      assertThat(
          parsedSchema.getFieldNames(),
          containsInAnyOrder("booleanProp", "integerProp", "numberProp", "stringProp"));
      assertThat(
          parsedSchema.getFields().stream().map(Schema.Field::getType).collect(Collectors.toList()),
          containsInAnyOrder(
              Schema.FieldType.BOOLEAN,
              Schema.FieldType.INT64,
              Schema.FieldType.DOUBLE,
              Schema.FieldType.STRING));
    }
  }

  @Test
  public void testNestedStructsJsonSchemaToBeamSchema() throws IOException {
    try (InputStream inputStream =
        getClass().getResourceAsStream("/schemas/json/nested_arrays_objects_json_schema.json")) {
      String stringJsonSchema = new String(ByteStreams.toByteArray(inputStream), "UTF-8");
      Schema parsedSchema = JsonUtils.beamSchemaFromJsonSchema(stringJsonSchema);

      assertThat(parsedSchema.getFieldNames(), containsInAnyOrder("fruits", "vegetables"));
      assertThat(
          parsedSchema.getFields().stream().map(Schema.Field::getType).collect(Collectors.toList()),
          containsInAnyOrder(
              Schema.FieldType.array(Schema.FieldType.STRING),
              Schema.FieldType.array(
                  Schema.FieldType.row(
                      Schema.of(
                          Schema.Field.of("veggieName", Schema.FieldType.STRING),
                          Schema.Field.of("veggieLike", Schema.FieldType.BOOLEAN))))));
    }
  }
}
