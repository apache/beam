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
package org.apache.beam.examples.iceberg.snippets;

// [START iceberg_schema_and_row]
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.beam.sdk.schemas.logicaltypes.Timestamp;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;

public class IcebergBeamSchemaAndRow {
  public Row createRow() {
    Schema nestedSchema =
        Schema.builder().addStringField("nested_field").addInt32Field("nested_field_2").build();
    Schema beamSchema =
        Schema.builder()
            .addBooleanField("boolean_field")
            .addInt32Field("int_field")
            .addInt64Field("long_field")
            .addFloatField("float_field")
            .addDoubleField("double_field")
            .addDecimalField("numeric_field")
            .addByteArrayField("bytes_field")
            .addStringField("string_field")
            .addLogicalTypeField("time_field", SqlTypes.TIME)
            .addLogicalTypeField("date_field", SqlTypes.DATE)
            .addLogicalTypeField("timestamp_field", Timestamp.MICROS)
            .addDateTimeField("timestamptz_field")
            .addArrayField("array_field", Schema.FieldType.INT32)
            .addMapField("map_field", Schema.FieldType.STRING, Schema.FieldType.INT32)
            .addRowField("struct_field", nestedSchema)
            .build();

    Row beamRow =
        Row.withSchema(beamSchema)
            .withFieldValues(
                ImmutableMap.<String, Object>builder()
                    .put("boolean_field", true)
                    .put("int_field", 1)
                    .put("long_field", 2L)
                    .put("float_field", 3.4f)
                    .put("double_field", 4.5d)
                    .put("numeric_field", new BigDecimal(67))
                    .put("bytes_field", new byte[] {1, 2, 3})
                    .put("string_field", "value")
                    .put("time_field", LocalTime.now(ZoneOffset.UTC))
                    .put("date_field", LocalDate.now(ZoneOffset.UTC))
                    .put("timestamp_field", Instant.now())
                    .put("timestamptz_field", DateTime.now())
                    .put("array_field", Arrays.asList(1, 2, 3))
                    .put("map_field", ImmutableMap.of("a", 1, "b", 2))
                    .put(
                        "struct_field",
                        Row.withSchema(nestedSchema).addValues("nested_value", 123).build())
                    .build())
            .build();

    return beamRow;
  }
}
// [END iceberg_schema_and_row]
