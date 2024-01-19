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
package org.apache.beam.sdk.extensions.avro.schemas.transforms;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.schemas.utils.AvroUtils;
import org.apache.beam.sdk.schemas.JavaFieldSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.transforms.Convert;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.UsesSchema;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for the {@link Convert} class. */
@RunWith(JUnit4.class)
@Category(UsesSchema.class)
public class ConvertTest {
  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  /** Test outer POJO. * */
  @DefaultSchema(JavaFieldSchema.class)
  public static class POJO1 {
    public String field1 = "field1";
    public long field2 = 42;
    public POJO1Nested field3 = new POJO1Nested();
    public POJO1Nested[] field4 = new POJO1Nested[] {new POJO1Nested(), new POJO1Nested()};
    public Map<String, POJO1Nested> field5 =
        ImmutableMap.of(
            "first", new POJO1Nested(),
            "second", new POJO1Nested());

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      POJO1 pojo1 = (POJO1) o;
      return field2 == pojo1.field2
          && Objects.equals(field1, pojo1.field1)
          && Objects.equals(field3, pojo1.field3)
          && Arrays.equals(field4, pojo1.field4)
          && Objects.equals(field5, pojo1.field5);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(field1, field2, field3, field5);
      result = 31 * result + Arrays.hashCode(field4);
      return result;
    }
  }

  /** Test inner POJO. * */
  @DefaultSchema(JavaFieldSchema.class)
  public static class POJO1Nested {
    public String yard1 = "yard2";
    public long yard2 = 43;

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      POJO1Nested that = (POJO1Nested) o;
      return yard2 == that.yard2 && Objects.equals(yard1, that.yard1);
    }

    @Override
    public int hashCode() {
      return Objects.hash(yard1, yard2);
    }
  }

  private static final Schema EXPECTED_SCHEMA1_NESTED =
      Schema.builder().addStringField("yard1").addInt64Field("yard2").build();

  private static final Schema EXPECTED_SCHEMA1 =
      Schema.builder()
          .addStringField("field1")
          .addInt64Field("field2")
          .addRowField("field3", EXPECTED_SCHEMA1_NESTED)
          .addArrayField("field4", FieldType.row(EXPECTED_SCHEMA1_NESTED))
          .addMapField("field5", FieldType.STRING, FieldType.row(EXPECTED_SCHEMA1_NESTED))
          .build();

  private static final Row EXPECTED_ROW1_NESTED =
      Row.withSchema(EXPECTED_SCHEMA1_NESTED).addValues("yard2", 43L).build();

  private static final Row EXPECTED_ROW1 =
      Row.withSchema(EXPECTED_SCHEMA1)
          .addValue("field1")
          .addValue(42L)
          .addValue(EXPECTED_ROW1_NESTED)
          .addArray(ImmutableList.of(EXPECTED_ROW1_NESTED, EXPECTED_ROW1_NESTED))
          .addValue(ImmutableMap.of("first", EXPECTED_ROW1_NESTED, "second", EXPECTED_ROW1_NESTED))
          .build();

  private static final GenericRecord EXPECTED_GENERICRECORD1 =
      AvroUtils.toGenericRecord(EXPECTED_ROW1, AvroUtils.toAvroSchema(EXPECTED_SCHEMA1));

  @Test
  @Category(NeedsRunner.class)
  public void testToGenericRecords() {
    PCollection<GenericRecord> records =
        pipeline.apply(Create.of(new POJO1())).apply(Convert.to(GenericRecord.class));
    PAssert.that(records).containsInAnyOrder(EXPECTED_GENERICRECORD1);
    pipeline.run();
  }
}
