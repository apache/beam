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
package org.apache.beam.sdk.io.iceberg;

import static java.util.Collections.singletonList;
import static org.apache.beam.sdk.io.iceberg.IcebergUtils.beamSchemaToIcebergSchema;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.logicaltypes.SqlTypes;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.transforms.Days;
import org.apache.iceberg.transforms.Hours;
import org.apache.iceberg.transforms.Months;
import org.apache.iceberg.transforms.Transform;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.Test;

/** Tests for {@link PartitionUtils}. */
public class PartitionUtilsTest {
  private static final Schema SCHEMA =
      Schema.builder()
          .addStringField("i_str")
          .addStringField("t_str")
          .addInt32Field("b_int")
          .addNullableStringField("v_str")
          .addLogicalTypeField("y_datetime", SqlTypes.DATETIME)
          .addLogicalTypeField("m_date", SqlTypes.DATE)
          .addLogicalTypeField("d_date", SqlTypes.DATE)
          .addDateTimeField("h_datetimetz")
          .build();

  @Test
  public void testInvalidPartitionFields() {
    List<String> invalids =
        Arrays.asList(
            "unknown(i_str)", "truncate(missing_int)", "bucket(missing_int)", "s p a c e s");

    for (String invalid : invalids) {
      assertThrows(
          IllegalArgumentException.class,
          () -> PartitionUtils.toPartitionSpec(singletonList(invalid), SCHEMA));
    }
  }

  @Test
  public void testIdentity() {
    String field = "i_str";
    TestCase.partitioningOn(field)
        .outputFieldName("i_str")
        .expectingTransform("Identity")
        .validate();
  }

  @Test
  public void testTruncate() {
    String field = "truncate(t_str, 5)";
    TestCase.partitioningOn(field)
        .outputFieldName("t_str_trunc")
        .expectingTransform("Truncate")
        .validate();
  }

  @Test
  public void testBucket() {
    String field = "bucket(b_int, 5)";
    TestCase.partitioningOn(field)
        .outputFieldName("b_int_bucket")
        .expectingTransform("Bucket")
        .validate();
  }

  @Test
  public void testAlwaysNull() {
    String field = "void(v_str)";
    TestCase.partitioningOn(field)
        .outputFieldName("v_str_null")
        .expectingTransform("VoidTransform")
        .validate();
  }

  @Test
  public void testYear() {
    String field = "year(y_datetime)";
    TestCase.partitioningOn(field)
        .outputFieldName("y_datetime_year")
        .expectingTransform("Years")
        .validate();
  }

  @Test
  public void testMonth() {
    String field = "month(m_date)";
    TestCase.partitioningOn(field)
        .outputFieldName("m_date_month")
        .expectingTransform(Months.class)
        .validate();
  }

  @Test
  public void testDay() {
    String field = "day(d_date)";
    TestCase.partitioningOn(field)
        .outputFieldName("d_date_day")
        .expectingTransform(Days.class)
        .validate();
  }

  @Test
  public void testHour() {
    String field = "hour(h_datetimetz)";
    TestCase.partitioningOn(field)
        .outputFieldName("h_datetimetz_hour")
        .expectingTransform(Hours.class)
        .validate();
  }

  @Test
  public void testAll() {
    PartitionSpec spec =
        PartitionUtils.toPartitionSpec(
            Arrays.asList(
                "i_str",
                "truncate(t_str, 5)",
                "bucket(b_int, 3)",
                "void(v_str)",
                "year(y_datetime)",
                "month(m_date)",
                "day(d_date)",
                "hour(h_datetimetz)"),
            SCHEMA);

    PartitionSpec expectedSpec =
        PartitionSpec.builderFor(beamSchemaToIcebergSchema(SCHEMA))
            .identity("i_str")
            .truncate("t_str", 5)
            .bucket("b_int", 3)
            .alwaysNull("v_str")
            .year("y_datetime")
            .month("m_date")
            .day("d_date")
            .hour("h_datetimetz")
            .build();

    assertEquals(expectedSpec, spec);
  }

  static class TestCase {
    private final String field;
    private @Nullable String name;

    @SuppressWarnings("rawtypes")
    private @Nullable Class<? extends Transform> expected;

    private @Nullable String expectedString;

    TestCase(String field) {
      this.field = field;
    }

    static TestCase partitioningOn(String field) {
      return new TestCase(field);
    }

    TestCase outputFieldName(String name) {
      this.name = name;
      return this;
    }

    @SuppressWarnings("rawtypes")
    TestCase expectingTransform(Class<? extends Transform> expected) {
      this.expected = expected;
      return this;
    }

    TestCase expectingTransform(String expected) {
      this.expectedString = expected;
      return this;
    }

    void validate() {
      PartitionSpec spec = PartitionUtils.toPartitionSpec(singletonList(field), SCHEMA);

      if (!Objects.equals("VoidTransform", expectedString)) {
        assertTrue(spec.isPartitioned());
      }
      assertEquals(1, spec.fields().size());
      PartitionField partitionField = spec.fields().get(0);
      if (expected != null) {
        assertEquals(expected, partitionField.transform().getClass());
      } else if (expectedString != null) {
        assertEquals(expectedString, partitionField.transform().getClass().getSimpleName());
      }
      assertEquals(name, partitionField.name(), name);
    }
  }
}
