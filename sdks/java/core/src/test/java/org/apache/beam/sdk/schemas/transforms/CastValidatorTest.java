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
package org.apache.beam.sdk.schemas.transforms;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.testing.UsesSchema;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link Cast.Widening}, {@link Cast.Narrowing}. */
@Category(UsesSchema.class)
@RunWith(JUnit4.class)
public class CastValidatorTest {

  public static final Map<TypeName, Number> NUMERICS =
      ImmutableMap.<TypeName, Number>builder()
          .put(TypeName.BYTE, Byte.valueOf((byte) 42))
          .put(TypeName.INT16, Short.valueOf((short) 42))
          .put(TypeName.INT32, Integer.valueOf(42))
          .put(TypeName.INT64, Long.valueOf(42))
          .put(TypeName.FLOAT, Float.valueOf(42))
          .put(TypeName.DOUBLE, Double.valueOf(42))
          .put(TypeName.DECIMAL, BigDecimal.valueOf(42))
          .build();

  public static final List<TypeName> NUMERIC_ORDER =
      ImmutableList.of(
          TypeName.BYTE,
          TypeName.INT16,
          TypeName.INT32,
          TypeName.INT64,
          TypeName.FLOAT,
          TypeName.DOUBLE,
          TypeName.DECIMAL);

  @Test
  public void testWideningOrder() {
    NUMERICS
        .keySet()
        .forEach(input -> NUMERICS.keySet().forEach(output -> testWideningOrder(input, output)));
  }

  @Test
  public void testCasting() {
    NUMERICS
        .keySet()
        .forEach(input -> NUMERICS.keySet().forEach(output -> testCasting(input, output)));
  }

  private void testCasting(TypeName inputType, TypeName outputType) {
    Object output =
        Cast.castValue(NUMERICS.get(inputType), FieldType.of(inputType), FieldType.of(outputType));

    assertEquals(NUMERICS.get(outputType), output);
  }

  @Test
  public void testCastingCompleteness() {
    boolean all =
        NUMERIC_ORDER.stream().filter(TypeName::isNumericType).allMatch(NUMERIC_ORDER::contains);

    assertTrue(all);
  }

  private void testWideningOrder(TypeName input, TypeName output) {
    Schema inputSchema = Schema.of(Schema.Field.of("f0", FieldType.of(input)));
    Schema outputSchema = Schema.of(Schema.Field.of("f0", FieldType.of(output)));

    List<Cast.CompatibilityError> errors = Cast.Widening.of().apply(inputSchema, outputSchema);

    if (NUMERIC_ORDER.indexOf(input) <= NUMERIC_ORDER.indexOf(output)) {
      assertThat(input + " is before " + output, errors, empty());
    } else {
      assertThat(input + " is after " + output, errors, not(empty()));
    }
  }

  @Test
  public void testWideningNullableToNotNullable() {
    Schema input = Schema.of(Schema.Field.nullable("f0", FieldType.INT32));
    Schema output = Schema.of(Schema.Field.of("f0", FieldType.INT32));

    List<Cast.CompatibilityError> errors = Cast.Widening.of().apply(input, output);
    Cast.CompatibilityError expected =
        Cast.CompatibilityError.create(
            Arrays.asList("f0"), "Can't cast nullable field to non-nullable field");

    assertThat(errors, containsInAnyOrder(expected));
  }

  @Test
  public void testNarrowingNullableToNotNullable() {
    Schema input = Schema.of(Schema.Field.nullable("f0", FieldType.INT32));
    Schema output = Schema.of(Schema.Field.of("f0", FieldType.INT32));

    List<Cast.CompatibilityError> errors = Cast.Narrowing.of().apply(input, output);

    assertThat(errors, empty());
  }
}
