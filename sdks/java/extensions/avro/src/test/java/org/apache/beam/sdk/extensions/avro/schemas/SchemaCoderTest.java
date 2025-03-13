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
package org.apache.beam.sdk.extensions.avro.schemas;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;

import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.avro.reflect.AvroSchema;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.logicaltypes.NanosInstant;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Unit tests for {@link Schema}. */
@RunWith(Enclosed.class)
@SuppressWarnings({
  "rawtypes", // TODO(https://github.com/apache/beam/issues/20447)
})
public class SchemaCoderTest {

  public static final Schema LOGICAL_NANOS_SCHEMA =
      Schema.of(Field.of("logicalNanos", FieldType.logicalType(new NanosInstant())));
  public static final Schema FLOATING_POINT_SCHEMA =
      Schema.of(Field.of("float", FieldType.FLOAT), Field.of("double", FieldType.DOUBLE));

  @DefaultSchema(AvroRecordSchema.class)
  private static class SimpleAvro {
    public String string;
    public Integer int32;
    public Long int64;

    @AvroSchema("{\"type\": \"long\", \"logicalType\": \"timestamp-millis\"}")
    public DateTime datetime;

    public SimpleAvro(String string, Integer int32, Long int64, DateTime datetime) {
      this.string = string;
      this.int32 = int32;
      this.int64 = int64;
      this.datetime = datetime;
    }

    @Override
    public boolean equals(@Nullable Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SimpleAvro that = (SimpleAvro) o;
      return string.equals(that.string)
          && int32.equals(that.int32)
          && int64.equals(that.int64)
          && datetime.equals(that.datetime);
    }

    @Override
    public int hashCode() {
      return Objects.hash(string, int32, int64, datetime);
    }
  }

  private static final SchemaRegistry REGISTRY = SchemaRegistry.createDefault();

  private static SchemaCoder coderFrom(TypeDescriptor typeDescriptor) throws NoSuchSchemaException {
    return SchemaCoder.of(
        REGISTRY.getSchema(typeDescriptor),
        typeDescriptor,
        REGISTRY.getToRowFunction(typeDescriptor),
        REGISTRY.getFromRowFunction(typeDescriptor));
  }

  @RunWith(Parameterized.class)
  public static class ParameterizedTests {

    @Parameterized.Parameter(0)
    public SchemaCoder coder;

    @Parameterized.Parameter(1)
    public ImmutableList<Supplier<Object>> testValues;

    @Parameterized.Parameter(2)
    public boolean expectDeterministic;

    @Parameterized.Parameters(name = "{index}: coder = {0}")
    public static Collection<Object[]> data() throws NoSuchSchemaException {
      return ImmutableList.of(
          new Object[] {
            coderFrom(TypeDescriptor.of(SimpleAvro.class)),
            ImmutableList.<Supplier<Object>>of(
                () ->
                    new SimpleAvro(
                        "foo",
                        9001,
                        0L,
                        new DateTime().withDate(1979, 3, 14).withTime(10, 30, 0, 0)),
                () ->
                    new SimpleAvro(
                        "bar",
                        9002,
                        1L,
                        new DateTime().withDate(1989, 3, 14).withTime(10, 30, 0, 0))),
            true
          },
          new Object[] {
            RowCoder.of(LOGICAL_NANOS_SCHEMA),
            ImmutableList.<Supplier<Object>>of(
                () ->
                    Row.withSchema(LOGICAL_NANOS_SCHEMA)
                        .withFieldValue("logicalNanos", Instant.ofEpochMilli(9001))
                        .build()),
            true
          },
          new Object[] {
            RowCoder.of(FLOATING_POINT_SCHEMA),
            ImmutableList.<Supplier<Object>>of(
                () ->
                    Row.withSchema(FLOATING_POINT_SCHEMA)
                        .withFieldValue("float", (float) 1.0)
                        .withFieldValue("double", 2.0)
                        .build()),
            false
          });
    }

    @Test
    public void coderSerializable() {
      CoderProperties.coderSerializable(coder);
    }

    @Test
    public void coderConsistentWithEquals() throws Exception {
      for (Supplier<Object> testValueA : testValues) {
        for (Supplier<Object> testValueB : testValues) {
          CoderProperties.coderConsistentWithEquals(coder, testValueA.get(), testValueB.get());
        }
      }
    }

    @Test
    public void verifyDeterministic() throws Exception {
      if (expectDeterministic) {
        for (Supplier<Object> testValue : testValues) {
          CoderProperties.coderDeterministic(coder, testValue.get(), testValue.get());
        }
      } else {
        assertNonDeterministic(coder);
      }
    }
  }

  private static void assertNonDeterministic(SchemaCoder<?> coder) {
    try {
      coder.verifyDeterministic();
      fail("Expected " + coder + " to be non-deterministic.");
    } catch (NonDeterministicException e) {
      assertThat(e.getReasons(), Matchers.iterableWithSize(1));
    }
  }
}
