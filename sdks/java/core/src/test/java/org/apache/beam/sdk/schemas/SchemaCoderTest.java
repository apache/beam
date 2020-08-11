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

import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import com.google.auto.value.AutoValue;
import java.time.Instant;
import java.util.Collection;
import java.util.Objects;
import org.apache.avro.reflect.AvroSchema;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.logicaltypes.NanosInstant;
import org.apache.beam.sdk.schemas.utils.SchemaTestUtils;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.junit.runners.Parameterized;

/** Unit tests for {@link Schema}. */
@RunWith(Enclosed.class)
public class SchemaCoderTest {

  private static final Schema INT32_SCHEMA =
      Schema.builder().addInt32Field("a").addInt32Field("b").build();
  public static final Schema LOGICAL_NANOS_SCHEMA =
      Schema.of(Field.of("logicalNanos", FieldType.logicalType(new NanosInstant())));
  public static final Schema FLOATING_POINT_SCHEMA =
      Schema.of(Field.of("float", FieldType.FLOAT), Field.of("double", FieldType.DOUBLE));

  @RunWith(JUnit4.class)
  public static class SingletonTests {
    @Test
    public void equals_sameSchemaDifferentType_returnsFalse() throws NoSuchSchemaException {
      SchemaCoder autovalueCoder = coderFrom(TypeDescriptor.of(SimpleAutoValue.class));
      SchemaCoder javabeanCoder = coderFrom(TypeDescriptor.of(SimpleBean.class));

      // These coders are *not* the same
      assertNotEquals(autovalueCoder, javabeanCoder);

      // These coders have equivalent schemas, but toRow and fromRow are not equal
      SchemaTestUtils.assertSchemaEquivalent(autovalueCoder.getSchema(), javabeanCoder.getSchema());
      assertNotEquals(autovalueCoder.getToRowFunction(), javabeanCoder.getToRowFunction());
      assertNotEquals(autovalueCoder.getFromRowFunction(), javabeanCoder.getFromRowFunction());
    }
  }

  @AutoValue
  @DefaultSchema(AutoValueSchema.class)
  public abstract static class SimpleAutoValue {
    public abstract String getString();

    public abstract Integer getInt32();

    public abstract Long getInt64();

    public abstract DateTime getDatetime();

    public static SimpleAutoValue of(String string, Integer int32, Long int64, DateTime datetime) {
      return new AutoValue_SchemaCoderTest_SimpleAutoValue(string, int32, int64, datetime);
    }
  }

  @DefaultSchema(JavaBeanSchema.class)
  private static class SimpleBean {
    private String string;
    private Integer int32;
    private Long int64;
    private DateTime datetime;

    public SimpleBean(String string, Integer int32, Long int64, DateTime datetime) {
      this.string = string;
      this.int32 = int32;
      this.int64 = int64;
      this.datetime = datetime;
    }

    public String getString() {
      return string;
    }

    public void setString(String string) {
      this.string = string;
    }

    public Integer getInt32() {
      return int32;
    }

    public void setInt32(Integer int32) {
      this.int32 = int32;
    }

    public Long getInt64() {
      return int64;
    }

    public void setInt64(Long int64) {
      this.int64 = int64;
    }

    public DateTime getDatetime() {
      return datetime;
    }

    public void setDatetime(DateTime datetime) {
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
      SimpleBean that = (SimpleBean) o;
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

  @DefaultSchema(JavaFieldSchema.class)
  private static class SimplePojo {
    public String string;
    public Integer int32;
    public Long int64;
    public DateTime datetime;

    public SimplePojo(String string, Integer int32, Long int64, DateTime datetime) {
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
      SimplePojo that = (SimplePojo) o;
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
    public ImmutableList<Object> testValues;

    @Parameterized.Parameter(2)
    public boolean expectDeterministic;

    @Parameterized.Parameters(name = "{index}: coder = {0}")
    public static Collection<Object[]> data() throws NoSuchSchemaException {
      return ImmutableList.of(
          new Object[] {
            SchemaCoder.of(INT32_SCHEMA),
            ImmutableList.of(
                Row.withSchema(INT32_SCHEMA).addValues(9001, 9002).build(),
                Row.withSchema(INT32_SCHEMA).addValues(3, 4).build()),
            true
          },
          new Object[] {
            coderFrom(TypeDescriptor.of(SimpleAutoValue.class)),
            ImmutableList.of(
                SimpleAutoValue.of(
                    "foo", 9001, 0L, new DateTime().withDate(1979, 3, 14).withTime(10, 30, 0, 0)),
                SimpleAutoValue.of(
                    "bar", 9002, 1L, new DateTime().withDate(1989, 3, 14).withTime(10, 30, 0, 0))),
            true
          },
          new Object[] {
            coderFrom(TypeDescriptor.of(SimpleBean.class)),
            ImmutableList.of(
                new SimpleBean(
                    "foo", 9001, 0L, new DateTime().withDate(1979, 3, 14).withTime(10, 30, 0, 0)),
                new SimpleBean(
                    "bar", 9002, 1L, new DateTime().withDate(1989, 3, 14).withTime(10, 30, 0, 0))),
            true
          },
          new Object[] {
            coderFrom(TypeDescriptor.of(SimplePojo.class)),
            ImmutableList.of(
                new SimplePojo(
                    "foo", 9001, 0L, new DateTime().withDate(1979, 3, 14).withTime(10, 30, 0, 0)),
                new SimplePojo(
                    "bar", 9002, 1L, new DateTime().withDate(1989, 3, 14).withTime(10, 30, 0, 0))),
            true
          },
          new Object[] {
            coderFrom(TypeDescriptor.of(SimpleAvro.class)),
            ImmutableList.of(
                new SimpleAvro(
                    "foo", 9001, 0L, new DateTime().withDate(1979, 3, 14).withTime(10, 30, 0, 0)),
                new SimpleAvro(
                    "bar", 9002, 1L, new DateTime().withDate(1989, 3, 14).withTime(10, 30, 0, 0))),
            true
          },
          new Object[] {
            RowCoder.of(LOGICAL_NANOS_SCHEMA),
            ImmutableList.of(
                Row.withSchema(LOGICAL_NANOS_SCHEMA)
                    .withFieldValue("logicalNanos", Instant.ofEpochMilli(9001))
                    .build()),
            true
          },
          new Object[] {
            RowCoder.of(FLOATING_POINT_SCHEMA),
            ImmutableList.of(
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
      for (Object testValueA : testValues) {
        for (Object testValueB : testValues) {
          CoderProperties.coderConsistentWithEquals(coder, testValueA, testValueB);
        }
      }
    }

    @Test
    public void verifyDeterministic() throws Exception {
      if (expectDeterministic) {
        assertDeterministic(coder);
      } else {
        assertNonDeterministic(coder);
      }
    }
  }

  private static void assertDeterministic(SchemaCoder coder) {
    try {
      coder.verifyDeterministic();
    } catch (NonDeterministicException e) {
      fail("Expected " + coder + " to be deterministic, but got:\n" + e);
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
