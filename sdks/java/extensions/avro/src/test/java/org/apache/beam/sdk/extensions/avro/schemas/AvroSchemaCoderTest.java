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

import java.util.Collection;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.avro.reflect.AvroSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoderTest;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.DateTime;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Enclosed.class)
public class AvroSchemaCoderTest extends SchemaCoderTest {
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

  @RunWith(Parameterized.class)
  public static class AvroTests extends ParameterizedTests {
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
          });
    }
  }
}
