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
package org.apache.beam.sdk.io.gcp.spanner;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Type;
import com.google.cloud.spanner.Value;
import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** A set of unit tests for {@link MutationSizeEstimator}. */
@RunWith(JUnit4.class)
public class MutationSizeEstimatorTest {

  @Test
  public void primitives() throws Exception {
    Mutation int64 = Mutation.newInsertOrUpdateBuilder("test").set("one").to(1).build();
    Mutation float32 = Mutation.newInsertOrUpdateBuilder("test").set("one").to(1.3f).build();
    Mutation float64 = Mutation.newInsertOrUpdateBuilder("test").set("one").to(2.9).build();
    Mutation bool = Mutation.newInsertOrUpdateBuilder("test").set("one").to(false).build();
    Mutation numeric =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .to(new BigDecimal("12345678901234567890.123456789"))
            .build();
    Mutation pgNumeric =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .to(Value.pgNumeric("12345678901234567890.123456789"))
            .build();
    Mutation pgNumericNaN =
        Mutation.newInsertOrUpdateBuilder("test").set("one").to(Value.pgNumeric("NaN")).build();
    Mutation json =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .to(Value.json("{\"key1\":\"value1\", \"key2\":\"value2\"}"))
            .build();
    Mutation deleteDouble = Mutation.delete("test", Key.of(1223.));
    Mutation jsonb =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .to(Value.pgJsonb("{\"key123\":\"value123\", \"key321\":\"value321\"}"))
            .build();

    assertThat(MutationSizeEstimator.sizeOf(int64), is(8L));
    assertThat(MutationSizeEstimator.sizeOf(float32), is(4L));
    assertThat(MutationSizeEstimator.sizeOf(float64), is(8L));
    assertThat(MutationSizeEstimator.sizeOf(bool), is(1L));
    assertThat(MutationSizeEstimator.sizeOf(numeric), is(30L));
    assertThat(MutationSizeEstimator.sizeOf(pgNumeric), is(30L));
    assertThat(MutationSizeEstimator.sizeOf(pgNumericNaN), is(3L));
    assertThat(MutationSizeEstimator.sizeOf(json), is(34L));
    assertThat(MutationSizeEstimator.sizeOf(deleteDouble), is(8L));
    assertThat(MutationSizeEstimator.sizeOf(jsonb), is(42L));
  }

  @Test
  public void primitiveArrays() throws Exception {
    Mutation int64 =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .toInt64Array(new long[] {1L, 2L, 3L})
            .build();
    Mutation float32 =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .toFloat32Array(new float[] {1.0f, 2.0f})
            .build();
    Mutation float64 =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .toFloat64Array(new double[] {1., 2.})
            .build();
    Mutation bool =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .toBoolArray(new boolean[] {true, true, false, true})
            .build();
    Mutation numeric =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .toNumericArray(
                ImmutableList.of(
                    new BigDecimal("12345678901234567890.123456789"),
                    new BigDecimal("12345678901234567890123.1234567890123"),
                    new BigDecimal("123456789012345678901234.1234567890123456"),
                    new BigDecimal("1234567890123456789012345.1234567890123456789")))
            .build();
    Mutation pgNumeric =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .toPgNumericArray(
                ImmutableList.of(
                    "12345678901234567890.123456789",
                    "12345678901234567890123.1234567890123",
                    "123456789012345678901234.1234567890123456",
                    "1234567890123456789012345.1234567890123456789",
                    "NaN"))
            .build();

    Mutation json =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .toJsonArray(
                ImmutableList.of(
                    "{\"key1\":\"value1\", \"key2\":\"value2\"}",
                    "{\"key1\":\"value1\", \"key2\":20}"))
            .build();
    Mutation bytes =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("bytes")
            .toBytesArray(
                ImmutableList.of(
                    ByteArray.copyFrom("some_bytes".getBytes(UTF_8)),
                    ByteArray.copyFrom("some_bytes".getBytes(UTF_8))))
            .build();
    Mutation jsonb =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .toPgJsonbArray(
                ImmutableList.of(
                    "{\"key123\":\"value123\", \"key321\":\"value321\"}",
                    "{\"key456\":\"value456\", \"key789\":600}"))
            .build();
    Mutation protoEnum =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .toProtoEnumArray(ImmutableList.of(1L, 2L, 3L), "customer.app.TestEnum")
            .build();
    Mutation protos =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("bytes")
            .toProtoMessageArray(
                ImmutableList.of(
                    ByteArray.copyFrom("some_bytes".getBytes(UTF_8)),
                    ByteArray.copyFrom("some_bytes".getBytes(UTF_8))),
                "customer.app.TestMessage")
            .build();
    assertThat(MutationSizeEstimator.sizeOf(int64), is(24L));
    assertThat(MutationSizeEstimator.sizeOf(float32), is(8L));
    assertThat(MutationSizeEstimator.sizeOf(float64), is(16L));
    assertThat(MutationSizeEstimator.sizeOf(bool), is(4L));
    assertThat(MutationSizeEstimator.sizeOf(numeric), is(153L));
    assertThat(MutationSizeEstimator.sizeOf(pgNumeric), is(156L));
    assertThat(MutationSizeEstimator.sizeOf(json), is(62L));
    assertThat(MutationSizeEstimator.sizeOf(bytes), is(20L));
    assertThat(MutationSizeEstimator.sizeOf(jsonb), is(77L));
    assertThat(MutationSizeEstimator.sizeOf(protoEnum), is(24L));
    assertThat(MutationSizeEstimator.sizeOf(protos), is(20L));
  }

  @Test
  public void nullPrimitiveArrays() throws Exception {
    Mutation int64 =
        Mutation.newInsertOrUpdateBuilder("test").set("one").toInt64Array((long[]) null).build();
    Mutation protoEnum =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .toProtoEnumArray(null, "customer.app.TestEnum")
            .build();
    Mutation float32 =
        Mutation.newInsertOrUpdateBuilder("test").set("one").toFloat32Array((float[]) null).build();
    Mutation float64 =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .toFloat64Array((double[]) null)
            .build();
    Mutation bool =
        Mutation.newInsertOrUpdateBuilder("test").set("one").toBoolArray((boolean[]) null).build();
    Mutation numeric =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .toNumericArray((Iterable<BigDecimal>) null)
            .build();
    Mutation pgNumeric =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .toPgNumericArray((Iterable<String>) null)
            .build();
    Mutation json = Mutation.newInsertOrUpdateBuilder("test").set("one").toJsonArray(null).build();
    Mutation jsonb =
        Mutation.newInsertOrUpdateBuilder("test").set("one").toPgJsonbArray(null).build();

    assertThat(MutationSizeEstimator.sizeOf(int64), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(float32), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(float64), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(bool), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(numeric), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(pgNumeric), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(json), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(jsonb), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(protoEnum), is(0L));
  }

  @Test
  public void strings() throws Exception {
    Mutation emptyString = Mutation.newInsertOrUpdateBuilder("test").set("one").to("").build();
    Mutation nullString =
        Mutation.newInsertOrUpdateBuilder("test").set("one").to((String) null).build();
    Mutation sampleString = Mutation.newInsertOrUpdateBuilder("test").set("one").to("abc").build();
    Mutation sampleArray =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .toStringArray(Arrays.asList("one", "two", null))
            .build();
    Mutation nullArray =
        Mutation.newInsertOrUpdateBuilder("test").set("one").toStringArray(null).build();
    Mutation deleteString = Mutation.delete("test", Key.of("one", "two"));

    assertThat(MutationSizeEstimator.sizeOf(emptyString), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(nullString), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(sampleString), is(3L));
    assertThat(MutationSizeEstimator.sizeOf(sampleArray), is(6L));
    assertThat(MutationSizeEstimator.sizeOf(nullArray), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(deleteString), is(6L));
  }

  @Test
  public void bytes() throws Exception {
    Mutation empty =
        Mutation.newInsertOrUpdateBuilder("test").set("one").to(ByteArray.fromBase64("")).build();
    Mutation nullValue =
        Mutation.newInsertOrUpdateBuilder("test").set("one").to((ByteArray) null).build();
    Mutation sample =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .to(ByteArray.fromBase64("abcdabcd"))
            .build();
    Mutation nullArray =
        Mutation.newInsertOrUpdateBuilder("test").set("one").toBytesArray(null).build();
    Mutation deleteBytes =
        Mutation.delete("test", Key.of(ByteArray.copyFrom("some_bytes".getBytes(UTF_8))));

    assertThat(MutationSizeEstimator.sizeOf(empty), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(nullValue), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(sample), is(6L));
    assertThat(MutationSizeEstimator.sizeOf(nullArray), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(deleteBytes), is(10L));
  }

  @Test
  public void protos() throws Exception {
    Mutation empty =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .to(ByteArray.fromBase64(""), "customer.app.TestMessage")
            .build();
    Mutation nullValue =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .to((ByteArray) null, "customer.app.TestMessage")
            .build();
    Mutation sample =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .to(ByteArray.fromBase64("abcdabcd"), "customer.app.TestMessage")
            .build();
    Mutation nullArray =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .toProtoMessageArray(null, "customer.app.TestMessage")
            .build();
    Mutation deleteBytes =
        Mutation.delete("test", Key.of(ByteArray.copyFrom("some_bytes".getBytes(UTF_8))));

    assertThat(MutationSizeEstimator.sizeOf(empty), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(nullValue), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(sample), is(6L));
    assertThat(MutationSizeEstimator.sizeOf(nullArray), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(deleteBytes), is(10L));
  }

  @Test
  public void jsons() throws Exception {
    Mutation empty =
        Mutation.newInsertOrUpdateBuilder("test").set("one").to(Value.json("{}")).build();
    Mutation nullValue =
        Mutation.newInsertOrUpdateBuilder("test").set("one").to(Value.json((String) null)).build();
    Mutation sample =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .to(Value.json("{\"name\":\"number\",\"val\":12345.123}"))
            .build();
    Mutation nullArray =
        Mutation.newInsertOrUpdateBuilder("test").set("one").toJsonArray(null).build();

    assertThat(MutationSizeEstimator.sizeOf(empty), is(2L));
    assertThat(MutationSizeEstimator.sizeOf(nullValue), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(sample), is(33L));
    assertThat(MutationSizeEstimator.sizeOf(nullArray), is(0L));
  }

  @Test
  public void pgJsonb() throws Exception {
    Mutation empty =
        Mutation.newInsertOrUpdateBuilder("test").set("one").to(Value.pgJsonb("{}")).build();
    Mutation nullValue =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .to(Value.pgJsonb((String) null))
            .build();
    Mutation sample =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .to(Value.pgJsonb("{\"type_name\":\"number\",\"value\":12345.123}"))
            .build();
    Mutation nullArray =
        Mutation.newInsertOrUpdateBuilder("test").set("one").toPgJsonbArray(null).build();

    assertThat(MutationSizeEstimator.sizeOf(empty), is(2L));
    assertThat(MutationSizeEstimator.sizeOf(nullValue), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(sample), is(40L));
    assertThat(MutationSizeEstimator.sizeOf(nullArray), is(0L));
  }

  @Test
  public void dates() throws Exception {
    Mutation timestamp =
        Mutation.newInsertOrUpdateBuilder("test").set("one").to(Timestamp.now()).build();
    Mutation nullTimestamp =
        Mutation.newInsertOrUpdateBuilder("test").set("one").to((Timestamp) null).build();
    Mutation date =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .to(Date.fromYearMonthDay(2017, 10, 10))
            .build();
    Mutation nullDate =
        Mutation.newInsertOrUpdateBuilder("test").set("one").to((Date) null).build();
    Mutation timestampArray =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .toTimestampArray(Arrays.asList(Timestamp.now(), null))
            .build();
    Mutation dateArray =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .toDateArray(
                Arrays.asList(
                    null,
                    Date.fromYearMonthDay(2017, 1, 1),
                    null,
                    Date.fromYearMonthDay(2017, 1, 2)))
            .build();

    Mutation nullTimestampArray =
        Mutation.newInsertOrUpdateBuilder("test").set("one").toTimestampArray(null).build();
    Mutation nullDateArray =
        Mutation.newInsertOrUpdateBuilder("test").set("one").toDateArray(null).build();
    Mutation deleteTimestamp =
        Mutation.delete("test", Key.of(Timestamp.parseTimestamp("2077-10-15T00:00:00Z")));
    Mutation deleteDate = Mutation.delete("test", Key.of(Date.fromYearMonthDay(2017, 1, 1)));

    assertThat(MutationSizeEstimator.sizeOf(timestamp), is(12L));
    assertThat(MutationSizeEstimator.sizeOf(date), is(12L));
    assertThat(MutationSizeEstimator.sizeOf(nullTimestamp), is(12L));
    assertThat(MutationSizeEstimator.sizeOf(nullDate), is(12L));
    assertThat(MutationSizeEstimator.sizeOf(timestampArray), is(24L));
    assertThat(MutationSizeEstimator.sizeOf(dateArray), is(48L));
    assertThat(MutationSizeEstimator.sizeOf(nullTimestampArray), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(nullDateArray), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(deleteTimestamp), is(12L));
    assertThat(MutationSizeEstimator.sizeOf(deleteDate), is(12L));
  }

  @Test
  public void group() throws Exception {
    Mutation int64 = Mutation.newInsertOrUpdateBuilder("test").set("one").to(1).build();
    Mutation float32 = Mutation.newInsertOrUpdateBuilder("test").set("one").to(1.3f).build();
    Mutation float64 = Mutation.newInsertOrUpdateBuilder("test").set("one").to(2.9).build();
    Mutation bool = Mutation.newInsertOrUpdateBuilder("test").set("one").to(false).build();

    MutationGroup group = MutationGroup.create(int64, float32, float64, bool);

    assertThat(MutationSizeEstimator.sizeOf(group), is(21L));
  }

  @Test
  public void deleteKeyRanges() throws Exception {
    Mutation range =
        Mutation.delete("test", KeySet.range(KeyRange.openOpen(Key.of(1L), Key.of(4L))));
    assertThat(MutationSizeEstimator.sizeOf(range), is(16L));
  }

  @Test(expected = IllegalArgumentException.class)
  public void unsupportedArrayType() {
    Mutation unsupportedArray =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .toStructArray(
                Type.struct(Type.StructField.of("int64", Type.int64())), ImmutableList.of())
            .build();
    MutationSizeEstimator.sizeOf(unsupportedArray);
  }

  @Test(expected = IllegalArgumentException.class)
  public void unsupportedStructType() {
    Mutation struct =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .to(Struct.newBuilder().build())
            .build();
    MutationSizeEstimator.sizeOf(struct);
  }
}
