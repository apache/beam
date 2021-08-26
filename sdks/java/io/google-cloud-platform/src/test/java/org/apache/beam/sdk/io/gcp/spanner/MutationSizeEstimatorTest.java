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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.google.cloud.ByteArray;
import com.google.cloud.Date;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** A set of unit tests for {@link MutationSizeEstimator}. */
@RunWith(JUnit4.class)
public class MutationSizeEstimatorTest {

  @Test
  public void primitives() throws Exception {
    Mutation int64 = Mutation.newInsertOrUpdateBuilder("test").set("one").to(1).build();
    Mutation float64 = Mutation.newInsertOrUpdateBuilder("test").set("one").to(2.9).build();
    Mutation bool = Mutation.newInsertOrUpdateBuilder("test").set("one").to(false).build();
    Mutation numeric =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .to(new BigDecimal("12345678901234567890.123456789"))
            .build();

    assertThat(MutationSizeEstimator.sizeOf(int64), is(8L));
    assertThat(MutationSizeEstimator.sizeOf(float64), is(8L));
    assertThat(MutationSizeEstimator.sizeOf(bool), is(1L));
    assertThat(MutationSizeEstimator.sizeOf(numeric), is(30L));
  }

  @Test
  public void primitiveArrays() throws Exception {
    Mutation int64 =
        Mutation.newInsertOrUpdateBuilder("test")
            .set("one")
            .toInt64Array(new long[] {1L, 2L, 3L})
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

    assertThat(MutationSizeEstimator.sizeOf(int64), is(24L));
    assertThat(MutationSizeEstimator.sizeOf(float64), is(16L));
    assertThat(MutationSizeEstimator.sizeOf(bool), is(4L));
    assertThat(MutationSizeEstimator.sizeOf(numeric), is(153L));
  }

  @Test
  public void nullPrimitiveArrays() throws Exception {
    Mutation int64 =
        Mutation.newInsertOrUpdateBuilder("test").set("one").toInt64Array((long[]) null).build();
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

    assertThat(MutationSizeEstimator.sizeOf(int64), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(float64), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(bool), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(numeric), is(0L));
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

    assertThat(MutationSizeEstimator.sizeOf(emptyString), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(nullString), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(sampleString), is(3L));
    assertThat(MutationSizeEstimator.sizeOf(sampleArray), is(6L));
    assertThat(MutationSizeEstimator.sizeOf(nullArray), is(0L));
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

    assertThat(MutationSizeEstimator.sizeOf(empty), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(nullValue), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(sample), is(6L));
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

    assertThat(MutationSizeEstimator.sizeOf(timestamp), is(12L));
    assertThat(MutationSizeEstimator.sizeOf(date), is(12L));
    assertThat(MutationSizeEstimator.sizeOf(nullTimestamp), is(12L));
    assertThat(MutationSizeEstimator.sizeOf(nullDate), is(12L));
    assertThat(MutationSizeEstimator.sizeOf(timestampArray), is(24L));
    assertThat(MutationSizeEstimator.sizeOf(dateArray), is(48L));
    assertThat(MutationSizeEstimator.sizeOf(nullTimestampArray), is(0L));
    assertThat(MutationSizeEstimator.sizeOf(nullDateArray), is(0L));
  }

  @Test
  public void group() throws Exception {
    Mutation int64 = Mutation.newInsertOrUpdateBuilder("test").set("one").to(1).build();
    Mutation float64 = Mutation.newInsertOrUpdateBuilder("test").set("one").to(2.9).build();
    Mutation bool = Mutation.newInsertOrUpdateBuilder("test").set("one").to(false).build();

    MutationGroup group = MutationGroup.create(int64, float64, bool);

    assertThat(MutationSizeEstimator.sizeOf(group), is(17L));
  }
}
