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
package org.apache.beam.sdk.extensions.zetasketch;

import com.google.zetasketch.HyperLogLogPlusPlus;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/** Tests for {@link ApproximateCountDistinct}. */
public class ApproximateCountDistinctTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  // Integer
  private static final List<Integer> INTS1 = Arrays.asList(1, 2, 3, 3, 1, 4);
  private static final Long INTS1_ESTIMATE;

  private static final int TEST_PRECISION = 20;

  static {
    HyperLogLogPlusPlus<Integer> hll = new HyperLogLogPlusPlus.Builder().buildForIntegers();
    INTS1.forEach(hll::add);
    INTS1_ESTIMATE = hll.longResult();
  }

  /** Test correct Builder is returned from Generic type. * */
  @Test
  public void testIntegerBuilder() {

    PCollection<Integer> ints = p.apply(Create.of(1));
    HllCount.Init.Builder<Integer> builder =
        ApproximateCountDistinct.<Integer>builderForType(
            ints.getCoder().getEncodedTypeDescriptor());
    PCollection<Long> result = ints.apply(builder.globally()).apply(HllCount.Extract.globally());
    PAssert.that(result).containsInAnyOrder(1L);
    p.run();
  }
  /** Test correct Builder is returned from Generic type. * */
  @Test
  public void testStringBuilder() {

    PCollection<String> strings = p.apply(Create.<String>of("43"));
    HllCount.Init.Builder<String> builder =
        ApproximateCountDistinct.<String>builderForType(
            strings.getCoder().getEncodedTypeDescriptor());
    PCollection<Long> result = strings.apply(builder.globally()).apply(HllCount.Extract.globally());
    PAssert.that(result).containsInAnyOrder(1L);
    p.run();
  }
  /** Test correct Builder is returned from Generic type. * */
  @Test
  public void testLongBuilder() {

    PCollection<Long> longs = p.apply(Create.<Long>of(1L));
    HllCount.Init.Builder<Long> builder =
        ApproximateCountDistinct.<Long>builderForType(longs.getCoder().getEncodedTypeDescriptor());
    PCollection<Long> result = longs.apply(builder.globally()).apply(HllCount.Extract.globally());
    PAssert.that(result).containsInAnyOrder(1L);
    p.run();
  }
  /** Test correct Builder is returned from Generic type. * */
  @Test
  public void testBytesBuilder() {

    byte[] byteArray = new byte[] {'A'};
    PCollection<byte[]> bytes = p.apply(Create.of(byteArray));
    HllCount.Init.Builder<byte[]> builder =
        ApproximateCountDistinct.<byte[]>builderForType(
            bytes.getCoder().getEncodedTypeDescriptor());
    PCollection<Long> result = bytes.apply(builder.globally()).apply(HllCount.Extract.globally());
    PAssert.that(result).containsInAnyOrder(1L);
    p.run();
  }

  /** Test Integer Globally. */
  @Test
  @Category(NeedsRunner.class)
  public void testStandardTypesGlobalForInteger() {
    PCollection<Long> approxResultInteger =
        p.apply("Int", Create.of(INTS1)).apply("IntHLL", ApproximateCountDistinct.globally());
    PAssert.thatSingleton(approxResultInteger).isEqualTo(INTS1_ESTIMATE);
    p.run();
  }

  /** Test Long Globally. */
  @Test
  @Category(NeedsRunner.class)
  public void testStandardTypesGlobalForLong() {

    PCollection<Long> approxResultLong =
        p.apply("Long", Create.of(INTS1.stream().map(Long::valueOf).collect(Collectors.toList())))
            .apply("LongHLL", ApproximateCountDistinct.globally());

    PAssert.thatSingleton(approxResultLong).isEqualTo(INTS1_ESTIMATE);

    p.run();
  }

  /** Test String Globally. */
  @Test
  @Category(NeedsRunner.class)
  public void testStandardTypesGlobalForStrings() {
    PCollection<Long> approxResultString =
        p.apply("Str", Create.of(INTS1.stream().map(String::valueOf).collect(Collectors.toList())))
            .apply("StrHLL", ApproximateCountDistinct.globally());

    PAssert.thatSingleton(approxResultString).isEqualTo(INTS1_ESTIMATE);

    p.run();
  }

  /** Test Byte Globally. */
  @Test
  @Category(NeedsRunner.class)
  public void testStandardTypesGlobalForBytes() {
    PCollection<Long> approxResultByte =
        p.apply(
                "BytesHLL",
                Create.of(
                    INTS1.stream()
                        .map(x -> ByteBuffer.allocate(4).putInt(x).array())
                        .collect(Collectors.toList())))
            .apply(ApproximateCountDistinct.globally());

    PAssert.thatSingleton(approxResultByte).isEqualTo(INTS1_ESTIMATE);

    p.run();
  }

  /** Test Integer Globally. */
  @Test
  @Category(NeedsRunner.class)
  public void testStandardTypesPerKeyForInteger() {

    List<KV<Integer, Integer>> ints = new ArrayList<>();

    for (int i = 0; i < 3; i++) {
      for (int k : INTS1) {
        ints.add(KV.of(i, k));
      }
    }

    PCollection<KV<Integer, Long>> result =
        p.apply("Int", Create.of(ints)).apply("IntHLL", ApproximateCountDistinct.perKey());

    PAssert.that(result)
        .containsInAnyOrder(
            ImmutableList.of(
                KV.of(0, INTS1_ESTIMATE), KV.of(1, INTS1_ESTIMATE), KV.of(2, INTS1_ESTIMATE)));

    p.run();
  }

  /** Test Long Globally. */
  @Test
  @Category(NeedsRunner.class)
  public void testStandardTypesPerKeyForLong() {

    List<KV<Integer, Long>> longs = new ArrayList<>();

    for (int i = 0; i < 3; i++) {
      for (int k : INTS1) {
        longs.add(KV.of(i, (long) k));
      }
    }

    PCollection<KV<Integer, Long>> result =
        p.apply("Long", Create.of(longs)).apply("LongHLL", ApproximateCountDistinct.perKey());

    PAssert.that(result)
        .containsInAnyOrder(
            ImmutableList.of(
                KV.of(0, INTS1_ESTIMATE), KV.of(1, INTS1_ESTIMATE), KV.of(2, INTS1_ESTIMATE)));

    p.run();
  }

  /** Test String Globally. */
  @Test
  @Category(NeedsRunner.class)
  public void testStandardTypesPerKeyForStrings() {
    List<KV<Integer, String>> strings = new ArrayList<>();

    for (int i = 0; i < 3; i++) {
      for (int k : INTS1) {
        strings.add(KV.of(i, String.valueOf(k)));
      }
    }

    PCollection<KV<Integer, Long>> result =
        p.apply("Str", Create.of(strings)).apply("StrHLL", ApproximateCountDistinct.perKey());

    PAssert.that(result)
        .containsInAnyOrder(
            ImmutableList.of(
                KV.of(0, INTS1_ESTIMATE), KV.of(1, INTS1_ESTIMATE), KV.of(2, INTS1_ESTIMATE)));

    p.run();
  }

  /** Test Byte Globally. */
  @Test
  @Category(NeedsRunner.class)
  public void testStandardTypesPerKeyForBytes() {

    List<KV<Integer, byte[]>> bytes = new ArrayList<>();

    for (int i = 0; i < 3; i++) {
      for (int k : INTS1) {
        bytes.add(KV.of(i, ByteBuffer.allocate(4).putInt(k).array()));
      }
    }

    PCollection<KV<Integer, Long>> result =
        p.apply("BytesHLL", Create.of(bytes)).apply(ApproximateCountDistinct.perKey());

    PAssert.that(result)
        .containsInAnyOrder(
            ImmutableList.of(
                KV.of(0, INTS1_ESTIMATE), KV.of(1, INTS1_ESTIMATE), KV.of(2, INTS1_ESTIMATE)));

    p.run();
  }

  /** Test a general object, we will make use of a KV as the object as it already has a coder. */
  @Test
  @Category(NeedsRunner.class)
  public void testObjectTypesGlobal() {

    PCollection<Long> approxResultInteger =
        p.apply(
                "Int",
                Create.of(
                    INTS1.stream().map(x -> KV.of(x, KV.of(x, x))).collect(Collectors.toList())))
            .apply(
                "IntHLL",
                ApproximateCountDistinct.<KV<Integer, KV<Integer, Integer>>>globally()
                    .via((KV<Integer, KV<Integer, Integer>> x) -> (long) x.getValue().hashCode()));

    PAssert.thatSingleton(approxResultInteger).isEqualTo(INTS1_ESTIMATE);

    p.run();
  }

  /** Test a general object, we will make use of a KV as the object as it already has a coder. */
  @Test
  @Category(NeedsRunner.class)
  public void testObjectTypesPerKey() {

    List<KV<Integer, KV<Integer, Integer>>> ints = new ArrayList<>();

    for (int i = 0; i < 3; i++) {
      for (int k : INTS1) {
        ints.add(KV.of(i, KV.of(i, k)));
      }
    }

    PCollection<KV<Integer, Long>> approxResultInteger =
        p.apply("Int", Create.of(ints))
            .apply(
                "IntHLL",
                ApproximateCountDistinct.<Integer, KV<Integer, Integer>>perKey()
                    .via(x -> KV.of(x.getKey(), (long) x.hashCode()))
                    .withPercision(TEST_PRECISION));

    PAssert.that(approxResultInteger)
        .containsInAnyOrder(
            ImmutableList.of(
                KV.of(0, INTS1_ESTIMATE), KV.of(1, INTS1_ESTIMATE), KV.of(2, INTS1_ESTIMATE)));

    p.run();
  }

  /** Test a general object, we will make use of a KV as the object as it already has a coder. */
  @Test
  @Category(NeedsRunner.class)
  public void testGlobalPercision() {

    PCollection<Long> approxResultInteger =
        p.apply("Int", Create.of(INTS1))
            .apply("IntHLL", ApproximateCountDistinct.globally().withPercision(TEST_PRECISION));

    PAssert.thatSingleton(approxResultInteger).isEqualTo(INTS1_ESTIMATE);

    p.run();
  }

  /** Test a general object, we will make use of a KV as the object as it already has a coder. */
  @Test
  @Category(NeedsRunner.class)
  public void testPerKeyPercision() {

    List<KV<Integer, Integer>> ints = new ArrayList<>();

    for (int i = 0; i < 3; i++) {
      for (int k : INTS1) {
        ints.add(KV.of(i, k));
      }
    }

    PCollection<KV<Integer, Long>> approxResultInteger =
        p.apply("Int", Create.of(ints))
            .apply("IntHLL", ApproximateCountDistinct.perKey().withPercision(TEST_PRECISION));

    PAssert.that(approxResultInteger)
        .containsInAnyOrder(
            ImmutableList.of(
                KV.of(0, INTS1_ESTIMATE), KV.of(1, INTS1_ESTIMATE), KV.of(2, INTS1_ESTIMATE)));

    p.run();
  }
}
