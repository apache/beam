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

import static org.junit.Assert.assertArrayEquals;

import com.google.zetasketch.HyperLogLogPlusPlus;
import com.google.zetasketch.shaded.com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link HllCount}. */
@RunWith(JUnit4.class)
public class HllCountTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();
  @Rule public transient ExpectedException thrown = ExpectedException.none();

  private static final byte[] EMPTY_SKETCH = new byte[0];

  // Integer
  private static final List<Integer> INTS1 = Arrays.asList(1, 2, 3, 3, 1, 4);
  private static final byte[] INTS1_SKETCH;
  private static final Long INTS1_ESTIMATE;

  static {
    HyperLogLogPlusPlus<Integer> hll = new HyperLogLogPlusPlus.Builder().buildForIntegers();
    INTS1.forEach(hll::add);
    INTS1_SKETCH = hll.serializeToByteArray();
    INTS1_ESTIMATE = hll.longResult();
  }

  private static final List<Integer> INTS2 = Arrays.asList(3, 3, 3, 3);
  private static final byte[] INTS2_SKETCH;
  private static final Long INTS2_ESTIMATE;

  static {
    HyperLogLogPlusPlus<Integer> hll = new HyperLogLogPlusPlus.Builder().buildForIntegers();
    INTS2.forEach(hll::add);
    INTS2_SKETCH = hll.serializeToByteArray();
    INTS2_ESTIMATE = hll.longResult();
  }

  private static final byte[] INTS1_INTS2_SKETCH;

  static {
    HyperLogLogPlusPlus<?> hll = HyperLogLogPlusPlus.forProto(INTS1_SKETCH);
    hll.merge(INTS2_SKETCH);
    INTS1_INTS2_SKETCH = hll.serializeToByteArray();
  }

  // Long
  private static final List<Long> LONGS = Collections.singletonList(1L);
  private static final byte[] LONGS_SKETCH;

  static {
    HyperLogLogPlusPlus<Long> hll = new HyperLogLogPlusPlus.Builder().buildForLongs();
    LONGS.forEach(hll::add);
    LONGS_SKETCH = hll.serializeToByteArray();
  }

  private static final byte[] LONGS_SKETCH_OF_EMPTY_SET;

  static {
    HyperLogLogPlusPlus<Long> hll = new HyperLogLogPlusPlus.Builder().buildForLongs();
    LONGS_SKETCH_OF_EMPTY_SET = hll.serializeToByteArray();
  }

  // String
  private static final List<String> STRINGS = Arrays.asList("s1", "s2", "s1", "s2");
  private static final byte[] STRINGS_SKETCH;

  static {
    HyperLogLogPlusPlus<String> hll = new HyperLogLogPlusPlus.Builder().buildForStrings();
    STRINGS.forEach(hll::add);
    STRINGS_SKETCH = hll.serializeToByteArray();
  }

  private static final int TEST_PRECISION = 20;
  private static final byte[] STRINGS_SKETCH_TEST_PRECISION;

  static {
    HyperLogLogPlusPlus<String> hll =
        new HyperLogLogPlusPlus.Builder().normalPrecision(TEST_PRECISION).buildForStrings();
    STRINGS.forEach(hll::add);
    STRINGS_SKETCH_TEST_PRECISION = hll.serializeToByteArray();
  }

  // Bytes
  private static final byte[] BYTES0 = {(byte) 0x1, (byte) 0xa};
  private static final byte[] BYTES1 = {(byte) 0xf};
  private static final List<byte[]> BYTES = Arrays.asList(BYTES0, BYTES1);
  private static final byte[] BYTES_SKETCH;

  static {
    HyperLogLogPlusPlus<ByteString> hll = new HyperLogLogPlusPlus.Builder().buildForBytes();
    BYTES.forEach(hll::add);
    BYTES_SKETCH = hll.serializeToByteArray();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testInitForIntegersGlobally() {
    PCollection<byte[]> result =
        p.apply(Create.of(INTS1)).apply(HllCount.Init.forIntegers().globally());

    PAssert.thatSingleton(result).isEqualTo(INTS1_SKETCH);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testInitForLongsGlobally() {
    PCollection<byte[]> result =
        p.apply(Create.of(LONGS)).apply(HllCount.Init.forLongs().globally());

    PAssert.thatSingleton(result).isEqualTo(LONGS_SKETCH);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testInitForLongsGloballyForEmptyInput() {
    PCollection<byte[]> result =
        p.apply(Create.empty(TypeDescriptor.of(Long.class)))
            .apply(HllCount.Init.forLongs().globally());

    PAssert.thatSingleton(result).isEqualTo(EMPTY_SKETCH);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testInitForStringsGlobally() {
    PCollection<byte[]> result =
        p.apply(Create.of(STRINGS)).apply(HllCount.Init.forStrings().globally());

    PAssert.thatSingleton(result).isEqualTo(STRINGS_SKETCH);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testInitForStringsGloballyWithPrecision() {
    PCollection<byte[]> result =
        p.apply(Create.of(STRINGS))
            .apply(HllCount.Init.forStrings().withPrecision(TEST_PRECISION).globally());

    PAssert.thatSingleton(result).isEqualTo(STRINGS_SKETCH_TEST_PRECISION);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testInitForStringsGloballyWithInvalidPrecision() {
    thrown.expect(IllegalArgumentException.class);
    p.apply(Create.of(STRINGS)).apply(HllCount.Init.forStrings().withPrecision(0).globally());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testInitForBytesGlobally() {
    PCollection<byte[]> result =
        p.apply(Create.of(BYTES)).apply(HllCount.Init.forBytes().globally());

    PAssert.thatSingleton(result).isEqualTo(BYTES_SKETCH);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testInitForIntegersPerKey() {
    List<KV<String, Integer>> input = new ArrayList<>();
    INTS1.forEach(i -> input.add(KV.of("k1", i)));
    INTS1.forEach(i -> input.add(KV.of("k2", i)));
    INTS2.forEach(i -> input.add(KV.of("k1", i)));
    PCollection<KV<String, byte[]>> result =
        p.apply(Create.of(input)).apply(HllCount.Init.forIntegers().perKey());

    PAssert.that(result)
        .containsInAnyOrder(
            Arrays.asList(KV.of("k1", INTS1_INTS2_SKETCH), KV.of("k2", INTS1_SKETCH)));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testInitForLongsPerKey() {
    List<KV<String, Long>> input = new ArrayList<>();
    LONGS.forEach(l -> input.add(KV.of("k", l)));
    PCollection<KV<String, byte[]>> result =
        p.apply(Create.of(input)).apply(HllCount.Init.forLongs().perKey());

    PAssert.that(result).containsInAnyOrder(Collections.singletonList(KV.of("k", LONGS_SKETCH)));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testInitForStringsPerKey() {
    List<KV<String, String>> input = new ArrayList<>();
    STRINGS.forEach(s -> input.add(KV.of("k", s)));
    PCollection<KV<String, byte[]>> result =
        p.apply(Create.of(input)).apply(HllCount.Init.forStrings().perKey());

    PAssert.that(result).containsInAnyOrder(Collections.singletonList(KV.of("k", STRINGS_SKETCH)));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testInitForStringsPerKeyWithPrecision() {
    List<KV<String, String>> input = new ArrayList<>();
    STRINGS.forEach(s -> input.add(KV.of("k", s)));
    PCollection<KV<String, byte[]>> result =
        p.apply(Create.of(input))
            .apply(HllCount.Init.forStrings().withPrecision(TEST_PRECISION).perKey());

    PAssert.that(result)
        .containsInAnyOrder(Collections.singletonList(KV.of("k", STRINGS_SKETCH_TEST_PRECISION)));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testInitForStringsPerKeyWithInvalidPrecision() {
    List<KV<String, String>> input = new ArrayList<>();
    STRINGS.forEach(s -> input.add(KV.of("k", s)));
    thrown.expect(IllegalArgumentException.class);
    p.apply(Create.of(input)).apply(HllCount.Init.forStrings().withPrecision(0).perKey());
  }

  @Test
  @Category(NeedsRunner.class)
  public void testInitForBytesPerKey() {
    List<KV<String, byte[]>> input = new ArrayList<>();
    BYTES.forEach(bs -> input.add(KV.of("k", bs)));
    PCollection<KV<String, byte[]>> result =
        p.apply(Create.of(input)).apply(HllCount.Init.forBytes().perKey());

    PAssert.that(result).containsInAnyOrder(Collections.singletonList(KV.of("k", BYTES_SKETCH)));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMergePartialGlobally() {
    PCollection<byte[]> result =
        p.apply(Create.of(INTS1_SKETCH, INTS2_SKETCH)).apply(HllCount.MergePartial.globally());

    PAssert.thatSingleton(result).isEqualTo(INTS1_INTS2_SKETCH);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMergePartialGloballyForEmptyInput() {
    PCollection<byte[]> result =
        p.apply(Create.empty(TypeDescriptor.of(byte[].class)))
            .apply(HllCount.MergePartial.globally());

    PAssert.thatSingleton(result).isEqualTo(EMPTY_SKETCH);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMergePartialGloballyForSingletonInput() {
    PCollection<byte[]> result =
        p.apply(Create.of(LONGS_SKETCH)).apply(HllCount.MergePartial.globally());

    PAssert.thatSingleton(result).isEqualTo(LONGS_SKETCH);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMergePartialGloballyForSingletonInputEmptySketch() {
    PCollection<byte[]> result =
        p.apply(Create.of(EMPTY_SKETCH)).apply(HllCount.MergePartial.globally());

    PAssert.thatSingleton(result).isEqualTo(EMPTY_SKETCH);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMergePartialGloballyForMergeWithEmptySketch() {
    PCollection<byte[]> result =
        p.apply(Create.of(LONGS_SKETCH, EMPTY_SKETCH)).apply(HllCount.MergePartial.globally());

    PAssert.thatSingleton(result).isEqualTo(LONGS_SKETCH);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMergePartialGloballyForMergeMultipleEmptySketches() {
    PCollection<byte[]> result =
        p.apply(Create.of(EMPTY_SKETCH, EMPTY_SKETCH)).apply(HllCount.MergePartial.globally());

    PAssert.thatSingleton(result).isEqualTo(EMPTY_SKETCH);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMergePartialGloballyForMergeWithSketchOfEmptySet() {
    PCollection<byte[]> result =
        p.apply(Create.of(LONGS_SKETCH, LONGS_SKETCH_OF_EMPTY_SET))
            .apply(HllCount.MergePartial.globally());

    PAssert.thatSingleton(result).isEqualTo(LONGS_SKETCH);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMergePartialGloballyForMergeEmptySketchWithSketchOfEmptySet() {
    PCollection<byte[]> result =
        p.apply(Create.of(EMPTY_SKETCH, LONGS_SKETCH_OF_EMPTY_SET))
            .apply(HllCount.MergePartial.globally());

    PAssert.thatSingleton(result).isEqualTo(LONGS_SKETCH_OF_EMPTY_SET);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMergePartialGloballyForIncompatibleSketches() {
    p.apply(Create.of(INTS1_SKETCH, STRINGS_SKETCH)).apply(HllCount.MergePartial.globally());

    thrown.expect(PipelineExecutionException.class);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMergePartialPerKey() {
    PCollection<KV<String, byte[]>> result =
        p.apply(
                Create.of(
                    KV.of("k1", INTS1_SKETCH),
                    KV.of("k2", STRINGS_SKETCH),
                    KV.of("k1", INTS2_SKETCH)))
            .apply(HllCount.MergePartial.perKey());

    PAssert.that(result)
        .containsInAnyOrder(
            Arrays.asList(KV.of("k1", INTS1_INTS2_SKETCH), KV.of("k2", STRINGS_SKETCH)));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMergePartialPerKeyForMergeWithEmptySketch() {
    PCollection<KV<String, byte[]>> result =
        p.apply(
                Create.of(
                    KV.of("k1", INTS1_SKETCH),
                    KV.of("k2", EMPTY_SKETCH),
                    KV.of("k1", EMPTY_SKETCH)))
            .apply(HllCount.MergePartial.perKey());

    PAssert.that(result)
        .containsInAnyOrder(Arrays.asList(KV.of("k1", INTS1_SKETCH), KV.of("k2", EMPTY_SKETCH)));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMergePartialPerKeyForMergeMultipleEmptySketches() {
    PCollection<KV<String, byte[]>> result =
        p.apply(
                Create.of(
                    KV.of("k1", EMPTY_SKETCH),
                    KV.of("k2", STRINGS_SKETCH),
                    KV.of("k1", EMPTY_SKETCH)))
            .apply(HllCount.MergePartial.perKey());

    PAssert.that(result)
        .containsInAnyOrder(Arrays.asList(KV.of("k1", EMPTY_SKETCH), KV.of("k2", STRINGS_SKETCH)));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testMergePartialPerKeyForIncompatibleSketches() {
    p.apply(
            Create.of(
                KV.of("k1", LONGS_SKETCH), KV.of("k2", STRINGS_SKETCH), KV.of("k1", BYTES_SKETCH)))
        .apply(HllCount.MergePartial.perKey());

    thrown.expect(PipelineExecutionException.class);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testExtractGlobally() {
    PCollection<Long> result =
        p.apply(Create.of(INTS1_SKETCH, INTS2_SKETCH)).apply(HllCount.Extract.globally());

    PAssert.that(result).containsInAnyOrder(INTS1_ESTIMATE, INTS2_ESTIMATE);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testExtractGloballyForEmptySketch() {
    PCollection<Long> result = p.apply(Create.of(EMPTY_SKETCH)).apply(HllCount.Extract.globally());

    PAssert.thatSingleton(result).isEqualTo(0L);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testExtractGloballyForSketchOfEmptySet() {
    PCollection<Long> result =
        p.apply(Create.of(LONGS_SKETCH_OF_EMPTY_SET)).apply(HllCount.Extract.globally());

    PAssert.thatSingleton(result).isEqualTo(0L);
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testExtractPerKey() {
    PCollection<KV<String, Long>> result =
        p.apply(Create.of(KV.of("k", INTS1_SKETCH), KV.of("k", INTS2_SKETCH)))
            .apply(HllCount.Extract.perKey());

    PAssert.that(result)
        .containsInAnyOrder(Arrays.asList(KV.of("k", INTS1_ESTIMATE), KV.of("k", INTS2_ESTIMATE)));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testExtractPerKeyForEmptySketch() {
    PCollection<KV<String, Long>> result =
        p.apply(Create.of(KV.of("k", EMPTY_SKETCH))).apply(HllCount.Extract.perKey());

    PAssert.thatSingleton(result).isEqualTo(KV.of("k", 0L));
    p.run();
  }

  @Test
  @Category(NeedsRunner.class)
  public void testExtractPerKeyForSketchOfEmptySet() {
    PCollection<KV<String, Long>> result =
        p.apply(Create.of(KV.of("k", LONGS_SKETCH_OF_EMPTY_SET))).apply(HllCount.Extract.perKey());

    PAssert.thatSingleton(result).isEqualTo(KV.of("k", 0L));
    p.run();
  }

  @Test
  public void testGetSketchFromByteBufferForEmptySketch() {
    assertArrayEquals(HllCount.getSketchFromByteBuffer(null), EMPTY_SKETCH);
  }

  @Test
  public void testGetSketchFromByteBufferForNonEmptySketch() {
    ByteBuffer bf = ByteBuffer.wrap(INTS1_SKETCH);
    assertArrayEquals(HllCount.getSketchFromByteBuffer(bf), INTS1_SKETCH);
  }
}
