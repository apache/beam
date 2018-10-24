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
package org.apache.beam.runners.dataflow.worker.util;

import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import org.apache.beam.runners.dataflow.worker.util.ScalableBloomFilter.Builder;
import org.apache.beam.runners.dataflow.worker.util.ScalableBloomFilter.ScalableBloomFilterCoder;
import org.apache.beam.sdk.testing.CoderProperties;
import org.apache.beam.sdk.util.CoderUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ScalableBloomFilter}. */
@RunWith(JUnit4.class)
public class ScalableBloomFilterTest {
  private static final int MAX_SIZE = 10_000;
  private static final ByteBuffer BUFFER = ByteBuffer.wrap(new byte[] {0x01, 0x02});

  @Test
  public void testBuilder() throws Exception {
    Builder builder = ScalableBloomFilter.withMaximumSizeBytes(MAX_SIZE);
    assertTrue("Expected Bloom filter to have been modified.", builder.put(BUFFER));

    // Re-adding should skip and not record the insertion.
    assertFalse("Expected Bloom filter to not have been modified.", builder.put(BUFFER));

    // Verify insertion
    int maxValue = insertAndVerifyContents(builder, 31);

    // Verify that the decoded value contains all the values and that it is much smaller
    // than the maximum size.
    verifyCoder(builder.build(), maxValue, MAX_SIZE / 50);
  }

  @Test
  public void testBuilderWithMaxSize() throws Exception {
    Builder builder = ScalableBloomFilter.withMaximumSizeBytes(MAX_SIZE);

    int maxValue = insertAndVerifyContents(builder, (int) (MAX_SIZE * 1.1));

    ScalableBloomFilter bloomFilter = builder.build();

    // Verify that the decoded value contains all the values and that it is much smaller
    // than the maximum size.
    verifyCoder(bloomFilter, maxValue, MAX_SIZE);
  }

  @Test
  public void testScalableBloomFilterCoder() throws Exception {
    Builder builderA = ScalableBloomFilter.withMaximumNumberOfInsertionsForOptimalBloomFilter(16);
    builderA.put(BUFFER);
    ScalableBloomFilter filterA = builderA.build();
    Builder builderB = ScalableBloomFilter.withMaximumNumberOfInsertionsForOptimalBloomFilter(16);
    builderB.put(BUFFER);
    ScalableBloomFilter filterB = builderB.build();

    CoderProperties.coderDecodeEncodeEqual(ScalableBloomFilterCoder.of(), filterA);
    CoderProperties.coderDeterministic(ScalableBloomFilterCoder.of(), filterA, filterB);
    CoderProperties.coderConsistentWithEquals(ScalableBloomFilterCoder.of(), filterA, filterB);
    CoderProperties.coderSerializable(ScalableBloomFilterCoder.of());
    CoderProperties.structuralValueConsistentWithEquals(
        ScalableBloomFilterCoder.of(), filterA, filterB);
  }

  /**
   * Inserts elements {@code 0, 1, ...} until the internal bloom filters have been modified {@code
   * maxNumberOfInsertions} times. Returns the largest value inserted.
   */
  private int insertAndVerifyContents(Builder builder, int maxNumberOfInsertions) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(4);
    int value = -1;
    while (maxNumberOfInsertions > 0) {
      value += 1;
      byteBuffer.clear();
      byteBuffer.putInt(value);
      byteBuffer.rewind();
      if (builder.put(byteBuffer)) {
        maxNumberOfInsertions -= 1;
      }
    }

    verifyContents(builder.build(), value);
    return value;
  }

  /** Verifies that the bloom filter contains all the values from {@code [0, 1, ..., maxValue]}. */
  private void verifyContents(ScalableBloomFilter bloomFilter, int maxValue) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(4);
    // Verify that all the values exist
    for (int i = 0; i <= maxValue; ++i) {
      byteBuffer.clear();
      byteBuffer.putInt(i);
      byteBuffer.rewind();
      assertTrue(bloomFilter.mightContain(byteBuffer));
    }
  }

  /**
   * Verifies that the coder correctly encodes and decodes and that all the values {@code [0, 1, 2,
   * ..., maxValue]} are contained within the decoded bloom filter. Also validates that the Bloom
   * filter is not larger than the specified maximum size.
   */
  private void verifyCoder(ScalableBloomFilter bloomFilter, int maxValue, int maxSizeBytes)
      throws Exception {
    byte[] encodedValue = CoderUtils.encodeToByteArray(ScalableBloomFilterCoder.of(), bloomFilter);
    ScalableBloomFilter decoded =
        CoderUtils.decodeFromByteArray(ScalableBloomFilterCoder.of(), encodedValue);
    assertThat("Bloom filter size", encodedValue.length, lessThan(maxSizeBytes));

    verifyContents(decoded, maxValue);
  }
}
