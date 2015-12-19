/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.dataflow.sdk.testing.CoderProperties;
import com.google.cloud.dataflow.sdk.util.ScalableBloomFilter.Builder;
import com.google.cloud.dataflow.sdk.util.ScalableBloomFilter.ScalableBloomFilterCoder;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.nio.ByteBuffer;

/**
 * Tests for {@link ScalableBloomFilter}.
 */
@RunWith(JUnit4.class)
public class ScalableBloomFilterTest {
  private static final ByteBuffer BUFFER = ByteBuffer.wrap(new byte[]{ 0x01, 0x02 });

  @Test
  public void testBuilderModeAddAll() throws Exception {
    Builder builder = ScalableBloomFilter.builder();
    assertTrue("Expected Bloom filter to have been modified.", builder.put(BUFFER));

    // Re-adding should skip and not record the insertion.
    assertFalse("Expected Bloom filter to not have been modified.", builder.put(BUFFER));

    // Verify insertion
    int maxValue = insertAndVerifyContents(builder, 31);

    // Verify that we only have one bloom filter instead of many since the number of insertions
    // is small.
    ScalableBloomFilter bloomFilter = builder.build();
    assertEquals(1, bloomFilter.numberOfBloomFilterSlices());
    verifyCoder(builder.build(), maxValue);
  }

  @Test
  public void testBuilderModeAddAllModeAtThreshold() throws Exception {
    // Use a builder where the insertion threshold to swap to add to last mode is 2^4 elements.
    Builder builder = ScalableBloomFilter.builder(4);

    // Verify insertion
    int maxValue = insertAndVerifyContents(builder, 16);

    ScalableBloomFilter bloomFilter = builder.build();
    // Verify at the threshold we have only built a single Bloom filter slice.
    assertEquals(1, bloomFilter.numberOfBloomFilterSlices());

    verifyCoder(bloomFilter, maxValue);
  }

  @Test
  public void testBuilderModeAddAllModeAtThresholdPlusOne() throws Exception {
    // Use a builder where the insertion threshold to swap to add to last mode is 2^4 elements.
    Builder builder = ScalableBloomFilter.builder(4);

    // Verify insertion
    int maxValue = insertAndVerifyContents(builder, 17);

    ScalableBloomFilter bloomFilter = builder.build();
    // Verify that at one over the threshold, we created two Bloom filter slices.
    assertEquals(2, bloomFilter.numberOfBloomFilterSlices());

    verifyCoder(bloomFilter, maxValue);
  }

  @Test
  public void testBuilderModeAddLastMode() throws Exception {
    // Use a builder where the insertion threshold to swap to add to last mode is 2^4 elements.
    Builder builder = ScalableBloomFilter.builder(4);

    // Verify insertion
    int maxValue = insertAndVerifyContents(builder, (int) Math.pow(2, 12) - 16);

    ScalableBloomFilter bloomFilter = builder.build();
    // Verify that we swapped to the scalable mode.
    // This is 9 because we inserted 16 elements swapping us into the add to all mode.
    // Then at every power of 2 (e.g. 32, 64, 128, ..) we add another filter.
    // Thus we have a filter for every power of 2 from 2^4 to 2^12 giving us 9 filters.
    assertEquals(9, bloomFilter.numberOfBloomFilterSlices());

    verifyCoder(bloomFilter, maxValue);
  }

  @Test
  public void testScalableBloomFilterCoder() throws Exception {
    Builder builderA = ScalableBloomFilter.builder();
    builderA.put(BUFFER);
    ScalableBloomFilter filterA = builderA.build();
    Builder builderB = ScalableBloomFilter.builder();
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
   * Inserts elements {@code 0, 1, ...} until the internal bloom filters have
   * been modified {@code maxNumberOfInsertions} times. Returns the largest value inserted.
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

  /**
   * Verifies that the bloom filter contains all the values from {@code [0, 1, ..., maxValue]}.
   */
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
   * Verifies that the coder correctly encodes and decodes and that all the values
   * {@code [0, 1, 2, ..., maxValue]} are contained within the decoded bloom filter.
   */
  private void verifyCoder(ScalableBloomFilter bloomFilter, int maxValue) throws Exception {
    byte[] encodedValue =
        CoderUtils.encodeToByteArray(ScalableBloomFilterCoder.of(), bloomFilter);
    ScalableBloomFilter decoded =
        CoderUtils.decodeFromByteArray(ScalableBloomFilterCoder.of(), encodedValue);
    verifyContents(decoded, maxValue);
  }
}

