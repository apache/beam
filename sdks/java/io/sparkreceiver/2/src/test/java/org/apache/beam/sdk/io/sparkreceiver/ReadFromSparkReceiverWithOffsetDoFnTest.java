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
package org.apache.beam.sdk.io.sparkreceiver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.checkerframework.checker.initialization.qual.Initialized;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.UnknownKeyFor;
import org.joda.time.Instant;
import org.junit.Test;

/** Test class for {@link ReadFromSparkReceiverWithOffsetDoFn}. */
public class ReadFromSparkReceiverWithOffsetDoFnTest {

  private static final byte[] TEST_ELEMENT = new byte[] {};

  private final ReadFromSparkReceiverWithOffsetDoFn<String> dofnInstance =
      new ReadFromSparkReceiverWithOffsetDoFn<>(makeReadTransform());

  private SparkReceiverIO.Read<String> makeReadTransform() {
    ReceiverBuilder<String, CustomReceiverWithOffset> receiverBuilder =
        new ReceiverBuilder<>(CustomReceiverWithOffset.class).withConstructorArgs();
    return SparkReceiverIO.<String>read()
        .withSparkReceiverBuilder(receiverBuilder)
        .withGetOffsetFn(Long::valueOf)
        .withTimestampFn(Instant::parse);
  }

  private static class MockOutputReceiver implements DoFn.OutputReceiver<String> {

    private final List<String> records = new ArrayList<>();

    @Override
    public void output(String output) {}

    @Override
    public void outputWithTimestamp(
        String output, @UnknownKeyFor @NonNull @Initialized Instant timestamp) {
      records.add(output);
    }

    public List<String> getOutputs() {
      return this.records;
    }
  }

  private final ManualWatermarkEstimator<Instant> mockWatermarkEstimator =
      new ManualWatermarkEstimator<Instant>() {

        @Override
        public void setWatermark(Instant watermark) {
          // do nothing
        }

        @Override
        public Instant currentWatermark() {
          return null;
        }

        @Override
        public Instant getState() {
          return null;
        }
      };

  private List<String> createExpectedRecords(int numRecords) {
    List<String> records = new ArrayList<>();
    for (int i = 0; i < numRecords; i++) {
      records.add(String.valueOf(i));
    }
    return records;
  }

  @Test
  public void testInitialRestriction() {
    long expectedStartOffset = 0L;
    OffsetRange result = dofnInstance.initialRestriction(TEST_ELEMENT);
    assertEquals(new OffsetRange(expectedStartOffset, Long.MAX_VALUE), result);
  }

  @Test
  public void testRestrictionTrackerSplit() {
    OffsetRangeTracker offsetRangeTracker =
        dofnInstance.restrictionTracker(
            TEST_ELEMENT, dofnInstance.initialRestriction(TEST_ELEMENT));
    assertEquals(0L, offsetRangeTracker.currentRestriction().getFrom());
    assertEquals(Long.MAX_VALUE, offsetRangeTracker.currentRestriction().getTo());

    // Split is not needed
    assertNull(offsetRangeTracker.trySplit(0d));

    offsetRangeTracker =
        dofnInstance.restrictionTracker(
            TEST_ELEMENT, dofnInstance.initialRestriction(TEST_ELEMENT));

    assertTrue(offsetRangeTracker.tryClaim(0L));
    // Split is not needed, because check done wasn't called
    assertNull(offsetRangeTracker.trySplit(0d));

    offsetRangeTracker.checkDone();
    // Perform split because check done was called
    assertEquals(
        SplitResult.of(new OffsetRange(0, 1), new OffsetRange(1, Long.MAX_VALUE)),
        offsetRangeTracker.trySplit(0d));
  }

  @Test
  public void testProcessElement() {
    MockOutputReceiver receiver = new MockOutputReceiver();
    DoFn.ProcessContinuation result =
        dofnInstance.processElement(
            TEST_ELEMENT,
            dofnInstance.restrictionTracker(
                TEST_ELEMENT, dofnInstance.initialRestriction(TEST_ELEMENT)),
            mockWatermarkEstimator,
            receiver);
    assertEquals(DoFn.ProcessContinuation.resume(), result);
    assertEquals(
        createExpectedRecords(CustomReceiverWithOffset.RECORDS_COUNT), receiver.getOutputs());
  }
}
