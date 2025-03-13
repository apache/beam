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
package org.apache.beam.runners.flink.streaming;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.TestCountingSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.UnboundedSourceWrapper;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.UnboundedReadFromBoundedSource.BoundedToUnboundedSourceAdapter;
import org.apache.beam.sdk.util.construction.UnboundedReadFromBoundedSource.BoundedToUnboundedSourceAdapter.Checkpoint;
import org.apache.beam.sdk.values.ValueWithRecordId;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Test for bounded source restore in streaming mode. */
@RunWith(Parameterized.class)
public class BoundedSourceRestoreTest {

  private final int numTasks;
  private final int numSplits;

  public BoundedSourceRestoreTest(int numTasks, int numSplits) {
    this.numTasks = numTasks;
    this.numSplits = numSplits;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    /* Parameters for initializing the tests: {numTasks, numSplits} */
    return Arrays.asList(
        new Object[][] {
          {1, 1}, {1, 2}, {1, 4},
        });
  }

  @Test
  public void testRestore() throws Exception {
    final int numElements = 102;
    final int firstBatchSize = 23;
    final int secondBatchSize = numElements - firstBatchSize;
    final Set<Long> emittedElements = new HashSet<>();
    final Object checkpointLock = new Object();
    PipelineOptions options = PipelineOptionsFactory.create();

    // bounded source wrapped as unbounded source
    BoundedSource<Long> source = CountingSource.upTo(numElements);
    BoundedToUnboundedSourceAdapter<Long> unboundedSource =
        new BoundedToUnboundedSourceAdapter<>(source);
    UnboundedSourceWrapper<Long, Checkpoint<Long>> flinkWrapper =
        new UnboundedSourceWrapper<>("stepName", options, unboundedSource, numSplits);

    StreamSource<
            WindowedValue<ValueWithRecordId<Long>>, UnboundedSourceWrapper<Long, Checkpoint<Long>>>
        sourceOperator = new StreamSource<>(flinkWrapper);

    AbstractStreamOperatorTestHarness<WindowedValue<ValueWithRecordId<Long>>> testHarness =
        new AbstractStreamOperatorTestHarness<>(
            sourceOperator,
            numTasks /* max parallelism */,
            numTasks /* parallelism */,
            0 /* subtask index */);
    testHarness.setTimeCharacteristic(TimeCharacteristic.EventTime);

    // the first half of elements is read
    boolean readFirstBatchOfElements = false;
    try {
      testHarness.open();
      StreamSources.run(
          sourceOperator, checkpointLock, new PartialCollector<>(emittedElements, firstBatchSize));
    } catch (SuccessException e) {
      // success
      readFirstBatchOfElements = true;
    }
    assertTrue("Did not successfully read first batch of elements.", readFirstBatchOfElements);

    // draw a snapshot
    OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);

    // finalize checkpoint
    final ArrayList<Integer> finalizeList = new ArrayList<>();
    TestCountingSource.setFinalizeTracker(finalizeList);
    testHarness.notifyOfCompletedCheckpoint(0);

    // create a completely new source but restore from the snapshot
    BoundedSource<Long> restoredSource = CountingSource.upTo(numElements);
    BoundedToUnboundedSourceAdapter<Long> restoredUnboundedSource =
        new BoundedToUnboundedSourceAdapter<>(restoredSource);
    UnboundedSourceWrapper<Long, Checkpoint<Long>> restoredFlinkWrapper =
        new UnboundedSourceWrapper<>("stepName", options, restoredUnboundedSource, numSplits);
    StreamSource<
            WindowedValue<ValueWithRecordId<Long>>, UnboundedSourceWrapper<Long, Checkpoint<Long>>>
        restoredSourceOperator = new StreamSource<>(restoredFlinkWrapper);

    // set parallelism to 1 to ensure that our testing operator gets all checkpointed state
    AbstractStreamOperatorTestHarness<WindowedValue<ValueWithRecordId<Long>>> restoredTestHarness =
        new AbstractStreamOperatorTestHarness<>(
            restoredSourceOperator,
            numTasks /* max parallelism */,
            1 /* parallelism */,
            0 /* subtask index */);

    restoredTestHarness.setTimeCharacteristic(TimeCharacteristic.EventTime);

    // restore snapshot
    restoredTestHarness.initializeState(snapshot);

    // run again and verify that we see the other elements
    boolean readSecondBatchOfElements = false;
    try {
      restoredTestHarness.open();
      StreamSources.run(
          restoredSourceOperator,
          checkpointLock,
          new PartialCollector<>(emittedElements, secondBatchSize));
    } catch (SuccessException e) {
      // success
      readSecondBatchOfElements = true;
    }
    assertTrue("Did not successfully read second batch of elements.", readSecondBatchOfElements);

    // verify that we saw all NUM_ELEMENTS elements
    assertTrue(emittedElements.size() == numElements);
  }

  /** A special {@link RuntimeException} that we throw to signal that the test was successful. */
  private static class SuccessException extends RuntimeException {}

  /** A collector which consumes only specified number of elements. */
  private static class PartialCollector<T>
      implements StreamSources.OutputWrapper<StreamRecord<WindowedValue<ValueWithRecordId<T>>>> {

    private final Set<T> emittedElements;
    private final int elementsToConsumeLimit;

    private int count = 0;

    private PartialCollector(Set<T> emittedElements, int elementsToConsumeLimit) {
      this.emittedElements = emittedElements;
      this.elementsToConsumeLimit = elementsToConsumeLimit;
    }

    @Override
    public void emitWatermark(Watermark watermark) {}

    @Override
    public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> streamRecord) {
      collect((StreamRecord) streamRecord);
    }

    @Override
    public void emitLatencyMarker(LatencyMarker latencyMarker) {}

    @Override
    public void collect(StreamRecord<WindowedValue<ValueWithRecordId<T>>> record) {
      emittedElements.add(record.getValue().getValue().getValue());
      count++;
      if (count >= elementsToConsumeLimit) {
        throw new SuccessException();
      }
    }

    @Override
    public void close() {}
  }
}
