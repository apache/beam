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

import org.apache.beam.runners.core.construction.SerializablePipelineOptions;
import org.apache.beam.runners.flink.translation.types.CoderTypeInformation;
import org.apache.beam.runners.flink.translation.types.CoderTypeSerializer;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.TestCountingSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.UnboundedSourceWrapper;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.FlinkSource;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.source.unbounded.FlinkUnboundedSource;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.CountingSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.util.construction.UnboundedReadFromBoundedSource.BoundedToUnboundedSourceAdapter;
import org.apache.beam.sdk.util.construction.UnboundedReadFromBoundedSource.BoundedToUnboundedSourceAdapter.Checkpoint;
import org.apache.beam.sdk.values.ValueWithRecordId;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.DataStreamSourceTest;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/** Test for bounded source restore in streaming mode. */
@RunWith(Parameterized.class)
public class BoundedSourceRestoreTest {

  // private final int numTasks;
  // private final int numSplits;

  public BoundedSourceRestoreTest(int numTasks, int numSplits) {
    // this.numTasks = numTasks;
    // this.numSplits = numSplits;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    /* Parameters for initializing the tests: {numTasks, numSplits} */
    return Arrays.asList(
        new Object[][] {
          {1, 1}, {1, 2}, {1, 4},
        });
  }

  // FIXME
  @Ignore
  @Test
  public void testRestore() throws Exception {
//    final int numElements = 102;
//    final int firstBatchSize = 23;
//    final int secondBatchSize = numElements - firstBatchSize;
//    final Set<Long> emittedElements = new HashSet<>();
//    final Object checkpointLock = new Object();
//    PipelineOptions options = PipelineOptionsFactory.create();
//
//    // bounded source wrapped as unbounded source
//    BoundedSource<Long> source = CountingSource.upTo(numElements);
//    BoundedToUnboundedSourceAdapter<Long> unboundedSource =
//        new BoundedToUnboundedSourceAdapter<>(source);
//    SerializablePipelineOptions serializablePipelineOptions = new SerializablePipelineOptions(
//        options);
//    FlinkUnboundedSource<Long> flinkWrapper =
//        FlinkSource.unbounded("stepName", unboundedSource, serializablePipelineOptions, numSplits);
//    WindowedValue.FullWindowedValueCoder<ValueWithRecordId<Long>> fullCoder = WindowedValue.getFullCoder(
//        ValueWithRecordId.ValueWithRecordIdCoder.of(VarLongCoder.of()),
//        GlobalWindow.Coder.INSTANCE);
//    TypeInformation<WindowedValue<ValueWithRecordId<Long>>> withIdTypeInfo =
//        new CoderTypeInformation<>(fullCoder, options);
//    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
//    env.getConfig().setAutoWatermarkInterval(100);
//
//    DataStreamSource<WindowedValue<ValueWithRecordId<Long>>> stream = env.fromSource(
//        flinkWrapper, WatermarkStrategy.noWatermarks(), "stepName", withIdTypeInfo);
//
//    // the first half of elements is read
//    boolean readFirstBatchOfElements = false;
//    try {
//      StreamSources.run(
//          sourceOperator, checkpointLock, new PartialCollector<>(emittedElements, firstBatchSize));
//    } catch (SuccessException e) {
//      // success
//      readFirstBatchOfElements = true;
//    }
//    assertTrue("Did not successfully read first batch of elements.", readFirstBatchOfElements);
//
//    // draw a snapshot
//    OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);
//
//    // finalize checkpoint
//    final ArrayList<Integer> finalizeList = new ArrayList<>();
//    TestCountingSource.setFinalizeTracker(finalizeList);
//    testHarness.notifyOfCompletedCheckpoint(0);
//
//    // create a completely new source but restore from the snapshot
//    BoundedSource<Long> restoredSource = CountingSource.upTo(numElements);
//    BoundedToUnboundedSourceAdapter<Long> restoredUnboundedSource =
//        new BoundedToUnboundedSourceAdapter<>(restoredSource);
//    UnboundedSourceWrapper<Long, Checkpoint<Long>> restoredFlinkWrapper =
//        new UnboundedSourceWrapper<>("stepName", options, restoredUnboundedSource, numSplits);
//    StreamSource<
//            WindowedValue<ValueWithRecordId<Long>>, UnboundedSourceWrapper<Long, Checkpoint<Long>>>
//        restoredSourceOperator = new StreamSource<>(restoredFlinkWrapper);
//
//    // set parallelism to 1 to ensure that our testing operator gets all checkpointed state
//    AbstractStreamOperatorTestHarness<WindowedValue<ValueWithRecordId<Long>>> restoredTestHarness =
//        new AbstractStreamOperatorTestHarness<>(
//            restoredSourceOperator,
//            numTasks /* max parallelism */,
//            1 /* parallelism */,
//            0 /* subtask index */);
//
//    restoredTestHarness.setTimeCharacteristic(TimeCharacteristic.EventTime);
//
//    // restore snapshot
//    restoredTestHarness.initializeState(snapshot);
//
//    // run again and verify that we see the other elements
//    boolean readSecondBatchOfElements = false;
//    try {
//      restoredTestHarness.open();
//      StreamSources.run(
//          restoredSourceOperator,
//          checkpointLock,
//          new PartialCollector<>(emittedElements, secondBatchSize));
//    } catch (SuccessException e) {
//      // success
//      readSecondBatchOfElements = true;
//    }
//    assertTrue("Did not successfully read second batch of elements.", readSecondBatchOfElements);
//
//    // verify that we saw all NUM_ELEMENTS elements
//    assertTrue(emittedElements.size() == numElements);
  }
}
