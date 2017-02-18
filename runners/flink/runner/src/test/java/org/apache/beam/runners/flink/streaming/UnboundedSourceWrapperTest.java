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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.runners.flink.translation.wrappers.streaming.io.UnboundedSourceWrapper;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.KV;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContextSynchronousImpl;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.util.InstantiationUtil;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Matchers;

/**
 * Tests for {@link UnboundedSourceWrapper}.
 */
@RunWith(Enclosed.class)
public class UnboundedSourceWrapperTest {

  /**
   * Parameterized tests.
   */
  @RunWith(Parameterized.class)
  public static class UnboundedSourceWrapperTestWithParams {
    private final int numTasks;
    private final int numSplits;

    public UnboundedSourceWrapperTestWithParams(int numTasks, int numSplits) {
      this.numTasks = numTasks;
      this.numSplits = numSplits;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
      /*
       * Parameters for initializing the tests:
       * {numTasks, numSplits}
       * The test currently assumes powers of two for some assertions.
       */
      return Arrays.asList(new Object[][]{
          {1, 1}, {1, 2}, {1, 4},
          {2, 1}, {2, 2}, {2, 4},
          {4, 1}, {4, 2}, {4, 4}
      });
    }

    /**
     * Creates a {@link UnboundedSourceWrapper} that has one or multiple readers per source.
     * If numSplits > numTasks the source has one source will manage multiple readers.
     */
    @Test
    public void testReaders() throws Exception {
      final int numElements = 20;
      final Object checkpointLock = new Object();
      PipelineOptions options = PipelineOptionsFactory.create();

      // this source will emit exactly NUM_ELEMENTS across all parallel readers,
      // afterwards it will stall. We check whether we also receive NUM_ELEMENTS
      // elements later.
      TestCountingSource source = new TestCountingSource(numElements);
      UnboundedSourceWrapper<KV<Integer, Integer>, TestCountingSource.CounterMark> flinkWrapper =
          new UnboundedSourceWrapper<>(options, source, numSplits);

      assertEquals(numSplits, flinkWrapper.getSplitSources().size());

      StreamSource<WindowedValue<
          KV<Integer, Integer>>,
          UnboundedSourceWrapper<
              KV<Integer, Integer>,
              TestCountingSource.CounterMark>> sourceOperator = new StreamSource<>(flinkWrapper);

      setupSourceOperator(sourceOperator, numTasks);

      try {
        sourceOperator.open();
        sourceOperator.run(checkpointLock,
            new Output<StreamRecord<WindowedValue<KV<Integer, Integer>>>>() {
              private int count = 0;

              @Override
              public void emitWatermark(Watermark watermark) {
              }

              @Override
              public void emitLatencyMarker(LatencyMarker latencyMarker) {
              }

              @Override
              public void collect(
                  StreamRecord<WindowedValue<KV<Integer, Integer>>> windowedValueStreamRecord) {

                count++;
                if (count >= numElements) {
                  throw new SuccessException();
                }
              }

              @Override
              public void close() {

              }
            });
      } catch (SuccessException e) {

        assertEquals(Math.max(1, numSplits / numTasks), flinkWrapper.getLocalSplitSources().size());

        // success
        return;
      }
      fail("Read terminated without producing expected number of outputs");
    }

    /**
     * Verify that snapshot/restore work as expected. We bring up a source and cancel
     * after seeing a certain number of elements. Then we snapshot that source,
     * bring up a completely new source that we restore from the snapshot and verify
     * that we see all expected elements in the end.
     */
    @Test
    public void testRestore() throws Exception {
      final int numElements = 20;
      final Object checkpointLock = new Object();
      PipelineOptions options = PipelineOptionsFactory.create();

      // this source will emit exactly NUM_ELEMENTS across all parallel readers,
      // afterwards it will stall. We check whether we also receive NUM_ELEMENTS
      // elements later.
      TestCountingSource source = new TestCountingSource(numElements);
      UnboundedSourceWrapper<KV<Integer, Integer>, TestCountingSource.CounterMark> flinkWrapper =
          new UnboundedSourceWrapper<>(options, source, numSplits);

      assertEquals(numSplits, flinkWrapper.getSplitSources().size());

      StreamSource<
          WindowedValue<KV<Integer, Integer>>,
          UnboundedSourceWrapper<
              KV<Integer, Integer>,
              TestCountingSource.CounterMark>> sourceOperator = new StreamSource<>(flinkWrapper);


      OperatorStateStore backend = mock(OperatorStateStore.class);

      TestingListState<KV<UnboundedSource, TestCountingSource.CounterMark>>
          listState = new TestingListState<>();

      when(backend.getOperatorState(Matchers.any(ListStateDescriptor.class)))
          .thenReturn(listState);

      StateInitializationContext initializationContext = mock(StateInitializationContext.class);

      when(initializationContext.getOperatorStateStore()).thenReturn(backend);
      when(initializationContext.isRestored()).thenReturn(false, true);

      flinkWrapper.initializeState(initializationContext);

      setupSourceOperator(sourceOperator, numTasks);

      final Set<KV<Integer, Integer>> emittedElements = new HashSet<>();

      boolean readFirstBatchOfElements = false;

      try {
        sourceOperator.open();
        sourceOperator.run(checkpointLock,
            new Output<StreamRecord<WindowedValue<KV<Integer, Integer>>>>() {
              private int count = 0;

              @Override
              public void emitWatermark(Watermark watermark) {
              }

              @Override
              public void emitLatencyMarker(LatencyMarker latencyMarker) {
              }

              @Override
              public void collect(
                  StreamRecord<WindowedValue<KV<Integer, Integer>>> windowedValueStreamRecord) {

                emittedElements.add(windowedValueStreamRecord.getValue().getValue());
                count++;
                if (count >= numElements / 2) {
                  throw new SuccessException();
                }
              }

              @Override
              public void close() {

              }
            });
      } catch (SuccessException e) {
        // success
        readFirstBatchOfElements = true;
      }

      assertTrue("Did not successfully read first batch of elements.", readFirstBatchOfElements);

      // draw a snapshot
      flinkWrapper.snapshotState(new StateSnapshotContextSynchronousImpl(0, 0));

      // test snapshot offsets
      assertEquals(flinkWrapper.getLocalSplitSources().size(),
          listState.getList().size());
      int totalEmit = 0;
      for (KV<UnboundedSource, TestCountingSource.CounterMark> kv : listState.get()) {
        totalEmit += kv.getValue().current + 1;
      }
      assertEquals(numElements / 2, totalEmit);

      // test that finalizeCheckpoint on CheckpointMark is called
      final ArrayList<Integer> finalizeList = new ArrayList<>();
      TestCountingSource.setFinalizeTracker(finalizeList);
      flinkWrapper.notifyCheckpointComplete(0);
      assertEquals(flinkWrapper.getLocalSplitSources().size(), finalizeList.size());

      // create a completely new source but restore from the snapshot
      TestCountingSource restoredSource = new TestCountingSource(numElements);
      UnboundedSourceWrapper<
          KV<Integer, Integer>, TestCountingSource.CounterMark> restoredFlinkWrapper =
          new UnboundedSourceWrapper<>(options, restoredSource, numSplits);

      assertEquals(numSplits, restoredFlinkWrapper.getSplitSources().size());

      StreamSource<
          WindowedValue<KV<Integer, Integer>>,
          UnboundedSourceWrapper<
              KV<Integer, Integer>,
              TestCountingSource.CounterMark>> restoredSourceOperator =
          new StreamSource<>(restoredFlinkWrapper);

      setupSourceOperator(restoredSourceOperator, numTasks);

      // restore snapshot
      restoredFlinkWrapper.initializeState(initializationContext);

      boolean readSecondBatchOfElements = false;

      // run again and verify that we see the other elements
      try {
        restoredSourceOperator.open();
        restoredSourceOperator.run(checkpointLock,
            new Output<StreamRecord<WindowedValue<KV<Integer, Integer>>>>() {
              private int count = 0;

              @Override
              public void emitWatermark(Watermark watermark) {
              }

              @Override
              public void emitLatencyMarker(LatencyMarker latencyMarker) {
              }

              @Override
              public void collect(
                  StreamRecord<WindowedValue<KV<Integer, Integer>>> windowedValueStreamRecord) {
                emittedElements.add(windowedValueStreamRecord.getValue().getValue());
                count++;
                if (count >= numElements / 2) {
                  throw new SuccessException();
                }
              }

              @Override
              public void close() {

              }
            });
      } catch (SuccessException e) {
        // success
        readSecondBatchOfElements = true;
      }

      assertEquals(Math.max(1, numSplits / numTasks), flinkWrapper.getLocalSplitSources().size());

      assertTrue("Did not successfully read second batch of elements.", readSecondBatchOfElements);

      // verify that we saw all NUM_ELEMENTS elements
      assertTrue(emittedElements.size() == numElements);
    }

    @Test
    public void testNullCheckpoint() throws Exception {
      final int numElements = 20;
      PipelineOptions options = PipelineOptionsFactory.create();

      TestCountingSource source = new TestCountingSource(numElements) {
        @Override
        public Coder<CounterMark> getCheckpointMarkCoder() {
          return null;
        }
      };
      UnboundedSourceWrapper<KV<Integer, Integer>, TestCountingSource.CounterMark> flinkWrapper =
          new UnboundedSourceWrapper<>(options, source, numSplits);

      OperatorStateStore backend = mock(OperatorStateStore.class);

      TestingListState<KV<UnboundedSource, TestCountingSource.CounterMark>>
          listState = new TestingListState<>();

      when(backend.getOperatorState(Matchers.any(ListStateDescriptor.class)))
          .thenReturn(listState);

      StateInitializationContext initializationContext = mock(StateInitializationContext.class);

      when(initializationContext.getOperatorStateStore()).thenReturn(backend);
      when(initializationContext.isRestored()).thenReturn(false, true);

      flinkWrapper.initializeState(initializationContext);

      StreamSource sourceOperator = new StreamSource<>(flinkWrapper);
      setupSourceOperator(sourceOperator, numTasks);
      sourceOperator.open();

      flinkWrapper.snapshotState(new StateSnapshotContextSynchronousImpl(0, 0));

      assertEquals(0, listState.getList().size());

      UnboundedSourceWrapper<
          KV<Integer, Integer>, TestCountingSource.CounterMark> restoredFlinkWrapper =
          new UnboundedSourceWrapper<>(options, new TestCountingSource(numElements),
              numSplits);

      StreamSource restoredSourceOperator = new StreamSource<>(flinkWrapper);
      setupSourceOperator(restoredSourceOperator, numTasks);
      sourceOperator.open();

      restoredFlinkWrapper.initializeState(initializationContext);

      assertEquals(Math.max(1, numSplits / numTasks), flinkWrapper.getLocalSplitSources().size());

    }

    @SuppressWarnings("unchecked")
    private static <T> void setupSourceOperator(StreamSource<T, ?> operator, int numSubTasks) {
      ExecutionConfig executionConfig = new ExecutionConfig();
      StreamConfig cfg = new StreamConfig(new Configuration());

      cfg.setTimeCharacteristic(TimeCharacteristic.EventTime);

      Environment env = new DummyEnvironment("MockTwoInputTask", numSubTasks, 0);

      StreamTask<?, ?> mockTask = mock(StreamTask.class);
      when(mockTask.getName()).thenReturn("Mock Task");
      when(mockTask.getCheckpointLock()).thenReturn(new Object());
      when(mockTask.getConfiguration()).thenReturn(cfg);
      when(mockTask.getEnvironment()).thenReturn(env);
      when(mockTask.getExecutionConfig()).thenReturn(executionConfig);
      when(mockTask.getAccumulatorMap())
          .thenReturn(Collections.<String, Accumulator<?, ?>>emptyMap());
      TestProcessingTimeService testProcessingTimeService = new TestProcessingTimeService();
      when(mockTask.getProcessingTimeService()).thenReturn(testProcessingTimeService);

      operator.setup(mockTask, cfg, (Output<StreamRecord<T>>) mock(Output.class));
    }

    /**
     * A special {@link RuntimeException} that we throw to signal that the test was successful.
     */
    private static class SuccessException extends RuntimeException {
    }
  }

  /**
   * Not parameterized tests.
   */
  public static class BasicTest {

    /**
     * Check serialization a {@link UnboundedSourceWrapper}.
     */
    @Test
    public void testSerialization() throws Exception {
      final int parallelism = 1;
      final int numElements = 20;
      PipelineOptions options = PipelineOptionsFactory.create();

      TestCountingSource source = new TestCountingSource(numElements);
      UnboundedSourceWrapper<KV<Integer, Integer>, TestCountingSource.CounterMark> flinkWrapper =
          new UnboundedSourceWrapper<>(options, source, parallelism);

      InstantiationUtil.serializeObject(flinkWrapper);
    }

  }

  private static final class TestingListState<T> implements ListState<T> {

    private final List<T> list = new ArrayList<>();

    @Override
    public void clear() {
      list.clear();
    }

    @Override
    public Iterable<T> get() throws Exception {
      return list;
    }

    @Override
    public void add(T value) throws Exception {
      list.add(value);
    }

    public List<T> getList() {
      return list;
    }

  }

}
